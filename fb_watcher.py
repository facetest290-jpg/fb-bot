#!/usr/bin/env python3
"""
Facebook pages -> Telegram watcher (Playwright + cookies), multi-page.
Optimized for Railway Cron (one short run, then exit).

Env vars:
    TG_TOKEN, TG_CHAT_ID, FB_COOKIES (or cookies.txt)
    FB_PAGES  one or more pages, separated by new lines, commas or ';'
              each item:  page  or  page|chat_id  or  page|chat_id|label
              page    = numeric id, username, or full facebook URL
              chat_id = (optional) send this page to a different channel
              label   = (optional) name shown above the post (default: page)
              examples:  100064825534678
                         somepage|@my_channel|Some Page
    FB_PAGE_ID  old single-page variable, still works if FB_PAGES is empty
    TG_OWNER_ID (optional), FB_USER_AGENT (optional), BOT_TZ (default Africa/Cairo)
    STATE_DIR   folder for state.json - point it to the Railway Volume (e.g. /data)
    QUIET_HOURS=off   disable the 22:00-08:00 quiet window (testing)
    RUN_BUDGET        max seconds per run before skipping remaining pages (default 300)
    REPORT_ALWAYS=1   owner report after every run (default: only when something happened)

Modes:
    --cron    ONE run over all pages, then exit (Railway Cron)
    --gate    decide if a check is due (no browser) -> GITHUB_OUTPUT
    --once    run one check now (ignores the schedule)
    --test    with --once/--cron: send the latest post of the first page
    --debug   diagnose only: no Telegram, saves debug_N.png / debug_N.html
    (none)    loop forever
"""
import argparse
import hashlib
import html
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

BASE = Path(__file__).resolve().parent


# ================================================================ config
def load_env():
    p = BASE / ".env"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()

# state.json must live on a persistent Railway Volume (STATE_DIR=/data),
# otherwise every cron run would start from zero.
_state_dir = Path(os.environ.get("STATE_DIR") or BASE)
_state_dir.mkdir(parents=True, exist_ok=True)
STATE_FILE = _state_dir / "state.json"


# ---- قيم للتجربة في ريبو خاص فقط (لو فاضية، بيقرأ من Variables / .env) ----
LOCAL_TOKEN = ""
LOCAL_CHAT_ID = ""
LOCAL_PAGES = ""        # مثال: "100064825534678, otherpage|@chan|Name"
LOCAL_COOKIES = r"""
"""
# --------------------------------------------------------------------


def env(name, default=""):
    return (os.environ.get(name) or default).strip()


TOKEN = env("TG_TOKEN", LOCAL_TOKEN)
CHAT_ID = env("TG_CHAT_ID", LOCAL_CHAT_ID)
OWNER_ID = env("TG_OWNER_ID")
TZ = ZoneInfo(env("BOT_TZ", "Africa/Cairo"))
USER_AGENT = env(
    "FB_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)

QUIET_START, QUIET_END = 22, 8          # no requests from 22:00 to 08:00
CHECK_GAP = 20 * 60                     # gap between runs (seconds)
GATE_TOLERANCE = 10 * 60
MAX_POSTS = 10                          # newest posts read per page
MAX_FAILS = 3                           # consecutive failures before stopping/alerting
SEEN_KEEP = 200                         # remembered post ids per page
try:
    RUN_BUDGET = int(env("RUN_BUDGET", "300"))
except ValueError:
    RUN_BUDGET = 300


# ================================================================ pages
def page_url(spec):
    if spec.lower().startswith("http"):
        return spec
    if spec.isdigit():
        return f"https://www.facebook.com/profile.php?id={spec}"
    return f"https://www.facebook.com/{spec.strip('/')}"


def parse_pages():
    """FB_PAGES = 'page', 'page|chat_id' or 'page|chat_id|label' items."""
    raw = env("FB_PAGES") or env("FB_PAGE_ID") or LOCAL_PAGES
    pages, keys = [], set()
    for item in re.split(r"[\n,;]+", raw):
        item = item.strip()
        if not item:
            continue
        parts = [x.strip() for x in item.split("|")]
        spec = parts[0]
        if not spec or spec in keys:
            continue
        keys.add(spec)
        short = re.sub(r"^https?://(www\.|m\.)?facebook\.com/", "", spec).strip("/")
        pages.append({
            "key": spec,
            "url": page_url(spec),
            "chat": (parts[1] if len(parts) > 1 and parts[1] else CHAT_ID),
            "label": (parts[2] if len(parts) > 2 and parts[2] else short),
        })
    return pages


PAGES = parse_pages()


def get_raw_cookies():
    raw = os.environ.get("FB_COOKIES", "")
    if not raw.strip():
        f = BASE / "cookies.txt"
        if f.exists():
            raw = f.read_text(encoding="utf-8")
    if not raw.strip():
        raw = LOCAL_COOKIES
    return raw.strip()


def cookie_hash():
    return hashlib.sha256(get_raw_cookies().encode()).hexdigest()[:16]


# ================================================================ cookies
def parse_cookies(raw):
    """Accepts Netscape format (cookies.txt) or a Cookie-Editor JSON export."""
    raw = raw.strip()
    cookies = []
    if raw.startswith("["):
        for c in json.loads(raw):
            item = {
                "name": c["name"],
                "value": c["value"],
                "domain": c.get("domain", ".facebook.com"),
                "path": c.get("path", "/"),
                "secure": bool(c.get("secure", True)),
            }
            exp = c.get("expirationDate") or c.get("expires")
            if isinstance(exp, (int, float)) and exp > 0:
                item["expires"] = exp
            cookies.append(item)
        return cookies
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            continue
        parts = re.split(r"\s+", line, maxsplit=6)
        if len(parts) < 7:
            continue
        domain, _, cpath, secure, expires, name, value = parts[:7]
        c = {"name": name, "value": value, "domain": domain,
             "path": cpath, "secure": secure.upper() == "TRUE"}
        try:
            if int(expires) > 0:
                c["expires"] = int(expires)
        except ValueError:
            pass
        cookies.append(c)
    return cookies


def normalize_cookies(cookies):
    """Keep the browser's refreshed Facebook cookies for the next run."""
    out = []
    for c in cookies:
        if "facebook.com" not in c.get("domain", ""):
            continue
        d = {k: c[k] for k in ("name", "value", "domain", "path",
                               "secure", "httpOnly", "sameSite") if k in c}
        exp = c.get("expires", -1)
        if isinstance(exp, (int, float)) and exp > 0:
            d["expires"] = exp
        out.append(d)
    return out


# ================================================================ telegram
LAST_TG_ERROR = ""


def tg(method, **payload):
    global LAST_TG_ERROR
    data = urllib.parse.urlencode(payload).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}", data=data)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "ignore")[:150]
        except Exception:
            body = ""
        LAST_TG_ERROR = f"HTTP {e.code} {body}".strip()
        print("Telegram error:", LAST_TG_ERROR)
        return False
    except Exception as e:
        LAST_TG_ERROR = type(e).__name__
        print("Telegram error:", LAST_TG_ERROR)
        return False


def send_post(post, page):
    text = post["text"] or "(post without text)"
    if len(text) > 3000:
        text = text[:3000] + "…"
    head = f"📌 {page['label']}\n\n" if len(PAGES) > 1 else ""
    caption = f"{head}{text}\n\n{post['url']}"
    chat = page["chat"]
    if post["images"]:
        if tg("sendPhoto", chat_id=chat, photo=post["images"][0],
              caption=caption[:1024]):
            return True
    return tg("sendMessage", chat_id=chat, text=caption)


def notify(msg):
    """Status messages: private chat only (never the channel)."""
    if OWNER_ID:
        tg("sendMessage", chat_id=OWNER_ID, text=msg)
    else:
        print("[TG_OWNER_ID not set]", msg.replace("\n", " | "))


def alert(msg):
    tg("sendMessage", chat_id=OWNER_ID or CHAT_ID, text=f"🚨 ALERT: {msg}")


def alert_throttled(key, msg, hours=6):
    """Same alert at most once every `hours` (avoids a message every 20 min)."""
    f = _state_dir / "alerts.json"
    try:
        sent = json.loads(f.read_text()) if f.exists() else {}
    except Exception:
        sent = {}
    now = time.time()
    if now - sent.get(key, 0) < hours * 3600:
        print("[alert suppressed]", msg.replace("\n", " | "))
        return
    sent[key] = now
    try:
        f.write_text(json.dumps(sent))
    except Exception:
        pass
    alert(msg)


# ================================================================ state
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            try:
                STATE_FILE.replace(STATE_FILE.with_suffix(".bad"))
            except Exception:
                pass
            alert("state.json was corrupted. The bot started from scratch (backup "
                  "saved as state.json.bad). Pages will be re-learned silently, "
                  "so no old posts are re-sent.")
    return {}


def save_state(st):
    for ps in st.get("pages", {}).values():
        if ps.get("seen"):
            ps["seen"] = ps["seen"][-SEEN_KEEP:]
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(st), encoding="utf-8")
    tmp.replace(STATE_FILE)  # atomic write


# ================================================================ schedule
def now_local():
    return datetime.now(TZ)


def in_quiet(dt):
    # QUIET_HOURS=off disables the quiet window (for testing at night)
    if env("QUIET_HOURS").lower() in ("off", "0", "false", "no"):
        return False
    return dt.hour >= QUIET_START or dt.hour < QUIET_END


def next_quiet_end(dt):
    end = dt.replace(hour=QUIET_END, minute=0, second=0, microsecond=0)
    if dt >= end:
        end += timedelta(days=1)
    return end


def plan_next(since=None):
    due = datetime.fromtimestamp((since or time.time()) + CHECK_GAP, TZ)
    if in_quiet(due):
        due = next_quiet_end(due)
    return due.timestamp()


def should_run(st, tolerance=0, ignore_due=False):
    if env("FORCE") == "1":
        return True, "force"
    if st.get("blocked"):
        if st.get("cookie_hash") != cookie_hash():
            return True, "new cookies"
        return False, "blocked"
    if in_quiet(now_local()):
        return False, "quiet hours"
    if not ignore_due and time.time() < st.get("next_due", 0) - tolerance:
        return False, "not due yet"
    return True, "due"


def gate():
    ok, why = should_run(load_state(), tolerance=GATE_TOLERANCE)
    print(f"gate: run={ok} ({why}) local={now_local():%H:%M}")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"run={'true' if ok else 'false'}\n")


# ================================================================ parsing
def walk(o):
    stack = [o]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            yield cur
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)


def find_first(o, pred):
    for d in walk(o):
        for k, v in d.items():
            if pred(k, v):
                return v
    return None


def find_all(o, pred):
    out = []
    for d in walk(o):
        for k, v in d.items():
            if pred(k, v):
                out.append(v)
    return out


POST_URL_RE = re.compile(
    r"facebook\.com/.+/(posts|videos|reel)/|story_fbid|/permalink|pfbid")


def extract_posts(page_html):
    blobs = re.findall(
        r'<script type="application/json"[^>]*>(.*?)</script>', page_html, re.S)
    posts = {}
    stories_seen = 0
    for raw in blobs:
        if "creation_time" not in raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for node in walk(data):
            if node.get("__typename") != "Story":
                continue
            stories_seen += 1
            ts = find_first(node, lambda k, v: k == "creation_time"
                            and isinstance(v, int) and v > 1_000_000_000)
            if not ts:
                continue
            url = find_first(node, lambda k, v: k in ("url", "wwwURL")
                             and isinstance(v, str) and POST_URL_RE.search(v))
            pid = node.get("post_id") or find_first(
                node, lambda k, v: k == "post_id" and isinstance(v, str))
            text = find_first(node, lambda k, v: k == "message"
                              and isinstance(v, dict) and v.get("text"))
            text = text.get("text") if isinstance(text, dict) else ""
            images = find_all(node, lambda k, v: k in ("photo_image", "image")
                              and isinstance(v, dict)
                              and isinstance(v.get("uri"), str))
            images = [i["uri"] for i in images]
            key = pid or url
            if not key:
                continue
            if not url and pid:
                url = f"https://www.facebook.com/{pid}"
            old = posts.get(key)
            if old is None or (not old["text"] and text):
                posts[key] = {
                    "id": str(key), "ts": ts, "url": html.unescape(url or ""),
                    "text": text or (old["text"] if old else ""),
                    "images": images or (old["images"] if old else []),
                }
    result = sorted(posts.values(), key=lambda p: p["ts"], reverse=True)
    return result, stories_seen, len(blobs)


# ================================================================ browser
BLOCKED_TYPES = {"image", "media", "font"}   # not needed: we parse the JSON
LOGIN_MARKS = ("login", "checkpoint", "two_step", "recover", "disabled")


def url_is_blocked(url):
    low = url.lower()
    return any(x in low for x in LOGIN_MARKS)


def fetch_pages(cookie_list, pages, debug=False):
    """One browser, one session, all pages one after the other.
    Returns (results, cookies, session_ok). results = [{page, content, error}]."""
    from playwright.sync_api import sync_playwright  # lazy: gate needs no deps

    results, cookies, session = [], [], True
    started = time.time()
    time.sleep(random.uniform(1, 4))
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel=env("BROWSER_CHANNEL") or None,
            headless=True,
            args=["--no-sandbox",
                  "--disable-dev-shm-usage",
                  "--disable-gpu",
                  "--disable-extensions",
                  "--disable-blink-features=AutomationControlled"])
        try:
            ctx = browser.new_context(
                locale="en-GB",
                timezone_id=env("BOT_TZ", "Africa/Cairo"),
                viewport={"width": 1366, "height": 900},
                user_agent=USER_AGENT,
            )
            ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            if not debug:  # keep screenshots intact in debug mode
                ctx.route("**/*", lambda r: r.abort()
                          if r.request.resource_type in BLOCKED_TYPES
                          else r.continue_())
            ctx.add_cookies(cookie_list)
            tab = ctx.new_page()
            for i, pg in enumerate(pages):
                if i:
                    if time.time() - started > RUN_BUDGET:
                        print(f"Run budget ({RUN_BUDGET}s) reached; "
                              f"skipping {len(pages) - i} page(s) until next run.")
                        break
                    time.sleep(random.uniform(3, 7))
                try:
                    tab.goto(pg["url"], wait_until="domcontentloaded", timeout=60000)
                    tab.wait_for_timeout(random.randint(3000, 5000))
                    if url_is_blocked(tab.url):
                        session = False
                        break
                    for _ in range(2):
                        tab.mouse.wheel(0, random.randint(900, 1600))
                        tab.wait_for_timeout(random.randint(1200, 2200))
                    content = tab.content()
                    if debug:
                        tab.screenshot(path=str(BASE / f"debug_{i + 1}.png"))
                        (BASE / f"debug_{i + 1}.html").write_text(
                            content, encoding="utf-8")
                    results.append({"page": pg, "content": content, "error": None})
                except Exception as e:
                    results.append({"page": pg, "content": None,
                                    "error": f"{type(e).__name__}: {str(e)[:100]}"})
            cookies = ctx.cookies("https://www.facebook.com")
        finally:
            browser.close()
    if "c_user" not in {c["name"] for c in cookies}:
        session = False
    return results, cookies, session


# ================================================================ runs
def register_failure(st, reason, t0=None):
    st["fails"] = st.get("fails", 0) + 1
    print(f"Failure #{st['fails']}: {reason}")
    if st["fails"] >= MAX_FAILS:
        st["blocked"] = True
        st["block_reason"] = reason
        alert(f"The bot failed {MAX_FAILS} times in a row and stopped automatically "
              "to avoid raising suspicion.\n"
              f"Last reason: {reason}\n"
              "After fixing the problem, update FB_COOKIES (or set RESET=1 once).")
    else:
        notify(f"⚠️ Run failed ({st['fails']}/{MAX_FAILS}).\nReason: {reason}")
    st["next_due"] = plan_next(t0)
    save_state(st)


def block(st, reason, t0=None):
    st["blocked"] = True
    st["block_reason"] = reason
    st["next_due"] = plan_next(t0)
    save_state(st)
    print("Blocked:", reason)
    alert(f"{reason}\nThe bot is fully stopped and will not send any request to "
          "Facebook until you update FB_COOKIES (or set RESET=1 once).")


def _run(test, t0):
    st = load_state()
    st.pop("seen", None)  # legacy single-page key (now stored per page)
    if os.environ.get("RESET") == "1" or st.get("cookie_hash") != cookie_hash():
        st.update({"blocked": False, "fails": 0, "cookies": None,
                   "cookie_hash": cookie_hash()})
        st.pop("block_reason", None)
        for ps in st.get("pages", {}).values():
            ps["fails"] = 0
    pstate = st.setdefault("pages", {})

    # least recently checked first, so a time-limited run never starves a page
    order = sorted(PAGES, key=lambda p: pstate.get(p["key"], {}).get("last_check", 0))

    cookie_list = st.get("cookies") or parse_cookies(get_raw_cookies())
    results, cookies, session = fetch_pages(cookie_list, order)

    if not session:
        return block(st, "Cookies expired or Facebook asked for login / identity "
                         "confirmation (checkpoint).", t0)

    ok_pages, test_done = 0, False
    lines, new_total = [], 0
    for r in results:
        pg = r["page"]
        ps = pstate.setdefault(pg["key"], {})
        ps["last_check"] = int(time.time())

        posts = []
        reason = r["error"]
        if not reason:
            posts = extract_posts(r["content"])[0][:MAX_POSTS]
            if not posts:
                reason = "no posts could be read from the page"
        if reason:
            ps["fails"] = ps.get("fails", 0) + 1
            print(f"[{pg['label']}] failure #{ps['fails']}: {reason}")
            lines.append(f"- ⚠️ {pg['label']}: FAILED {ps['fails']}/{MAX_FAILS} ({reason})")
            if ps["fails"] == MAX_FAILS:
                alert(f"Page '{pg['label']}' failed {MAX_FAILS} times in a row.\n"
                      f"{pg['url']}\nLast reason: {reason}\n"
                      "The bot keeps trying it on every run.")
            continue

        ok_pages += 1
        ps["fails"] = 0
        seen = ps.get("seen")
        if seen is None:  # first time we see this page: remember, send nothing
            seen = ps["seen"] = [p["id"] for p in reversed(posts)]
            notify(f"🟢 Page activated: {pg['label']}\n{pg['url']}\n"
                   f"Saved {len(posts)} current posts without sending them.\n"
                   f"Only new posts will be sent to {pg['chat']} from now on.")
            if not (test and not test_done):
                continue
        if test and not test_done:
            test_done = True
            notify(f"Test: sending the latest post of {pg['label']}...")
            if send_post(posts[0], pg):
                if posts[0]["id"] not in seen:
                    seen.append(posts[0]["id"])
            else:
                notify("The test post could not be sent.")

        seen_set = set(seen)
        new = [p for p in posts if p["id"] not in seen_set]
        sent = 0
        for p in sorted(new, key=lambda x: x["ts"]):  # oldest first
            if send_post(p, pg):
                seen.append(p["id"])
                sent += 1
                time.sleep(random.uniform(1, 3))
        ps["seen"] = seen
        new_total += len(new)
        line = f"- {pg['label']}: {len(posts)} read, {len(new)} new, {sent} sent"
        if sent < len(new):
            line += (f" ({len(new) - sent} will retry next run; "
                     f"Telegram: {LAST_TG_ERROR or 'unknown error'})")
        lines.append(line)

    if results and ok_pages == 0:
        return register_failure(
            st, "Session is valid but no page could be read.", t0)

    st["cookies"] = normalize_cookies(cookies)  # keep the session fresh
    st["fails"] = 0
    st["next_due"] = plan_next(t0)
    st["last_ok"] = int(time.time())
    save_state(st)
    nd = datetime.fromtimestamp(st["next_due"], TZ)
    print(f"Run OK: {ok_pages}/{len(order)} page(s) read, {new_total} new post(s).")

    has_failed = any("FAILED" in l for l in lines)
    if new_total == 0 and not has_failed and env("REPORT_ALWAYS") != "1":
        return
    notify(f"Run report ({now_local():%Y-%m-%d %H:%M} {TZ.key})\n"
           + "\n".join(lines) + f"\nNext run: about {nd:%H:%M}")


RUN_FLAG = _state_dir / "running.flag"


def run_once(test=False):
    t0 = time.time()
    if RUN_FLAG.exists():  # the previous run never reached its end
        alert("The previous run did not finish: the process was probably killed "
              "(out of memory, timeout or a redeploy). If it repeats, check "
              "Railway Metrics/Logs.")
    try:
        RUN_FLAG.write_text(str(int(t0)))
    except Exception:
        pass
    try:
        _run(test, t0)
    except Exception as e:
        register_failure(load_state(), f"{type(e).__name__}: {str(e)[:150]}", t0)
    finally:
        try:
            RUN_FLAG.unlink()
        except Exception:
            pass


def run_cron(test=False):
    """Railway Cron entry: one run, then the process exits (stops billing)."""
    ok, why = should_run(load_state(), ignore_due=True)
    print(f"cron: run={ok} ({why}) local={now_local():%H:%M}")
    if ok:
        run_once(test)


def run_debug():
    results, cookies, session = fetch_pages(
        parse_cookies(get_raw_cookies()), PAGES, debug=True)
    print("Session OK     :", session)
    for i, r in enumerate(results, 1):
        pg = r["page"]
        print(f"\n[{i}] {pg['label']}  ->  {pg['url']}")
        if r["error"]:
            print("  error:", r["error"])
            continue
        posts, stories, blobs = extract_posts(r["content"])
        print(f"  JSON blobs: {blobs} | Story nodes: {stories} | Posts: {len(posts)}")
        for p in posts[:MAX_POSTS]:
            when = time.strftime("%Y-%m-%d %H:%M", time.localtime(p["ts"]))
            print(f"  - {when} | {p['id']} | {p['text'][:60]!r} | imgs={len(p['images'])}")
    print("\nSaved debug_N.png and debug_N.html")


def loop(test):
    first = True
    while True:
        ok, why = should_run(load_state())
        if ok:
            run_once(test=test and first)
            first = False
        st = load_state()
        if st.get("blocked") or in_quiet(now_local()):
            wait = 300
        else:
            wait = max(30, min(600, st.get("next_due", 0) - time.time()))
        time.sleep(wait)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gate", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--cron", action="store_true")
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--debug", action="store_true")
    a = ap.parse_args()

    if a.gate:
        return gate()

    missing = []
    if not PAGES:
        missing.append("FB_PAGES")
    if not get_raw_cookies():
        missing.append("FB_COOKIES")
    if not a.debug:
        if not TOKEN:
            missing.append("TG_TOKEN")
        if not CHAT_ID:
            missing.append("TG_CHAT_ID")
    if missing:
        msg = "Missing settings: " + ", ".join(missing)
        if TOKEN and (OWNER_ID or CHAT_ID):
            alert_throttled("config", msg)
        sys.exit(msg)
    if not OWNER_ID:
        print("WARNING: TG_OWNER_ID is not set - status messages are not delivered "
              "and alerts go to the channel.")

    try:
        if a.debug:
            return run_debug()
        test = a.test or env("TEST") == "1"
        if a.cron:
            return run_cron(test)
        if a.once:
            return run_once(test)
        loop(test)
    except Exception as e:
        alert_throttled("crash", f"Unexpected crash: {type(e).__name__}: "
                                 f"{str(e)[:200]}", hours=1)
        raise


if __name__ == "__main__":
    main()
