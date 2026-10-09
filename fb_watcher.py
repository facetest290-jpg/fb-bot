#!/usr/bin/env python3
"""
Facebook page -> Telegram watcher (Playwright + cookies).
Optimized for Railway Cron (one short run every 20 min, then exit).

Env vars:
    TG_TOKEN, TG_CHAT_ID, FB_PAGE_ID, FB_COOKIES (or cookies.txt),
    TG_OWNER_ID (optional), FB_USER_AGENT (optional), BOT_TZ (default Africa/Cairo),
    STATE_DIR (folder for state.json - point it to the Railway Volume, e.g. /data)

Modes:
    --cron    ONE check then exit (Railway Cron). Only quiet hours/blocked are checked.
    --gate    decide if a check is due (no browser) -> GITHUB_OUTPUT
    --once    run one check now (ignores the schedule)
    --test    with --once/--cron: send "bot is up" + the latest post
    --debug   diagnose only: no Telegram, saves debug.png / debug.html
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
LOCAL_PAGE_ID = ""
LOCAL_COOKIES = r"""
"""
# --------------------------------------------------------------------


def env(name, default=""):
    return (os.environ.get(name) or default).strip()


TOKEN = env("TG_TOKEN", LOCAL_TOKEN)
CHAT_ID = env("TG_CHAT_ID", LOCAL_CHAT_ID)
OWNER_ID = env("TG_OWNER_ID")
PAGE_ID = env("FB_PAGE_ID", LOCAL_PAGE_ID)
TZ = ZoneInfo(env("BOT_TZ", "Africa/Cairo"))
USER_AGENT = env(
    "FB_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)

QUIET_START, QUIET_END = 22, 8          # no requests from 22:00 to 08:00
CHECK_GAP = 20 * 60                     # gap between checks (seconds)
GATE_TOLERANCE = 10 * 60
MAX_POSTS = 10
MAX_FAILS = 3


def page_url():
    if PAGE_ID.isdigit():
        return f"https://www.facebook.com/profile.php?id={PAGE_ID}"
    return f"https://www.facebook.com/{PAGE_ID}"


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
def tg(method, **payload):
    data = urllib.parse.urlencode(payload).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}", data=data)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        print("Telegram error:", e.code)
        return False
    except Exception as e:
        print("Telegram error:", type(e).__name__)
        return False


def send_post(post):
    text = post["text"] or "(post without text)"
    if len(text) > 3000:
        text = text[:3000] + "…"
    caption = f"{text}\n\n{post['url']}"
    if post["images"]:
        if tg("sendPhoto", chat_id=CHAT_ID, photo=post["images"][0],
              caption=caption[:1024]):
            return True
    return tg("sendMessage", chat_id=CHAT_ID, text=caption)


def notify(msg):
    """Status messages: private chat only (never the channel)."""
    if OWNER_ID:
        tg("sendMessage", chat_id=OWNER_ID, text=msg)
    else:
        print("[TG_OWNER_ID not set]", msg.replace("\n", " | "))


def alert(msg):
    tg("sendMessage", chat_id=OWNER_ID or CHAT_ID, text=f"ALERT: {msg}")


# ================================================================ state
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(st):
    if st.get("seen"):
        st["seen"] = st["seen"][-300:]
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


def fetch_page(cookie_list, debug=False):
    from playwright.sync_api import sync_playwright  # lazy: gate needs no deps

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
            page = ctx.new_page()
            page.goto(page_url(), wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(random.randint(3000, 5000))
            for _ in range(2):
                page.mouse.wheel(0, random.randint(900, 1600))
                page.wait_for_timeout(random.randint(1200, 2200))
            content = page.content()
            final_url = page.url
            cookies = ctx.cookies("https://www.facebook.com")
            if debug:
                page.screenshot(path=str(BASE / "debug.png"))
                (BASE / "debug.html").write_text(content, encoding="utf-8")
        finally:
            browser.close()
    return content, final_url, cookies


def session_ok(final_url, cookies):
    low = final_url.lower()
    if any(x in low for x in ("login", "checkpoint", "two_step", "recover", "disabled")):
        return False
    return "c_user" in {c["name"] for c in cookies}


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
        notify(f"Check failed ({st['fails']}/{MAX_FAILS}).\n"
               f"Page: {PAGE_ID}\n{page_url()}\n"
               f"Reason: {reason}")
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
    if os.environ.get("RESET") == "1" or st.get("cookie_hash") != cookie_hash():
        st.update({"blocked": False, "fails": 0, "cookies": None,
                   "cookie_hash": cookie_hash()})
        st.pop("block_reason", None)

    cookie_list = st.get("cookies") or parse_cookies(get_raw_cookies())
    content, final_url, cookies = fetch_page(cookie_list)

    if not session_ok(final_url, cookies):
        return block(st, "Cookies expired or Facebook asked for login / identity "
                         "confirmation (checkpoint).", t0)

    posts, _, _ = extract_posts(content)
    posts = posts[:MAX_POSTS]
    if not posts:
        return register_failure(
            st, "Session is valid but no posts could be read from the page.", t0)

    st["cookies"] = normalize_cookies(cookies)

    seen = st.get("seen")
    first_run = seen is None
    if first_run:
        st["seen"] = [p["id"] for p in reversed(posts)]
        seen = st["seen"]
        notify(f"Bot activated. Saved {len(posts)} current posts without sending "
               f"them.\nPage: {PAGE_ID}\n{page_url()}\n"
               "From now on only new posts will be sent to the channel.")
    if test:
        notify("Bot is running. Sending the latest post to the channel as a test...")
        if send_post(posts[0]):
            if posts[0]["id"] not in seen:
                seen.append(posts[0]["id"])
        else:
            notify("The test post could not be sent to the channel.")
    seen_set = set(seen)
    new = [p for p in posts if p["id"] not in seen_set]
    sent = []
    for p in sorted(new, key=lambda x: x["ts"]):
        if send_post(p):
            seen.append(p["id"])
            sent.append(p)
            time.sleep(random.uniform(1, 3))
    st["seen"] = seen

    st["fails"] = 0
    st["next_due"] = plan_next(t0)
    st["last_ok"] = int(time.time())
    save_state(st)
    nd = datetime.fromtimestamp(st["next_due"], TZ)
    print(f"Checked OK: {len(new)} new post(s). Next check about {nd:%H:%M}")

    if first_run:
        return
    # Only message the owner when something happened (saves nothing in credits
    # but avoids 40+ chat messages per day). Set REPORT_ALWAYS=1 to get every check.
    if not new and env("REPORT_ALWAYS") != "1":
        return
    report = (f"Page checked: {PAGE_ID}\n{page_url()}\n"
              f"Time: {now_local():%Y-%m-%d %H:%M} ({TZ.key})\n"
              f"Posts read: {len(posts)}\n")
    if not new:
        report += "Result: no new posts.\n"
    else:
        report += (f"Result: found {len(new)} new post(s), "
                   f"sent {len(sent)} to the channel.\n")
        for p in sent:
            report += f"- {p['url']}\n"
        if len(sent) < len(new):
            report += (f"{len(new) - len(sent)} post(s) could not be sent and will "
                       "be retried on the next check.\n")
    report += f"Next check: about {nd:%H:%M}"
    notify(report)


def run_once(test=False):
    t0 = time.time()
    try:
        _run(test, t0)
    except Exception as e:
        register_failure(load_state(), f"{type(e).__name__}: {str(e)[:150]}", t0)


def run_cron(test=False):
    """Railway Cron entry: one check, then the process exits (stops billing)."""
    ok, why = should_run(load_state(), ignore_due=True)
    print(f"cron: run={ok} ({why}) local={now_local():%H:%M}")
    if ok:
        run_once(test)


def run_debug():
    content, final_url, cookies = fetch_page(parse_cookies(get_raw_cookies()), debug=True)
    print("URL after load :", final_url)
    print("Session OK     :", session_ok(final_url, cookies))
    posts, stories, blobs = extract_posts(content)
    print("JSON blobs     :", blobs)
    print("Story nodes    :", stories)
    print("Posts parsed   :", len(posts))
    for p in posts[:MAX_POSTS]:
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(p["ts"]))
        print(f"- {when} | {p['id']} | {p['text'][:60]!r} | imgs={len(p['images'])}")
    print("Saved debug.png and debug.html")


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
    if not PAGE_ID or not get_raw_cookies():
        sys.exit("Need FB_PAGE_ID and FB_COOKIES (or a cookies.txt file).")
    if a.debug:
        return run_debug()
    if not TOKEN or not CHAT_ID:
        sys.exit("Need TG_TOKEN and TG_CHAT_ID.")
    test = a.test or env("TEST") == "1"
    if a.cron:
        return run_cron(test)
    if a.once:
        return run_once(test)
    loop(test)


if __name__ == "__main__":
    main()
