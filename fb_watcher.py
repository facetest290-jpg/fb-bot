#!/usr/bin/env python3
"""
Facebook pages -> Telegram watcher (Playwright + cookies), multi-page.
Built for Railway Cron (one short run, then exit) + a one-time WATCH mode.

=====================================================================
 1) REQUIRED
=====================================================================
    TG_TOKEN, TG_CHAT_ID, FB_COOKIES (or cookies.txt), FB_PAGES

=====================================================================
 2) PAGES
=====================================================================
    FB_PAGES  one or more pages, separated by new lines, commas or ';'
              each item:  page  or  page|chat_id  or  page|chat_id|label
              page    = numeric id, username, or full facebook URL
              chat_id = (optional) send this page to a different channel
              label   = (optional) name shown above the post (default: page)
              examples:  100064825534678
                         somepage|@my_channel|Some Page
    FB_PAGE_ID  old single-page variable, still works if FB_PAGES is empty

=====================================================================
 3) REPORTS  (status messages - NOT the posts themselves)
=====================================================================
    REPORTS=on|off   (default on)
        off = the bot ONLY sends the posts. No run reports, no watch plan /
              progress messages, no "run failed" warnings.
              Critical 🚨 alerts (cookies expired, blocked, crash) still arrive,
              and command replies (/status ...) still work.
        on  = everything below is sent.
    TG_OWNER_ID     owner's private chat: reports + alerts go here
    TG_ADMIN_IDS    extra Telegram user ids allowed to send commands
    GROUP_COMMANDS  on (default) = anyone in the destination group can send
                    commands; off = only the owner / TG_ADMIN_IDS
    REPORT_ALWAYS=1 cron mode: report after every run (default: only when
                    something happened)
    MSG_LANG        ar (default) / en - language of the WATCH-mode messages

=====================================================================
 4) CRON MODE
=====================================================================
    SCROLLS          scroll steps per page (default 2)
    CHECK_EVERY_MIN  minutes between cron runs, display only (default 20)
    RUN_BUDGET       max seconds per run before skipping remaining pages (300)
    IDLE_TIMEOUT     browser silent this long = hung -> kill + retry (75)
    HARD_TIMEOUT     last-resort kill of a run that never ends
    QUIET_HOURS=off  disable the 22:00-08:00 quiet window (testing)
    STATE_DIR        folder for state.json - Railway Volume (e.g. /data)
    TG_OWNER_ID (optional), FB_USER_AGENT (optional), BOT_TZ (Africa/Cairo)

=====================================================================
 5) WATCH MODE (one-time event, FIRST page of FB_PAGES only)
=====================================================================
    WATCH_MODE=1   keeps ONE browser open and checks the page very often
                   around a known release time, sends the new post the moment
                   it appears, then stops. Times are Cairo time (BOT_TZ).

    As soon as the process starts it sends a PLAN message: page id, start
    time, minutes remaining, and every phase (normal / fast / normal / end).
    Once started it does NOT exit until its mission is done:
        - a (matching) new post was found and sent, or
        - WATCH_END was reached, or
        - the session was blocked (cookies expired).

    WATCH_START=09:57:00      first check
    WATCH_FAST_FROM=09:59:40  from here: fast checks
    WATCH_FAST_TO=10:02:00    from here: normal checks again
    WATCH_END=10:30:00        hard stop if nothing was found
    WATCH_SLOW_SEC=30  WATCH_FAST_SEC=10   seconds between checks
    WATCH_DATE=2026-10-20     (optional) only run on this day (YYYY-MM-DD)
    WATCH_KEYWORD=form,تسجيل  (optional) stop only when a new post contains one
    WATCH_LEAD_MIN=0          0 = start waiting whenever the cron fires (default).
                              N = exit if the cron fires more than N min early.
    WATCH_WARMUP_SEC=60       open the browser this many seconds before the start
    WATCH_FETCH_TIMEOUT=25    a check that takes longer = hung browser -> restart
    WATCH_TEST=1              test: treat the newest existing post as "new"
    /pause /resume /watch work while it runs (read every few seconds).

=====================================================================
 6) RUN MODES (command line)
=====================================================================
    --cron    ONE run over all pages, then exit (Railway Cron)
              (with WATCH_MODE=1 it runs the watch mode instead)
    --gate    decide if a check is due (no browser) -> GITHUB_OUTPUT
    --once    run one check now (ignores the schedule)
    --test    with --once/--cron: send the latest post of the first page
              (env TEST=all: the latest post of every page)
    --debug   diagnose only: no Telegram, saves debug_N.png / debug_N.html
    (none)    loop forever
"""
import argparse
import faulthandler
import hashlib
import html
import json
import multiprocessing
import os
import queue
import random
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

BASE = Path(__file__).resolve().parent

try:  # show log lines immediately (a hung run would otherwise show nothing)
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass


# ################################################################
# 1) CONFIG
# ################################################################
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

OFF_VALUES = ("off", "0", "false", "no")


def env(name, default=""):
    return (os.environ.get(name) or default).strip()


def env_int(name, default, lo=None):
    try:
        v = int(env(name, str(default)))
    except ValueError:
        v = default
    return max(lo, v) if lo is not None else v


TOKEN = env("TG_TOKEN", LOCAL_TOKEN)
CHAT_ID = env("TG_CHAT_ID", LOCAL_CHAT_ID)
OWNER_ID = env("TG_OWNER_ID")
GROUP_COMMANDS = env("GROUP_COMMANDS", "on").lower() not in OFF_VALUES
ADMINS = {x for x in re.split(r"[,\s;]+", f"{env('TG_OWNER_ID')},{env('TG_ADMIN_IDS')}") if x}
TZ = ZoneInfo(env("BOT_TZ", "Africa/Cairo"))
USER_AGENT = env(
    "FB_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)

WATCH_MODE = env("WATCH_MODE") == "1"
LANG = env("MSG_LANG", "ar").lower()           # language of the watch messages

QUIET_START, QUIET_END = 22, 8                  # no requests from 22:00 to 08:00
CHECK_EVERY_MIN = env_int("CHECK_EVERY_MIN", 20, lo=1)
CHECK_GAP = CHECK_EVERY_MIN * 60                # gap between runs (seconds)
GATE_TOLERANCE = 10 * 60
MAX_POSTS = 10                                  # newest posts read per page
MAX_FAILS = 3                                   # consecutive failures before stopping
TEST_ALL = env("TEST").lower() == "all"         # TEST=all -> test post from EVERY page
SEEN_KEEP = 200                                 # remembered post ids per page
RUN_BUDGET = env_int("RUN_BUDGET", 300)
SCROLLS = env_int("SCROLLS", 2, lo=1)
IDLE_TIMEOUT = env_int("IDLE_TIMEOUT", 75)
HARD_TIMEOUT = env_int("HARD_TIMEOUT", RUN_BUDGET + 2 * IDLE_TIMEOUT + 60)
WATCH_SETTLE_MS = env_int("WATCH_SETTLE_MS", 400)


def reports_enabled():
    """REPORTS=off -> the bot only sends posts (plus critical 🚨 alerts)."""
    return env("REPORTS", "on").lower() not in OFF_VALUES


def T(en, ar):
    """Pick the text in the configured language (MSG_LANG)."""
    return ar if LANG == "ar" else en


# ################################################################
# 2) PAGES
# ################################################################
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
        item = item.strip().strip("\"'").strip()  # tolerate pasted quotes
        if not item:
            continue
        parts = [x.strip().strip("\"'").strip() for x in item.split("|")]
        spec = parts[0]
        if not spec or spec in keys:
            continue
        keys.add(spec)
        short = re.sub(r"^https?://(www\.|m\.)?facebook\.com/", "", spec).strip("/")
        pages.append({
            "key": spec,
            "id": short,   # shown in the private (owner) messages
            "url": page_url(spec),
            "chat": (parts[1] if len(parts) > 1 and parts[1] else CHAT_ID),
            "label": (parts[2] if len(parts) > 2 and parts[2] else short),
        })
    return pages


PAGES = parse_pages()


# ################################################################
# 3) COOKIES
# ################################################################
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


# ################################################################
# 4) TELEGRAM  (posts / reports / alerts)
# ################################################################
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


def tg_json(method, http_timeout=30, **payload):
    """Like tg() but returns the parsed JSON answer (or None)."""
    global LAST_TG_ERROR
    data = urllib.parse.urlencode(payload).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}", data=data)
    try:
        with urllib.request.urlopen(req, timeout=http_timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        LAST_TG_ERROR = f"HTTP {e.code}"
        print("Telegram error:", LAST_TG_ERROR, "(" + method + ")")
        return None
    except Exception as e:
        LAST_TG_ERROR = type(e).__name__
        print("Telegram error:", LAST_TG_ERROR, "(" + method + ")")
        return None


# ---- posts: always sent, REPORTS never affects them ----
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


# ---- reports / status messages: silenced by REPORTS=off ----
def notify(msg):
    """Status message to the owner's private chat (or the main chat)."""
    if not reports_enabled():
        print("[reports off]", msg.replace("\n", " | "))
        return
    target = OWNER_ID or CHAT_ID
    if target:
        tg("sendMessage", chat_id=target, text=msg)
    else:
        print("[no TG_OWNER_ID / TG_CHAT_ID]", msg.replace("\n", " | "))


def send_report(st, text):
    """Reports go where the /reports_* commands say:
    default = owner chat (or the main chat if no owner), 'chat' = the chat that
    sent /reports_on, 'off' = never the group (owner chat only)."""
    if not reports_enabled():
        print("[reports off]", text.replace("\n", " | "))
        return
    mode = st.get("report_mode")
    if mode == "chat" and st.get("report_chat"):
        extra = ({"message_thread_id": st["report_thread"]}
                 if st.get("report_thread") else {})
        tg("sendMessage", chat_id=st["report_chat"], text=text, **extra)
    elif mode == "off":
        if OWNER_ID:
            tg("sendMessage", chat_id=OWNER_ID, text=text)
    else:
        notify(text)


# ---- alerts: critical, ALWAYS sent ----
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


# ################################################################
# 5) STATE
# ################################################################
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


# ################################################################
# 6) SCHEDULE
# ################################################################
def now_local():
    return datetime.now(TZ)


def quiet_disabled():
    return env("QUIET_HOURS").lower() in OFF_VALUES


def in_quiet(dt):
    if quiet_disabled():
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
    if st.get("paused"):
        return False, "paused (/resume to continue)"
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


# ################################################################
# 7) PARSING  (Facebook JSON -> posts)
# ################################################################
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


# ################################################################
# 8) BROWSER  (cron mode: one browser, all pages)
# ################################################################
BLOCKED_TYPES = {"image", "media", "font"}   # not needed: we parse the JSON
LOGIN_MARKS = ("login", "checkpoint", "two_step", "recover", "disabled")

CHROMIUM_ARGS = ["--no-sandbox",
                 "--disable-dev-shm-usage",
                 "--disable-gpu",
                 "--disable-extensions",
                 "--disable-background-networking",
                 "--disable-component-update",
                 "--disable-sync",
                 "--mute-audio",
                 "--no-first-run",
                 "--renderer-process-limit=1",
                 "--disable-features=IsolateOrigins,site-per-process,TranslateUI",
                 "--disable-blink-features=AutomationControlled"]


def url_is_blocked(url):
    low = url.lower()
    return any(x in low for x in LOGIN_MARKS)


def fetch_pages(cookie_list, pages, debug=False, on_result=None, budget=None):
    """One browser, one session, all pages one after the other.
    Returns (results, cookies, session_ok). results = [{page, content, error}]."""
    from playwright.sync_api import sync_playwright  # lazy: gate needs no deps

    budget = RUN_BUDGET if budget is None else budget
    results, cookies, session = [], [], True
    started = time.time()

    def emit(res):
        results.append(res)
        if on_result:
            on_result(res)

    time.sleep(random.uniform(1, 4))
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel=env("BROWSER_CHANNEL") or None,
            headless=True,
            timeout=60000,
            args=CHROMIUM_ARGS)
        try:
            ctx = browser.new_context(
                locale="en-GB",
                timezone_id=env("BOT_TZ", "Africa/Cairo"),
                viewport={"width": 1366, "height": 900},
                user_agent=USER_AGENT,
            )
            ctx.set_default_timeout(30000)
            ctx.set_default_navigation_timeout(45000)
            ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            if not debug:  # keep screenshots intact in debug mode
                ctx.route("**/*", lambda r: r.abort()
                          if r.request.resource_type in BLOCKED_TYPES
                          else r.continue_())
            ctx.add_cookies(cookie_list)
            for i, pg in enumerate(pages):
                if i:
                    if time.time() - started > budget:
                        print(f"Run budget ({budget}s) reached; "
                              f"skipping {len(pages) - i} page(s) until next run.")
                        break
                    time.sleep(random.uniform(3, 7))
                res, blocked = None, False
                for attempt in (1, 2):
                    # a NEW tab per page, closed afterwards: frees the memory of
                    # the previous page and a crashed tab cannot affect the next one
                    tab = ctx.new_page()
                    try:
                        tab.goto(pg["url"], wait_until="domcontentloaded", timeout=60000)
                        tab.wait_for_timeout(random.randint(3000, 5000))
                        if url_is_blocked(tab.url):
                            blocked = True
                            break
                        for _ in range(SCROLLS):
                            tab.mouse.wheel(0, random.randint(900, 1600))
                            tab.wait_for_timeout(random.randint(1200, 2200))
                        content = tab.content()
                        if debug:
                            tab.screenshot(path=str(BASE / f"debug_{i + 1}.png"))
                            (BASE / f"debug_{i + 1}.html").write_text(
                                content, encoding="utf-8")
                        res = {"page": pg, "content": content, "error": None}
                        break
                    except Exception as e:
                        res = {"page": pg, "content": None,
                               "error": f"{type(e).__name__}: {str(e)[:100]}"}
                        crashed = any(w in str(e).lower() for w in ("crash", "closed"))
                        if attempt == 1 and crashed:
                            print(f"[{pg['id']}] tab crashed, retrying once in a new tab")
                            continue
                        break
                    finally:
                        try:
                            tab.close()
                        except Exception:
                            pass
                if blocked:
                    session = False
                    break
                emit(res)
            cookies = ctx.cookies("https://www.facebook.com")
        finally:
            browser.close()
    if "c_user" not in {c["name"] for c in cookies}:
        session = False
    return results, cookies, session


def _fetch_worker(q, cookie_list, pages, budget):
    """Child process: runs the browser and reports every finished page."""
    try:
        os.setsid()  # own process group, so the parent can kill Chromium too
    except Exception:
        pass
    try:
        _, cookies, session = fetch_pages(
            cookie_list, pages, on_result=lambda r: q.put(("page", r)), budget=budget)
        q.put(("done", cookies, session))
    except Exception as e:
        q.put(("error", f"{type(e).__name__}: {str(e)[:150]}"))


def _kill_group(proc):
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    try:
        proc.join(5)
    except Exception:
        pass


def fetch_pages_safe(cookie_list, pages):
    """Run the browser in a child process. If it stays silent for IDLE_TIMEOUT
    seconds (hung Chromium) it is killed and the unfinished pages are retried
    once. Returns (results, cookies, session_ok, hangs)."""
    ctx = multiprocessing.get_context("fork")
    started = time.time()
    results, cookies, session, hangs, last_error = [], [], True, 0, None
    remaining = list(pages)
    for attempt in range(2):
        if not remaining:
            break
        budget = max(30, RUN_BUDGET - (time.time() - started))
        q = ctx.Queue()
        proc = ctx.Process(target=_fetch_worker,
                           args=(q, cookie_list, remaining, budget), daemon=True)
        proc.start()
        got, hung = set(), False
        while True:
            try:
                msg = q.get(timeout=IDLE_TIMEOUT)
            except queue.Empty:
                hung = True
                break
            if msg[0] == "page":
                results.append(msg[1])
                got.add(msg[1]["page"]["key"])
            elif msg[0] == "done":
                cookies, session = msg[1], msg[2]
                break
            else:  # "error"
                last_error = msg[1]
                break
        _kill_group(proc)  # also removes any Chromium left behind
        if hung:
            hangs += 1
            print(f"Browser silent for {IDLE_TIMEOUT}s (attempt {attempt + 1}); "
                  "killed it" + (" and retrying the unfinished pages." if attempt == 0 else "."))
            remaining = [pg for pg in remaining if pg["key"] not in got]
            if len(remaining) > 1:  # the page that hung goes last on the retry
                remaining = remaining[1:] + remaining[:1]
            continue
        break  # finished or errored: no retry
    if last_error and not results:
        raise RuntimeError(last_error)
    return results, cookies, session, hangs


# ################################################################
# 9) CRON RUN  (one run over all pages)
# ################################################################
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


def _reset_if_new_cookies(st):
    """New FB_COOKIES (or RESET=1) = clear the 'blocked' flag and the fail counters."""
    if os.environ.get("RESET") == "1" or st.get("cookie_hash") != cookie_hash():
        st.update({"blocked": False, "fails": 0, "cookies": None,
                   "cookie_hash": cookie_hash()})
        st.pop("block_reason", None)
        for ps in st.get("pages", {}).values():
            ps["fails"] = 0
        return True
    return False


def _run(test, t0):
    st = load_state()
    st.pop("seen", None)  # legacy single-page key (now stored per page)
    _reset_if_new_cookies(st)
    pstate = st.setdefault("pages", {})

    # least recently checked first, so a time-limited run never starves a page
    order = sorted(PAGES, key=lambda p: pstate.get(p["key"], {}).get("last_check", 0))

    cookie_list = st.get("cookies") or parse_cookies(get_raw_cookies())
    results, cookies, session, hangs = fetch_pages_safe(cookie_list, order)

    if hangs and not results:  # browser hung on every attempt, nothing was read
        alert_throttled("hang", "⏱ The browser hung on every attempt in this run and "
                                "was killed. Nothing was read; the next run retries.",
                        hours=1)
        st["next_due"] = plan_next(t0)
        save_state(st)
        return

    if not session:
        return block(st, "Cookies expired or Facebook asked for login / identity "
                         "confirmation (checkpoint).", t0)

    ok_pages, test_done = 0, False
    lines, new_total, important = [], 0, False
    for r in results:
        pg = r["page"]
        pid = pg["id"]  # owner messages show the page ID (posts in the chat show the label)
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
            print(f"[{pid}] failure #{ps['fails']}: {reason}")
            lines.append(f"⚠️ {pid}: FAILED {ps['fails']}/{MAX_FAILS} ({reason})")
            important = True
            if ps["fails"] == MAX_FAILS:
                alert(f"Page {pid} failed {MAX_FAILS} times in a row.\n"
                      f"{pg['url']}\nLast reason: {reason}\n"
                      "The bot keeps trying it on every run.")
            continue

        ok_pages += 1
        ps["fails"] = 0
        seen = ps.get("seen")
        activated = seen is None
        if activated:  # first time we see this page: remember, send nothing
            seen = ps["seen"] = [p["id"] for p in reversed(posts)]
            lines.append(f"🟢 {pid}: activated, {len(posts)} current post(s) saved "
                         f"(not sent), new ones go to {pg['chat']}")
            important = True
        do_test = test and (TEST_ALL or not test_done)
        if do_test:
            test_done = True
            important = True
            if send_post(posts[0], pg):
                if posts[0]["id"] not in seen:
                    seen.append(posts[0]["id"])
                lines.append(f"🧪 {pid}: test post sent")
            else:
                lines.append(f"⚠️ {pid}: test post could not be sent "
                             f"(Telegram: {LAST_TG_ERROR or 'unknown error'})")
        if activated:
            continue  # nothing else to send on the activation run

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
        if new:
            important = True
        line = f"{pid}: {len(posts)} read, {len(new)} new, {sent} sent"
        if sent < len(new):
            line = "⚠️ " + line + (f" ({len(new) - sent} will retry next run; "
                                   f"Telegram: {LAST_TG_ERROR or 'unknown error'})")
        lines.append(line)

    if results and ok_pages == 0:
        return register_failure(
            st, "Session is valid but no page could be read.", t0)

    if cookies:  # empty if the browser was killed before it could report them
        st["cookies"] = normalize_cookies(cookies)  # keep the session fresh
    st["fails"] = 0
    st["next_due"] = plan_next(t0)
    st["last_ok"] = int(time.time())
    nd = datetime.fromtimestamp(st["next_due"], TZ)
    print(f"Run OK: {ok_pages}/{len(order)} page(s) read, {new_total} new post(s).")

    if hangs:
        lines.append(f"⏱ browser hung {hangs} time(s) and was restarted")
        important = True

    report = (f"📋 Run report ({now_local():%Y-%m-%d %H:%M} {TZ.key})\n"
              f"📄 Pages checked: {', '.join(p['id'] for p in order)}\n"
              + "\n".join(lines) + f"\nNext run: about {nd:%H:%M}")
    st["last_report"] = {"text": report, "ts": int(time.time())}  # for /last
    save_state(st)

    # ONE message per run, covering all pages (only when something happened)
    if important or env("REPORT_ALWAYS") == "1":
        send_report(st, report)


RUN_FLAG = _state_dir / "running.flag"


def _watchdog_fire():
    """The run is hung (e.g. Chromium froze). Cron would skip every later
    schedule while this process is alive, so tell the owner and exit hard."""
    print(f"WATCHDOG: run exceeded {HARD_TIMEOUT}s - killing the process.")
    try:
        RUN_FLAG.unlink()
    except Exception:
        pass
    try:
        alert_throttled("hang", f"⏱ A run hung for more than {HARD_TIMEOUT}s and was "
                                "force-stopped so the next cron runs are not blocked. "
                                "If this repeats, check Railway Metrics (memory).",
                        hours=1)
    except Exception:
        pass
    os._exit(3)


def run_once(test=False):
    t0 = time.time()
    watchdog = threading.Timer(HARD_TIMEOUT, _watchdog_fire)
    watchdog.daemon = True
    watchdog.start()
    faulthandler.dump_traceback_later(max(10, HARD_TIMEOUT - 5), exit=False)
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
        watchdog.cancel()
        faulthandler.cancel_dump_traceback_later()
        try:
            RUN_FLAG.unlink()
        except Exception:
            pass


# ################################################################
# 10) TELEGRAM COMMANDS
# ################################################################
HELP_TEXT = (
    "🤖 Commands\n"
    "/status - bot state\n"
    "/last - the last run report\n"
    "/watch - watch mode plan (times, phases, page)\n"
    "/reports_on - send reports to THIS chat\n"
    "/reports_off - stop sending reports to the group (owner chat only)\n"
    "/pages - watched pages\n"
    "/pause - stop checking Facebook (also pauses watch mode)\n"
    "/resume - continue checking\n"
    "/help - this list\n\n"
    f"In cron mode commands are read at the start of each run (every ~{CHECK_EVERY_MIN} "
    "min), so the answer is not instant. In watch mode they are read every few seconds.")


def ago(ts):
    if not ts:
        return "never"
    d = int(time.time() - ts)
    if d < 90:
        return f"{d}s ago"
    if d < 5400:
        return f"{d // 60} min ago"
    if d < 172800:
        return f"{d // 3600} h ago"
    return f"{d // 86400} d ago"


def _reply(msg, text):
    extra = {}
    if msg.get("is_topic_message") and msg.get("message_thread_id"):
        extra["message_thread_id"] = msg["message_thread_id"]
    tg("sendMessage", chat_id=msg["chat"]["id"], text=text, **extra)


def _is_me(st, name):
    """Is '/cmd@name' addressed to this bot? (several bots can share a group)"""
    if not st.get("bot_username"):
        r = tg_json("getMe")
        if r and r.get("ok"):
            st["bot_username"] = (r["result"].get("username") or "").lower()
    return not st.get("bot_username") or name.lower() == st["bot_username"]


def _status_text(st):
    if st.get("blocked"):
        state = f"⛔ blocked ({st.get('block_reason', 'unknown reason')[:80]})"
    elif st.get("paused"):
        state = "⏸ paused"
    else:
        state = "▶️ running"
    if not reports_enabled():
        rep_txt = "OFF (REPORTS=off: posts only)"
    else:
        mode = st.get("report_mode")
        if mode == "chat":
            rep_txt = "to the chat that sent /reports_on"
        elif mode == "off":
            rep_txt = "owner chat only (group muted)" if OWNER_ID else "off (no owner set)"
        else:
            rep_txt = "owner chat" if OWNER_ID else "main chat (no owner set)"
    quiet = "off" if quiet_disabled() else f"{QUIET_START}:00-{QUIET_END}:00"
    failing = [pg["id"] for pg in PAGES
               if st.get("pages", {}).get(pg["key"], {}).get("fails", 0)]
    w = st.get("watch")
    watch_txt = ""
    if w and w.get("end", 0) > time.time() - 3600:
        watch_txt = (f"Watch: {w.get('phase', '?')} (every {w.get('interval', '?')}s), "
                     f"last check {ago(w.get('last_check'))}, {w.get('checks', 0)} checks, "
                     f"{w.get('restarts', 0)} browser restarts\n")
    return (f"📊 Status\n{state}\n" + watch_txt +
            f"Last OK run: {ago(st.get('last_ok'))}\n"
            f"Pages: {len(PAGES)}" + (f" (failing: {', '.join(failing)})" if failing else "") + "\n"
            f"Reports: {rep_txt}\n"
            f"Quiet hours: {quiet}")


def _in_allowed_group(chat):
    """True if the message came from one of the groups the bot posts to."""
    if not GROUP_COMMANDS or chat.get("type") not in ("group", "supergroup"):
        return False
    dests = {CHAT_ID.lower()} | {p["chat"].lower() for p in PAGES}
    username = (chat.get("username") or "").lower()
    return (str(chat.get("id")).lower() in dests
            or (username and f"@{username}" in dests))


def watch_info_text():
    if not WATCH_MODE:
        return T("Watch mode is OFF (set WATCH_MODE=1 to use it).",
                 "وضع الواتش مقفول (فعّله بـ WATCH_MODE=1).")
    try:
        return watch_plan_text(watch_config(), PAGES[0])
    except ValueError as e:
        return T(f"Watch settings problem: {e}", f"في مشكلة في إعدادات الواتش: {e}")


def process_command(st, msg):
    """Returns 1 if the message was a command from an allowed sender."""
    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return 0
    sender = str((msg.get("from") or {}).get("id", ""))
    if sender not in ADMINS and not _in_allowed_group(msg["chat"]):
        return 0  # silently ignore everybody else
    cmd, _, target = text.split()[0][1:].partition("@")
    if target and not _is_me(st, target):
        return 0
    cmd = cmd.lower()
    chat = msg["chat"]["id"]

    if cmd in ("help", "start"):
        _reply(msg, HELP_TEXT)
    elif cmd == "status":
        _reply(msg, _status_text(st))
    elif cmd in ("last", "report"):
        lr = st.get("last_report")
        _reply(msg, f"{lr['text']}\n\n🕒 {ago(lr['ts'])}" if lr
               else "No report yet - it is created at the end of the next run.")
    elif cmd == "watch":
        _reply(msg, watch_info_text())
    elif cmd == "pages":
        rows = []
        for pg in PAGES:
            ps = st.get("pages", {}).get(pg["key"], {})
            rows.append(f"{pg['id']} ({pg['label']}): fails {ps.get('fails', 0)}, "
                        f"checked {ago(ps.get('last_check'))}, to {pg['chat']}")
        _reply(msg, "📄 Pages\n" + "\n".join(rows))
    elif cmd == "reports_on":
        st["report_mode"] = "chat"
        st["report_chat"] = chat
        st["report_thread"] = (msg.get("message_thread_id")
                               if msg.get("is_topic_message") else None)
        note = ("" if reports_enabled() else
                "\n⚠️ But REPORTS=off is set in the variables, so nothing is sent "
                "until you set REPORTS=on.")
        _reply(msg, "✅ Reports will be sent to this chat. "
                    "(🚨 alerts keep going to the owner chat.)" + note)
    elif cmd == "reports_off":
        st["report_mode"] = "off"
        _reply(msg, "🔕 Reports are no longer sent to the group."
                    + (" They go to the owner chat." if OWNER_ID
                       else " No owner is set, so nobody receives them."))
    elif cmd == "pause":
        st["paused"] = True
        _reply(msg, "⏸ Paused. No Facebook requests until /resume. "
                    "Commands still work.")
    elif cmd == "resume":
        st["paused"] = False
        _reply(msg, "▶️ Resumed." if WATCH_MODE
               else "▶️ Resumed. The next scheduled run will check the pages.")
    else:
        return 0
    return 1


def commands_enabled():
    return bool(TOKEN and (ADMINS or GROUP_COMMANDS))


def handle_commands(wait=0):
    """Read pending Telegram commands (cheap: one getUpdates call, no browser).
    wait > 0 = long polling: returns as soon as a message arrives, or after
    `wait` seconds (used as the sleep between checks in watch mode)."""
    if not commands_enabled():
        return
    st = load_state()
    offset, changed = st.get("tg_offset"), False
    for i in range(10):  # a busy group can have several pages of updates
        payload = {"timeout": wait if i == 0 else 0, "limit": 100,
                   "allowed_updates": json.dumps(["message"])}
        if offset is not None:
            payload["offset"] = offset
        resp = tg_json("getUpdates", http_timeout=(wait if i == 0 else 0) + 20,
                       **payload)
        if not resp or not resp.get("ok"):
            break
        updates = resp.get("result", [])
        if not updates:
            break
        for u in updates:
            offset, changed = u["update_id"] + 1, True
            msg = u.get("message")
            if msg:
                try:
                    process_command(st, msg)
                except Exception as e:
                    print("command error:", type(e).__name__, str(e)[:100])
        if len(updates) < 100:
            break
    if changed:
        st["tg_offset"] = offset
        save_state(st)


# ################################################################
# 11) WATCH MODE
# ################################################################
# ---- 11a) browser child process --------------------------------
def _watch_child(reqq, respq, cookie_list, url):
    """Child process: keeps ONE Chromium + ONE tab open and reloads the page
    every time the parent asks. If this process hangs the parent kills it."""
    try:
        os.setsid()  # own process group, so Chromium dies with it
    except Exception:
        pass
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(
                channel=env("BROWSER_CHANNEL") or None, headless=True,
                timeout=60000, args=CHROMIUM_ARGS)
            try:
                ctx = browser.new_context(
                    locale="en-GB", timezone_id=env("BOT_TZ", "Africa/Cairo"),
                    viewport={"width": 1366, "height": 900}, user_agent=USER_AGENT)
                ctx.set_default_timeout(15000)
                ctx.set_default_navigation_timeout(20000)
                ctx.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
                ctx.route("**/*", lambda r: r.abort()
                          if r.request.resource_type in BLOCKED_TYPES
                          else r.continue_())
                ctx.add_cookies(cookie_list)
                tab = ctx.new_page()
                respq.put(("ready",))
                n = 0
                while True:
                    cmd = reqq.get()
                    if cmd == "stop":
                        try:
                            ck = ctx.cookies("https://www.facebook.com")
                        except Exception:
                            ck = []
                        respq.put(("cookies", ck))
                        break
                    n += 1
                    if n % 40 == 0:  # a fresh tab now and then keeps memory flat
                        try:
                            tab.close()
                        except Exception:
                            pass
                        tab = ctx.new_page()
                    try:
                        tab.goto(url, wait_until="domcontentloaded", timeout=20000)
                        tab.wait_for_timeout(WATCH_SETTLE_MS)
                        respq.put(("page", tab.content(), tab.url))
                    except Exception as e:
                        respq.put(("err", f"{type(e).__name__}: {str(e)[:100]}"))
                        try:
                            tab.close()
                        except Exception:
                            pass
                        try:
                            tab = ctx.new_page()
                        except Exception:
                            pass
            finally:
                browser.close()
    except Exception as e:
        try:
            respq.put(("fatal", f"{type(e).__name__}: {str(e)[:120]}"))
        except Exception:
            pass


class WatchBrowser:
    """Parent-side handle of the browser child process."""

    def __init__(self, cookie_list, url):
        mp = multiprocessing.get_context("fork")
        self.reqq, self.respq = mp.Queue(), mp.Queue()
        self.proc = mp.Process(target=_watch_child, daemon=True,
                               args=(self.reqq, self.respq, cookie_list, url))
        self.proc.start()

    def _get(self, timeout):
        """Next answer, ('dead',) if the child died, or None on timeout."""
        deadline = time.time() + timeout
        while True:
            try:
                return self.respq.get(timeout=max(0.05, min(1.0, deadline - time.time())))
            except queue.Empty:
                if not self.proc.is_alive():
                    return ("dead",)
                if time.time() >= deadline:
                    return None

    def wait_ready(self, timeout=60):
        msg = self._get(timeout)
        return bool(msg) and msg[0] == "ready"

    def fetch(self, timeout):
        self.reqq.put("fetch")
        return self._get(timeout)

    def stop(self):
        """Polite stop; returns the browser's cookies (or None)."""
        cookies = None
        try:
            self.reqq.put("stop")
            msg = self._get(10)
            if msg and msg[0] == "cookies":
                cookies = msg[1]
        except Exception:
            pass
        self.kill()
        return cookies

    def kill(self):
        _kill_group(self.proc)


make_watch_browser = WatchBrowser   # (tests replace this)


# ---- 11b) settings + time helpers --------------------------------
def _parse_hms(txt, name):
    try:
        parts = [int(x) for x in str(txt).strip().split(":")]
        if not 1 <= len(parts) <= 3:
            raise ValueError
        while len(parts) < 3:
            parts.append(0)
        h, m, sec = parts
        if not (0 <= h < 24 and 0 <= m < 60 and 0 <= sec < 60):
            raise ValueError
        return h, m, sec
    except ValueError:
        raise ValueError(f"{name} must look like HH:MM or HH:MM:SS (got {txt!r})")


def watch_config():
    """All watch settings, times converted to timestamps for today (Cairo time)."""
    base = now_local()

    def at(name, default):
        h, m, sec = _parse_hms(env(name) or default, name)
        return base.replace(hour=h, minute=m, second=sec, microsecond=0)

    def num(name, default, lo=1):
        try:
            return max(lo, int(env(name) or default))
        except ValueError:
            raise ValueError(f"{name} must be a whole number (got {env(name)!r})")

    start = at("WATCH_START", "09:57:00")
    fast_from = at("WATCH_FAST_FROM", "09:59:40")
    fast_to = at("WATCH_FAST_TO", "10:02:00")
    end = at("WATCH_END", "10:30:00")
    if end <= start:
        end += timedelta(days=1)
    return {
        "start": start.timestamp(), "fast_from": fast_from.timestamp(),
        "fast_to": fast_to.timestamp(), "end": end.timestamp(),
        "slow_sec": num("WATCH_SLOW_SEC", 30), "fast_sec": num("WATCH_FAST_SEC", 10),
        "lead": num("WATCH_LEAD_MIN", 0, 0) * 60,        # 0 = no limit
        "warmup": num("WATCH_WARMUP_SEC", 60, 0),
        "fetch_timeout": num("WATCH_FETCH_TIMEOUT", 25, 5),
        "keywords": [k.strip().lower() for k in env("WATCH_KEYWORD").split(",") if k.strip()],
    }


def watch_phase(cfg, ts):
    if cfg["fast_from"] <= ts < cfg["fast_to"]:
        return "fast", cfg["fast_sec"]
    return "slow", cfg["slow_sec"]


def _hms(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%H:%M:%S")


def _dur(sec):
    """Seconds -> '44 دقيقة و 30 ثانية' / '44 min 30 s'."""
    sec = max(0, int(round(sec)))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if LANG == "ar":
        units, joiner = (("ساعة", h), ("دقيقة", m), ("ثانية", s)), " و "
    else:
        units, joiner = (("h", h), ("min", m), ("s", s)), " "
    parts = [f"{v} {n}" for n, v in units if v]
    return joiner.join(parts) if parts else T("0 s", "0 ثانية")


# ---- 11c) the plan message ---------------------------------------
def watch_plan_text(cfg, pg, now=None):
    """The 'what will happen and when' message, sent the moment watch mode starts
    (and by /watch). Works for any combination of times."""
    now = time.time() if now is None else now
    L = [T("🛰 WATCH MODE is ON", "🛰 وضع الواتش (Watch) شغّال"), ""]
    L.append(T(f"📄 Page: {pg['id']} ({pg['label']})",
               f"📄 الصفحة: {pg['id']} ({pg['label']})"))
    L.append(f"🔗 {pg['url']}")
    L.append(T(f"📨 New posts go to: {pg['chat']}",
               f"📨 البوستات الجديدة هتتبعت على: {pg['chat']}"))
    L.append(T(f"🕒 Now: {_hms(now)} ({TZ.key})",
               f"🕒 الوقت دلوقتي: {_hms(now)} ({TZ.key})"))
    L.append("")

    # --- countdown
    if now < cfg["start"]:
        rem = cfg["start"] - now
        extra = (T(f" (= {int(rem // 60)} min)", f" (= {int(rem // 60)} دقيقة)")
                 if rem >= 3600 else "")
        L.append(T(f"⏳ Checking starts at {_hms(cfg['start'])}",
                   f"⏳ الفحص هيبدأ الساعة {_hms(cfg['start'])}"))
        L.append(T(f"   Time left: {_dur(rem)}{extra}",
                   f"   باقي: {_dur(rem)}{extra}"))
        if cfg["warmup"]:
            warm = cfg["start"] - cfg["warmup"]
            L.append(T(f"🔧 The browser opens at {_hms(warm)} ({cfg['warmup']}s before) "
                       "and saves the posts already on the page, so they are not re-sent.",
                       f"🔧 المتصفح هيتفتح الساعة {_hms(warm)} (قبلها بـ {cfg['warmup']} ثانية) "
                       "وهيحفظ البوستات الموجودة أصلًا عشان ميبعتهاش تاني."))
    elif now < cfg["end"]:
        L.append(T(f"▶️ The checking window already started ({_hms(cfg['start'])}), "
                   "checking now.",
                   f"▶️ وقت الفحص بدأ بالفعل ({_hms(cfg['start'])}) والبوت بيفحص دلوقتي."))
    L.append("")

    # --- phases
    pts = sorted({cfg["start"], cfg["end"]} |
                 {t for t in (cfg["fast_from"], cfg["fast_to"])
                  if cfg["start"] < t < cfg["end"]})
    L.append(T("📋 Plan, in order:", "📋 الخطة بالترتيب:"))
    n = 0
    for a, b in zip(pts, pts[1:]):
        if b <= now:
            continue
        n += 1
        phase, interval = watch_phase(cfg, a)
        icon = "⚡" if phase == "fast" else "🔄"
        name = (T("FAST checks", "فحص سريع (فاست)") if phase == "fast"
                else T("Normal checks", "فحص عادي"))
        live = T("  ← running now", "  ← شغّال دلوقتي") if a <= now < b else ""
        L.append(f"{n}) {icon} {name}: {_hms(a)} → {_hms(b)} ({_dur(b - a)}) — "
                 + T(f"every {interval}s", f"كل {interval} ثانية") + live)
    L.append("")

    # --- how it ends
    L.append(T(f"🛑 Hard stop at {_hms(cfg['end'])} if nothing is found.",
               f"🛑 النهاية الساعة {_hms(cfg['end'])} لو مفيش بوست جديد."))
    if cfg["keywords"]:
        kw = ", ".join(cfg["keywords"])
        L.append(T(f"🎯 Stops when a new post containing: {kw}",
                   f"🎯 بيقف أول ما ينزل بوست جديد فيه: {kw}"))
    else:
        L.append(T("🎯 Stops as soon as a new post is found and sent.",
                   "🎯 بيقف أول ما يلاقي بوست جديد ويبعته."))
    L.append(T("   It does not stop before that (or the hard stop).",
               "   ومش هيقف قبل كده (أو قبل وقت النهاية)."))
    if env("WATCH_TEST") == "1":
        L.append(T("🧪 TEST: the newest existing post is treated as new.",
                   "🧪 تجربة: أحدث بوست موجود هيتعامل كأنه جديد."))
    L.append("")
    L.append(T("Commands: /pause  /resume  /watch  /status",
               "الأوامر: /pause  /resume  /watch  /status"))
    return "\n".join(L)


# ---- 11d) the watcher ---------------------------------------------
def _poll(secs):
    """Wait up to `secs` seconds while listening to Telegram commands.
    Returns early when something arrives, so the caller can react (e.g. /pause)."""
    if secs <= 0:
        return
    if secs >= 1.5 and commands_enabled():
        t0 = time.time()
        try:
            handle_commands(wait=int(min(secs - 0.5, 25)))
        except Exception as e:
            print("handle_commands failed:", type(e).__name__, str(e)[:100])
        if time.time() - t0 < 0.3:  # answered instantly (error?) -> do not spin
            time.sleep(min(secs, 0.5))
    else:
        time.sleep(min(secs, 25))


class Watch:
    """One-time event mode: fast checks of ONE page around a release time.
    run() returns only when the mission is finished."""

    def __init__(self, cfg, pg, cookie_list, test):
        self.cfg, self.pg, self.cookie_list, self.test = cfg, pg, cookie_list, test
        ps0 = load_state().get("pages", {}).get(pg["key"], {})
        self.browser = None
        self.baselined = ps0.get("seen") is not None
        self.checks = self.restarts = self.consec_err = 0
        self.last_warn = 0.0
        self.phase = self.interval = None
        self.paused = False
        self.found = None
        self.final = None            # found / ended / blocked
        self.test_done = False
        self.ready_sent = False
        self.next_check = 0.0

    # ---------- messages ----------
    def announce(self, text):
        """Progress messages (follow REPORTS and /reports_on /reports_off)."""
        send_report(load_state(), text)

    def warn(self, text):
        """Problems: owner chat, at most one every 20 s."""
        if time.time() - self.last_warn > 20:
            self.last_warn = time.time()
            notify(text)

    def matches(self, post):
        kws = self.cfg["keywords"]
        if not kws:
            return True
        low = (post["text"] or "").lower()
        return any(k in low for k in kws)

    def summary(self):
        return T(f"{self.checks} checks, {self.restarts} browser restart(s)",
                 f"{self.checks} فحص، {self.restarts} إعادة تشغيل للمتصفح")

    # ---------- reading the page ----------
    def save_status(self, st, phase, interval):
        st["watch"] = {"phase": phase, "interval": interval, "checks": self.checks,
                       "restarts": self.restarts, "last_check": int(time.time()),
                       "end": self.cfg["end"]}

    def handle_posts(self, posts, active):
        cfg, pg = self.cfg, self.pg
        st = load_state()
        ps = st.setdefault("pages", {}).setdefault(pg["key"], {})
        seen = ps.get("seen")
        if seen is None:  # first look at this page: remember what is already there
            old = [p for p in reversed(posts) if p["ts"] < cfg["start"]]
            seen = ps["seen"] = [p["id"] for p in old]
            print(f"watch: baseline saved ({len(seen)} post(s))")
        self.baselined = True
        if not active:  # warm-up: only remember what is already on the page
            save_state(st)
            return "ok"
        if self.test and not self.test_done:  # WATCH_TEST: newest post counts as new
            self.test_done = True
            if posts and posts[0]["id"] in seen:
                seen.remove(posts[0]["id"])
        seen_set = set(seen)
        new = sorted([p for p in posts if p["id"] not in seen_set],
                     key=lambda x: x["ts"])
        sent = []
        for p in new:
            t_send = time.time()
            if send_post(p, pg):
                seen.append(p["id"])
                sent.append((p, t_send))
            else:
                self.warn(T(
                    "⚠️ A new post was found but could not be sent to Telegram "
                    f"({LAST_TG_ERROR or 'unknown error'}). Retrying on the next check.",
                    "⚠️ لقيت بوست جديد بس معرفتش أبعته على تيليجرام "
                    f"({LAST_TG_ERROR or 'سبب غير معروف'}). هحاول تاني في الفحص الجاي."))
        ps["seen"] = seen
        ps["last_check"] = int(time.time())
        phase, interval = watch_phase(cfg, time.time())
        self.save_status(st, phase, interval)
        save_state(st)
        hit = [(p, t) for p, t in sent if self.matches(p)]
        if hit:
            p, t_send = hit[0]
            delay = t_send - p["ts"]
            if 0 <= delay < 3600:
                when = T(f"published {_hms(p['ts'])}, sent {_hms(t_send)}, delay {int(delay)}s",
                         f"اتنشر {_hms(p['ts'])}، اتبعت {_hms(t_send)}، التأخير {int(delay)} ثانية")
            else:
                when = T(f"sent {_hms(t_send)}", f"اتبعت {_hms(t_send)}")
            self.found = T(f"🎯 New post detected and sent ({when}).",
                           f"🎯 اتلقى بوست جديد واتبعت ({when}).")
            return "found"
        return "ok"

    def do_check(self, active):
        """One page load. Returns ok / found / blocked / retry / retry_soon."""
        self.checks += 1
        r = self.browser.fetch(self.cfg["fetch_timeout"])
        kind = r[0] if r else "hang"
        if kind == "page":
            _, html_text, url = r
            if url_is_blocked(url):
                block(load_state(), "Cookies expired or Facebook asked for login / "
                                    "identity confirmation (checkpoint) during watch "
                                    "mode.", time.time())
                return "blocked"
            posts = extract_posts(html_text)[0][:MAX_POSTS]
            if posts:
                self.consec_err = 0
                return self.handle_posts(posts, active)
            reason = "no posts could be read from the page"
        elif kind == "hang":
            reason = f"the browser did not answer for {self.cfg['fetch_timeout']}s"
        elif kind == "dead":
            reason = "the browser process died"
        else:  # err / fatal
            reason = r[1] if len(r) > 1 else kind
        self.consec_err += 1
        hard = kind in ("hang", "dead", "fatal") or self.consec_err >= 3
        if hard:
            self.drop_browser()
            self.restarts += 1
            self.consec_err = 0
            self.warn(T(f"⚠️ Watch: {reason}. Restarting the browser, checking continues.",
                        f"⚠️ الواتش: {reason}. بعيد تشغيل المتصفح والفحص مكمّل."))
            return "retry"
        self.warn(T(f"⚠️ Watch: check failed ({reason}). Trying again in a moment.",
                    f"⚠️ الواتش: الفحص فشل ({reason}). هحاول تاني حالًا."))
        return "retry_soon"

    # ---------- browser ----------
    def start_browser(self):
        b = make_watch_browser(self.cookie_list, self.pg["url"])
        if b.wait_ready(60):
            self.browser = b
            if not self.ready_sent and time.time() < self.cfg["start"]:
                self.ready_sent = True
                rem = self.cfg["start"] - time.time()
                self.announce(T(
                    f"🔧 Browser is open and ready for page {self.pg['id']}. "
                    f"Checking starts at {_hms(self.cfg['start'])} (in {_dur(rem)}).",
                    f"🔧 المتصفح اتفتح وجاهز للصفحة {self.pg['id']}. "
                    f"الفحص هيبدأ الساعة {_hms(self.cfg['start'])} (بعد {_dur(rem)})."))
            return True
        b.kill()
        self.restarts += 1
        self.warn(T("⚠️ Watch: the browser could not start. Trying again.",
                    "⚠️ الواتش: المتصفح معرفش يشتغل. هحاول تاني."))
        return False

    def drop_browser(self):
        b, self.browser = self.browser, None
        if b:
            b.kill()

    def close_browser_save_cookies(self):
        b, self.browser = self.browser, None
        if not b:
            return
        ck = b.stop()
        if ck:
            st = load_state()
            st["cookies"] = normalize_cookies(ck)
            save_state(st)

    # ---------- one step of the main loop ----------
    def tick(self):
        cfg, pg = self.cfg, self.pg
        now = time.time()
        if now >= cfg["end"]:
            self.final = "ended"
            return
        phase, interval = watch_phase(cfg, now)

        # --- paused by /pause: close the browser, keep listening
        if load_state().get("paused"):
            self.close_browser_save_cookies()
            if not self.paused:
                self.paused = True
                self.announce(T(
                    "⏸ Watch paused: no checks are made. Send /resume to continue.",
                    "⏸ الواتش واقف مؤقتًا: مفيش فحص. ابعت /resume عشان يكمّل."))
            return _poll(min(25, cfg["end"] - now))
        if self.paused:
            self.paused = False
            self.next_check = 0.0
            if now >= cfg["start"]:
                self.phase = phase  # the message below already says the interval
                self.announce(T(f"▶️ Watch resumed: checking every {interval}s.",
                                f"▶️ الواتش كمّل: فحص كل {interval} ثانية."))
            else:
                self.announce(T(f"▶️ Watch resumed: checking starts at {_hms(cfg['start'])}.",
                                f"▶️ الواتش كمّل: الفحص هيبدأ الساعة {_hms(cfg['start'])}."))

        # --- waiting for the warm-up moment
        warm_ts = cfg["start"] - cfg["warmup"]
        if now < warm_ts:
            return _poll(min(25, warm_ts - now))
        if self.browser is None:
            if not self.start_browser():
                _poll(min(5, cfg["end"] - time.time()))
            return  # re-evaluate the time after the (slow) start

        # --- phase announcements
        active = now >= cfg["start"]
        if active and phase != self.phase:
            if self.phase is None:
                self.announce(T(
                    f"🟢 Checking started: page {pg['id']} ({pg['label']}), every "
                    f"{interval}s, until {_hms(cfg['end'])}. Send /pause to stop.",
                    f"🟢 الفحص بدأ: الصفحة {pg['id']} ({pg['label']})، كل {interval} "
                    f"ثانية، لحد {_hms(cfg['end'])}. ابعت /pause للإيقاف."))
            elif phase == "fast":
                self.announce(T(
                    f"⚡ FAST mode on (page {pg['id']}): checking every {interval}s "
                    f"until {_hms(cfg['fast_to'])}.",
                    f"⚡ دخلنا وضع الفاست (الصفحة {pg['id']}): فحص كل {interval} ثانية "
                    f"لحد {_hms(cfg['fast_to'])}."))
            else:
                self.announce(T(
                    f"🔄 Back to normal checks (page {pg['id']}): every {interval}s "
                    f"until {_hms(cfg['end'])}.",
                    f"🔄 رجعنا للفحص العادي (الصفحة {pg['id']}): كل {interval} ثانية "
                    f"لحد {_hms(cfg['end'])}."))
            self.phase, self.interval = phase, interval
            self.next_check = min(self.next_check, now)  # check right at the boundary

        # --- check now?
        due = now >= self.next_check and (active or not self.baselined)
        if due:
            t0 = time.time()
            res = self.do_check(active)
            if res in ("found", "blocked"):
                self.final = res
            elif res == "retry":
                self.next_check = 0.0
            elif res == "retry_soon":
                self.next_check = time.time() + 2
            elif active:
                self.next_check = t0 + interval
            return

        # --- sleep until the next check or the next phase boundary
        targets = [cfg["end"]]
        if active or not self.baselined:
            targets.append(self.next_check)
        if not active:
            targets.append(cfg["start"])
        targets += [t for t in (cfg["fast_from"], cfg["fast_to"]) if t > now]
        _poll(min(25, max(0.0, min(targets) - time.time())))

    # ---------- main loop ----------
    def run(self):
        cfg = self.cfg
        print(f"watch: page {self.pg['id']} | start {_hms(cfg['start'])} | fast "
              f"{_hms(cfg['fast_from'])}-{_hms(cfg['fast_to'])} | end {_hms(cfg['end'])} | "
              f"every {cfg['slow_sec']}s / {cfg['fast_sec']}s")
        errors = 0
        try:
            while self.final is None:
                try:
                    self.tick()
                    errors = 0
                except Exception as e:  # nothing may stop the watch before its end
                    errors += 1
                    print("watch: unexpected error:", type(e).__name__, str(e)[:150])
                    if errors in (1, 10):
                        self.warn(T(f"⚠️ Watch: internal error ({type(e).__name__}). "
                                    "Still running.",
                                    f"⚠️ الواتش: حصل خطأ داخلي ({type(e).__name__}). "
                                    "لسه شغّال."))
                    self.drop_browser()
                    time.sleep(min(2 * errors, 10))
        finally:
            self.close_browser_save_cookies()

        if self.final == "found":
            self.announce(T(f"{self.found}\n✅ Watch finished ({self.summary()}).",
                            f"{self.found}\n✅ الواتش خلّص مهمته ({self.summary()})."))
        elif self.final == "ended":
            self.announce(T(
                f"⏹ Watch window ended at {_hms(cfg['end'])} without a "
                f"{'matching ' if cfg['keywords'] else ''}new post ({self.summary()}).",
                f"⏹ وقت الواتش خلص الساعة {_hms(cfg['end'])} من غير ما ينزل بوست "
                f"{'مطابق ' if cfg['keywords'] else ''}جديد ({self.summary()})."))
        print(f"watch: finished ({self.final}); {self.summary()}")


def _watch_dead():
    print("WATCHDOG: the watch run did not end on time - killing the process.")
    try:
        alert_throttled("watch_dead", "⏱ Watch mode did not stop on time and was "
                                      "force-killed.", hours=1)
    except Exception:
        pass
    os._exit(3)


def run_watch(test=False):
    """Entry of watch mode: checks, plan message, then Watch.run()."""
    try:
        cfg = watch_config()
    except ValueError as e:
        print("watch: bad setting -", e)
        alert_throttled("watch_config", f"Watch mode: bad setting - {e}", hours=1)
        return
    pg = PAGES[0]
    if len(PAGES) > 1:
        print(f"watch: only the first page ({pg['id']}) is used in watch mode")
    now = time.time()
    want_date = env("WATCH_DATE")
    if want_date and want_date != now_local().strftime("%Y-%m-%d"):
        print(f"watch: today is not WATCH_DATE ({want_date}) - nothing to do")
        return
    if now >= cfg["end"]:
        print(f"watch: the window already ended at {_hms(cfg['end'])} - nothing to do")
        return
    if cfg["lead"] and cfg["start"] - now > cfg["lead"]:
        print(f"watch: starts in {int((cfg['start'] - now) / 60)} min, more than "
              f"WATCH_LEAD_MIN ({cfg['lead'] // 60}) - exiting to save credit")
        return

    st = load_state()
    if _reset_if_new_cookies(st):
        save_state(st)
    if st.get("blocked"):
        print("watch: blocked -", st.get("block_reason"))
        alert_throttled("watch_blocked", "Watch mode cannot start: the bot is blocked "
                        f"({st.get('block_reason', 'unknown')}). Update FB_COOKIES "
                        "or set RESET=1 once.", hours=1)
        return

    watchdog = threading.Timer(max(60, cfg["end"] - now) + 300, _watch_dead)
    watchdog.daemon = True
    watchdog.start()
    try:
        cookie_list = st.get("cookies") or parse_cookies(get_raw_cookies())
        w = Watch(cfg, pg, cookie_list, test or env("WATCH_TEST") == "1")
        # first thing: tell the owner exactly what will happen and when
        w.announce(watch_plan_text(cfg, pg))
        w.run()
    finally:
        watchdog.cancel()


# ################################################################
# 12) ENTRY POINTS
# ################################################################
def run_cron(test=False):
    """Railway Cron entry: one run, then the process exits (stops billing)."""
    if WATCH_MODE:
        return run_watch(test)
    try:
        handle_commands()
    except Exception as e:
        print("handle_commands failed:", type(e).__name__, str(e)[:100])
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
        try:
            handle_commands()
        except Exception as e:
            print("handle_commands failed:", type(e).__name__, str(e)[:100])
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
        print("WARNING: TG_OWNER_ID is not set - reports and alerts will be "
              "sent to TG_CHAT_ID (the group) instead.")
    print(f"Reports: {'ON' if reports_enabled() else 'OFF (posts only)'} | "
          f"Mode: {'WATCH' if WATCH_MODE else 'normal'}")

    try:
        if a.debug:
            return run_debug()
        test = a.test or env("TEST").lower() in ("1", "all")
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
