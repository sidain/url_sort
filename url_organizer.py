import os
import re
import csv
import json
import time
import html
import zipfile
import configparser
import shutil
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import MaxRetryError, TimeoutError as urllib3_TimeoutError
from bs4 import BeautifulSoup
import ollama

# ==================== CONFIGURATION ====================
SOURCE_DIR = Path("./2bsorted")
LOGS_DIR = Path("./logs")
DESTINATION_DIR = r"F:\@ STORAGE 1\! DOWNLOADS\@ everything\__urls"
BASE_TARGET_DIR = Path(DESTINATION_DIR)
DEAD_LINK_DIR = BASE_TARGET_DIR / "dead_link"
UNKNOWN_DIR = BASE_TARGET_DIR / "unknown"

OLLAMA_MODEL = "llama3.2"       # Pulled Ollama model name
OLLAMA_HOST = "http://localhost:11434"  # Host address for Ollama / local inference
REQUEST_TIMEOUT = 10            # Seconds before timing out

# Fetching and classifying are split into two separate thread pools rather than one shared
# pool. The fetch stage (requests.get) is network I/O-bound and benefits from a much higher
# thread count than the classify stage. The classify stage calls out to a local Ollama
# server, which -- unless you've explicitly raised OLLAMA_NUM_PARALLEL -- typically only
# processes one generate() call at a time internally. Giving classify its own small pool
# means fetch threads stay fully parallel instead of piling up behind Ollama, while classify
# never floods Ollama with more concurrent requests than it can actually service.
FETCH_WORKERS = 16              # Concurrent threads for HTTP requests (network I/O-bound)
CLASSIFY_WORKERS = 2            # Concurrent threads for Ollama calls -- raise this only if
                                 # OLLAMA_NUM_PARALLEL on your Ollama server is raised to match

# Progress/ETA logging cadence: whichever threshold hits first triggers a log line, so long
# stretches of slow files still surface a heartbeat instead of going quiet.
PROGRESS_LOG_EVERY_N = 25       # Log at least every N completed files...
PROGRESS_LOG_EVERY_SECS = 30    # ...or every X seconds, whichever comes first

TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
LOG_FILE = LOGS_DIR / f"log_{TIMESTAMP}.log"
DEAD_LINK_AUDIT_FILE = LOGS_DIR / f"dead_link_audit_{TIMESTAMP}.csv"

# Drop Firefox/Chrome-style bookmarks JSON exports here; each one is converted into
# individual .url shortcuts inside SOURCE_DIR before the normal scan runs, so bookmarks
# flow through the exact same test/categorize/move pipeline as everything else. Processed
# JSON files are moved into the 'imported' subfolder so they aren't re-converted next run.
# All import staging/archive folders live under a single ARCHIVES_DIR to keep the working
# directory root clean.
ARCHIVES_DIR = Path("./archives")
BOOKMARKS_IMPORT_DIR = ARCHIVES_DIR / "bookmarks_to_import"
BOOKMARKS_IMPORTED_DIR = BOOKMARKS_IMPORT_DIR / "imported"
VALID_BOOKMARK_URI_SCHEMES = ("http://", "https://")

# Loose text files -- e.g. "copy link" saves, or exports from tools that don't produce a
# proper .url shortcut -- are converted into regular .url shortcuts the same way bookmark
# JSON entries are. Originals are archived here afterward so they aren't reprocessed and
# don't clutter SOURCE_DIR.
#
# Two shapes are recognized:
#   1. Bare URL: the entire (whitespace-trimmed) content is nothing but a URL.
#   2. Title + URL: exactly one other non-blank line plus a bare URL line, in either
#      order (title-then-url is the common case; url-then-title covers some export tools).
#      The non-URL line becomes the bookmark title instead of falling back to the filename.
#
# Discovery is NOT limited to .txt files -- some save/export tools write these with no
# extension at all, or with a misleading one (a page title containing a dot, e.g. "...
# Koa.js" or "...Netlify.com", ends up read by the filesystem as the file's actual suffix).
# Any file under SOURCE_DIR that isn't already a recognized type is a candidate; only its
# content (matching one of the two shapes above) determines whether it gets converted.
TEXT_URL_IMPORTED_DIR = ARCHIVES_DIR / "text_url_imports_archive"
BARE_URL_TXT_PATTERN = re.compile(r'^\s*(https?://\S+)\s*$')
BARE_URL_LINE_PATTERN = re.compile(r'^(https?://\S+)$')
# Extensions handled by other stages already -- skipped here to avoid double-processing.
LOOSE_TEXT_SKIP_SUFFIXES = {".url", ".json", ".html", ".htm"}

# ==================== PUSHBULLET INTEGRATION ====================
# Feature 1: one-time import of a Pushbullet "Data Export" (pushbullet.com -> Settings ->
# Account -> "Export your data"), which arrives as a .zip containing a single large static
# viewer.html plus a files/ folder of attachments. The large majority of pushes in a typical
# export are link pushes; those get converted into the same .url shortcuts used everywhere
# else in this pipeline, so they get fetched/dead-checked/categorized exactly like a bookmark
# import. Drop the zip in PUSHBULLET_EXPORT_IMPORT_DIR, or just straight into SOURCE_DIR
# alongside everything else (same convention as the bookmarks JSON/HTML import above).
PUSHBULLET_EXPORT_IMPORT_DIR = ARCHIVES_DIR / "pushbullet_exports_to_import"
PUSHBULLET_EXPORT_ARCHIVED_DIR = ARCHIVES_DIR / "pushbullet_exports_archive"

# Feature 2: incremental sync against the live Pushbullet API, so pushes made *after* the
# one-time export above keep flowing into the pipeline on every run without ever re-exporting.
# Get a personal access token from https://www.pushbullet.com/#settings/account and put it in
# a .env file next to this script (see .env.example) as PUSHBULLET_API_KEY=your_token_here.
# Sync is skipped (not an error) if no key is configured, so this script still runs fine for
# people who only want the export import.
ENV_FILE = Path("./.env")

def _load_dotenv(path: Path = ENV_FILE) -> dict:
    """Minimal .env parser (KEY=VALUE per line, '#' comments, optional quotes around the
    value) -- no third-party dependency needed for just this. Malformed lines are skipped
    rather than raising, so a stray typo in .env doesn't take down the whole script."""
    values = {}
    if not path.exists():
        return values
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                values[key] = value
    except OSError as e:
        logging.getLogger("URLSorter").warning(f"Could not read '{path}': {e}")
    return values

DOTENV_VALUES = _load_dotenv()

def _load_pushbullet_api_key() -> str:
    # A real environment variable (e.g. set by CI, Task Scheduler, systemd) always wins over
    # .env, matching how every other .env-based tool resolves precedence.
    env_key = os.environ.get("PUSHBULLET_API_KEY", "").strip()
    if env_key:
        return env_key
    return DOTENV_VALUES.get("PUSHBULLET_API_KEY", "").strip()

PUSHBULLET_API_KEY = _load_pushbullet_api_key()
PUSHBULLET_API_BASE = "https://api.pushbullet.com/v2/pushes"
PUSHBULLET_STATE_FILE = LOGS_DIR / "pushbullet_sync_state.json"
PUSHBULLET_FILES_DIR = BASE_TARGET_DIR / "pushbullet_files"   # downloaded 'file'-type pushes
PUSHBULLET_NOTES_DIR = BASE_TARGET_DIR / "pushbullet_notes"   # 'note'-type pushes (no URL/file)
PUSHBULLET_PAGE_LIMIT = 200                                   # pushes per API page (max 500)

# Explicit status codes that indicate a truly dead, removed, or server-failing resource
DEAD_STATUS_CODES = {404, 410, 500, 502, 503, 504}

# A dead-status response body smaller than this (bytes) is flagged as "suspect" in the
# audit CSV rather than trusted outright -- many bot-mitigation layers (Cloudflare, Akamai,
# etc.) return a real 404/403 with a tiny generic body instead of the site's real content.
# This doesn't change what gets moved, it just gives you a flag to spot-check before you
# permanently delete the dead_link folder.
SUSPECT_BODY_BYTES = 512

# Headers configured to mimic a browser to bypass aggressive anti-bot / Cloudflare hangs
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# Allowed categories for the LLM to choose from
ALLOWED_CATEGORIES = [
    "Technology",
    "Programming",
    "Finance",
    "News",     
    "Entertainment",
    "Education",
    "Shopping",
    "Science",
    "Fitness",
    "Health",
    "Gaming",
    "Tools & Utilities",
    "Magic"
]

# Few-shot examples for the classification prompt. Small local models (llama3.2:3b, etc.)
# collapse to a single default category under zero-shot + low-signal input (e.g. bare
# search-result pages, redirects, SPA shells with no real body text) -- observed in
# practice as ~90% of a batch getting labeled "Finance" regardless of content, and after
# a first round of few-shotting, ~90% collapsing onto "Entertainment" instead. Two things
# fixed this in combination: (1) one example per category, so every label has a concrete
# anchor instead of leaving weak categories to be picked only by elimination, and (2) a
# short "reasoning" field the model must fill in before "category" -- a lightweight
# chain-of-thought that reliably improves small-model classification accuracy far more
# than asking for the label directly, even though we discard the reasoning text itself.
FEW_SHOT_EXAMPLES = [
    {"title": "Best Roth IRA Accounts for 2026", "url": "investopedia.com/best-roth-ira-accounts", "snippet": "Compare top Roth IRA providers by fees, minimums, and investment options.", "reasoning": "About investment accounts and retirement savings.", "category": "Finance"},
    {"title": "React Router Docs - useNavigate", "url": "reactrouter.com/en/main/hooks/use-navigate", "snippet": "API reference for the useNavigate hook.", "reasoning": "Developer documentation for a JavaScript library.", "category": "Programming"},
    {"title": "Wowhead - The War Within PTR Patch Notes", "url": "wowhead.com/news/ptr-patch-notes", "snippet": "Datamined changes and class tuning for the current PTR build.", "reasoning": "Patch notes for an MMO video game.", "category": "Gaming"},
    {"title": "Amazon.com: Logitech Wireless Mouse", "url": "amazon.com/dp/B0025", "snippet": "Logitech M510 Wireless Mouse, 4.5 stars, 12,000 ratings.", "reasoning": "A product listing on a retail site.", "category": "Shopping"},
    {"title": "The Office - Season 3 Bloopers", "url": "youtube.com/watch?v=abc123", "snippet": "Blooper reel from the hit NBC sitcom.", "reasoning": "A video clip from a TV comedy show.", "category": "Entertainment"},
    {"title": "MDN Web Docs: Array.prototype.map()", "url": "developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/map", "snippet": "The map() method creates a new array populated with the results of calling a function.", "reasoning": "Developer documentation for a JavaScript built-in method.", "category": "Programming"},
    {"title": "CDC - Seasonal Flu Vaccine Guidance", "url": "cdc.gov/flu/vaccines", "snippet": "Recommendations for the current flu season.", "reasoning": "Public health guidance about a vaccine.", "category": "Health"},
    {"title": "Reuters - Fed Holds Interest Rates Steady", "url": "reuters.com/markets/fed-holds-rates", "snippet": "The Federal Reserve left its benchmark rate unchanged at today's meeting.", "reasoning": "A current-events report from a news wire service.", "category": "News"},
    {"title": "Khan Academy - Intro to Photosynthesis", "url": "khanacademy.org/science/biology/photosynthesis", "snippet": "Free video lesson covering the light and dark reactions.", "reasoning": "An instructional lesson meant for learning a subject.", "category": "Education"},
    {"title": "NASA - James Webb Telescope Latest Images", "url": "nasa.gov/webb/latest-images", "snippet": "New deep-field images released from the observatory.", "reasoning": "Astronomical research findings from a space agency.", "category": "Science"},
    {"title": "Notion - All-in-one workspace", "url": "notion.so", "snippet": "Notes, docs, and project tracking in one app.", "reasoning": "A general-purpose productivity/utility software product.", "category": "Tools & Utilities"},
]
# =======================================================

stats_lock = threading.Lock()
audit_lock = threading.Lock()

def format_duration(seconds: float) -> str:
    """Formats a seconds count as e.g. '1h 12m 03s' / '4m 30s' / '17s' for log output."""
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"

class ProgressTracker:
    """Thread-safe completion tracker that logs a heartbeat with rate + ETA.

    Long runs (thousands of files, each involving a network round trip and an LLM call)
    can go quiet for uncomfortably long stretches between the per-file 'Processing (n/N)'
    lines if a given file happens to be slow. This logs a summary line at least every
    PROGRESS_LOG_EVERY_N completions or every PROGRESS_LOG_EVERY_SECS seconds, whichever
    comes first, so a stalled-looking run and a genuinely hung one are easy to tell apart.
    """
    def __init__(self, total: int, log_every_n: int = PROGRESS_LOG_EVERY_N, log_every_secs: float = PROGRESS_LOG_EVERY_SECS):
        self.total = total
        self.completed = 0
        self.start = time.monotonic()
        self.last_log_time = self.start
        self.log_every_n = log_every_n
        self.log_every_secs = log_every_secs
        self.lock = threading.Lock()

    def tick(self):
        """Marks one file as fully finished (moved or errored-out). Call exactly once per file."""
        with self.lock:
            self.completed += 1
            now = time.monotonic()
            due_by_count = self.completed % self.log_every_n == 0
            due_by_time = (now - self.last_log_time) >= self.log_every_secs
            is_last = self.completed == self.total
            if not (due_by_count or due_by_time or is_last):
                return
            self.last_log_time = now

        elapsed = now - self.start
        rate = self.completed / elapsed if elapsed > 0 else 0
        remaining = self.total - self.completed
        eta = remaining / rate if rate > 0 else 0
        pct = (self.completed / self.total * 100) if self.total else 100
        logger.info(
            f"Progress: {self.completed}/{self.total} ({pct:.1f}%) | "
            f"{rate:.2f} files/sec | Elapsed: {format_duration(elapsed)} | ETA: {format_duration(eta)}"
        )

def setup_logging():
    """Configures dual-logging to stdout and log file with debug level support."""
    logger = logging.getLogger("URLSorter")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s')
    console_formatter = logging.Formatter('[%(levelname)s] %(message)s')

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(file_formatter)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

def setup_dead_link_audit():
    """Initializes the dead-link audit CSV with a header row (overwritten each run)."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(DEAD_LINK_AUDIT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "file_name", "url", "http_status", "body_bytes", "suspect"])
    return DEAD_LINK_AUDIT_FILE

def append_dead_link_audit(file_name: str, url: str, status_code, body_bytes: int, suspect: bool):
    """Appends a single dead-link record to the audit CSV. Thread-safe."""
    with audit_lock:
        with open(DEAD_LINK_AUDIT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(timespec='seconds'),
                file_name,
                url,
                status_code if status_code is not None else "N/A",
                body_bytes,
                "YES" if suspect else ""
            ])

def create_resilient_session() -> requests.Session:
    """Creates a requests session configured with retries and connection pooling."""
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=FETCH_WORKERS * 2, pool_maxsize=FETCH_WORKERS * 2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

http_session = create_resilient_session()

def sanitize_folder_name(name: str) -> str:
    """Sanitizes directory names by removing illegal path characters."""
    sanitized = re.sub(r'[\\/*?:"<>|]', '', name)
    sanitized = sanitized.strip().replace(" ", "_")
    return sanitized if sanitized else "unknown"

# Windows MAX_PATH is 260 chars. Old .url bookmark filenames are sometimes 200+ chars on
# their own (article titles saved verbatim), and once a target_folder path is prepended,
# shutil.move can blow past the limit and fail with a cryptic [WinError 3] "path not
# found" -- which looks nothing like a length problem in the logs. Cap the filename we
# actually write to disk so the full destination path stays under a safe threshold.
MAX_SAFE_PATH_LEN = 240

def safe_dest_filename(target_folder: Path, original_name: str) -> str:
    """Shortens a filename if needed so target_folder/filename stays under MAX_SAFE_PATH_LEN.

    Measured against the *resolved absolute* path, not the relative one -- target_folder
    is typically a short relative path like './categories/unknown', which stays well under
    the threshold on its own even when the real destination (once the working directory is
    prepended) blows past Windows' 260-char MAX_PATH. Using the relative length let files
    slip through un-truncated and fail at move time with a cryptic [WinError 3] "path not
    found" instead of the length problem it actually was.
    """
    full_len = len(str((target_folder / original_name).resolve()))
    if full_len <= MAX_SAFE_PATH_LEN:
        return original_name

    overage = full_len - MAX_SAFE_PATH_LEN
    stem, ext = os.path.splitext(original_name)
    new_stem_len = max(10, len(stem) - overage)
    truncated = stem[:new_stem_len].rstrip() + ext
    logger.warning(f"Filename too long for safe path length, truncated: '{original_name}' -> '{truncated}'")
    return truncated

def safe_move(src: Path, target_folder: Path, original_name: str) -> tuple[bool, str]:
    """
    Moves src into target_folder, truncating the filename if the destination path would be
    too long. Returns (success, error_message). Never raises -- callers must check the
    returned success flag before updating stats, so a failed move can never be silently
    counted as a success.
    """
    dest_name = safe_dest_filename(target_folder, original_name)
    dest_path = target_folder / dest_name

    # If a truncated name collides with an existing file, disambiguate rather than overwrite.
    if dest_path.exists():
        stem, ext = os.path.splitext(dest_name)
        for i in range(1, 1000):
            candidate = target_folder / f"{stem}_{i}{ext}"
            if not candidate.exists():
                dest_path = candidate
                break

    try:
        shutil.move(str(src), str(dest_path))
        return True, ""
    except OSError as e:
        logger.error(f"Failed to move '{original_name}' to '{dest_path}': {e}")
        return False, str(e)

def extract_url_from_file(file_path: Path) -> str | None:
    """Extracts target URL safely from Windows .url or shortcut files across various encodings."""
    logger.debug(f"Parsing URL file: {file_path.name}")
    
    raw_content = ""
    for enc in ['utf-8', 'utf-16', 'cp1252', 'latin-1']:
        try:
            raw_content = file_path.read_text(encoding=enc)
            break
        except (UnicodeDecodeError, Exception):
            continue

    if raw_content:
        try:
            # interpolation=None: without this, configparser tries to interpret '%' as
            # its own interpolation syntax, which blows up on any URL containing a raw
            # '%' (e.g. UTM-tagged marketing links like "...AS_123%20-%20Copy..."). That
            # was previously falling back to regex every time it happened -- functionally
            # fine, but needless failures/log noise on very common URLs.
            config = configparser.ConfigParser(interpolation=None)
            config.read_string(raw_content)
            if 'InternetShortcut' in config and 'URL' in config['InternetShortcut']:
                return config['InternetShortcut']['URL'].strip()
        except Exception as e:
            logger.debug(f"INI string parsing failed for {file_path.name}: {e}. Falling back to regex.")

        match = re.search(r'URL=(https?://[^\s\r\n]+)', raw_content, re.IGNORECASE)
        if match:
            return match.group(1).strip()
            
        generic_match = re.search(r'https?://[^\s\r\n]+', raw_content)
        if generic_match:
            return generic_match.group(0).strip()

    preview = raw_content[:200].replace("\r", "\\r").replace("\n", "\\n") if raw_content else "<no content decoded>"
    logger.error(f"Failed to extract valid URL from {file_path.name}. Raw content preview: {preview!r}")
    return None

def repair_and_load_bookmarks_json(json_path: Path):
    """
    Loads a Firefox/Chrome-style bookmarks JSON export. Some old Firefox
    'text/x-moz-place' backups (this one included) ship with a stray trailing comma
    before a closing ] or }, which is invalid JSON. Falls back to stripping those before
    re-parsing rather than failing outright.
    """
    raw = json_path.read_text(encoding='utf-8')
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f"'{json_path.name}' is malformed JSON ({e}); attempting trailing-comma repair.")
        repaired = re.sub(r',(\s*[\]}])', r'\1', raw)
        return json.loads(repaired)

def iter_bookmark_entries(node, seen_uris: set):
    """Recursively yields (title, uri) for every bookmark leaf under node, skipping
    folders, separators, and non-http(s) entries (place: queries, javascript: bookmarklets,
    etc.), and de-duplicating identical URLs within the same import."""
    if isinstance(node, dict):
        if node.get("type") == "text/x-moz-place":
            uri = node.get("uri", "")
            if uri.startswith(VALID_BOOKMARK_URI_SCHEMES) and uri not in seen_uris:
                seen_uris.add(uri)
                yield node.get("title", ""), uri
        for child in node.get("children", []) or []:
            yield from iter_bookmark_entries(child, seen_uris)
    elif isinstance(node, list):
        for item in node:
            yield from iter_bookmark_entries(item, seen_uris)

def parse_bookmarks_html(html_path: Path) -> list:
    """
    Parses a standard Netscape Bookmark File (the .html export format used by Chrome,
    Firefox, Edge, etc.). Returns a de-duplicated list of (title, uri) tuples for every
    http(s) link in the file, regardless of which folder it's nested under -- folder
    structure isn't preserved since categorization is handled by the LLM step downstream
    anyway, same as the JSON import.
    """
    content = html_path.read_text(encoding='utf-8', errors='ignore')
    soup = BeautifulSoup(content, 'html.parser')

    seen_uris = set()
    entries = []
    for a in soup.find_all('a'):
        href = a.get('href', '')
        if href.startswith(VALID_BOOKMARK_URI_SCHEMES) and href not in seen_uris:
            seen_uris.add(href)
            entries.append((a.get_text(strip=True), href))
    return entries

def import_bookmarks_html(html_path: Path, dest_dir: Path = None) -> int:
    """Converts one Netscape Bookmark File (.html) export into individual .url files in
    dest_dir (defaults to SOURCE_DIR), mirroring import_bookmarks_json. Returns the number
    of bookmark entries imported."""
    if dest_dir is None:
        dest_dir = SOURCE_DIR

    try:
        entries = parse_bookmarks_html(html_path)
    except Exception as e:
        logger.error(f"Could not parse bookmarks file '{html_path.name}': {e}")
        return 0

    if not entries:
        logger.warning(f"No bookmark URLs found in '{html_path.name}'.")
        return 0

    dest_dir.mkdir(parents=True, exist_ok=True)
    used_names = {p.name.lower() for p in SOURCE_DIR.rglob("*.url")}

    count = 0
    for title, uri in entries:
        try:
            write_bookmark_shortcut(dest_dir, title, uri, used_names)
            count += 1
        except OSError as e:
            logger.error(f"Failed writing shortcut for '{title or uri}': {e}")

    logger.info(f"Imported {count} bookmark(s) from '{html_path.name}' into '{dest_dir}'.")
    return count

def sanitize_bookmark_title(title: str, uri: str) -> str:
    """Builds a filesystem-safe filename base from a bookmark title, falling back to the
    URL's domain when the title is blank (common for bookmarklets/search-result saves)."""
    base = title.strip() if title else ""
    if not base:
        base = urlparse(uri).netloc or "untitled"
    base = re.sub(r'[\\/*?:"<>|]', '', base).strip()
    base = re.sub(r'\s+', ' ', base)
    # Leave headroom for the .url extension and later category-folder prefixing;
    # safe_dest_filename() handles final truncation at move time regardless.
    return base[:150] if base else "untitled"

def write_bookmark_shortcut(dest_dir: Path, title: str, uri: str, used_names: set) -> None:
    """Writes a Windows .url shortcut file for one bookmark, disambiguating on collision
    (Firefox exports frequently contain repeated titles for different URLs)."""
    base_name = sanitize_bookmark_title(title, uri)
    candidate = f"{base_name}.url"
    n = 1
    while candidate.lower() in used_names or (dest_dir / candidate).exists():
        candidate = f"{base_name} ({n}).url"
        n += 1
    used_names.add(candidate.lower())
    (dest_dir / candidate).write_text(f"[InternetShortcut]\nURL={uri}\n", encoding='utf-8')

def import_bookmarks_json(json_path: Path, dest_dir: Path = None) -> int:
    """Converts one bookmarks JSON export into individual .url files in dest_dir (defaults
    to SOURCE_DIR) so they flow through the normal fetch/categorize/move pipeline. Returns
    the number of bookmark entries imported."""
    if dest_dir is None:
        dest_dir = SOURCE_DIR

    try:
        data = repair_and_load_bookmarks_json(json_path)
    except Exception as e:
        logger.error(f"Could not parse bookmarks file '{json_path.name}': {e}")
        return 0

    seen_uris = set()
    entries = list(iter_bookmark_entries(data, seen_uris))
    if not entries:
        logger.warning(f"No bookmark URLs found in '{json_path.name}'.")
        return 0

    dest_dir.mkdir(parents=True, exist_ok=True)
    used_names = {p.name.lower() for p in SOURCE_DIR.rglob("*.url")}

    count = 0
    for title, uri in entries:
        try:
            write_bookmark_shortcut(dest_dir, title, uri, used_names)
            count += 1
        except OSError as e:
            logger.error(f"Failed writing shortcut for '{title or uri}': {e}")

    logger.info(f"Imported {count} bookmark(s) from '{json_path.name}' into '{dest_dir}'.")
    return count

def import_all_pending_bookmark_files() -> int:
    """
    Scans for bookmark exports -- both JSON (Firefox 'places' backups) and Netscape
    Bookmark File .html/.htm (Chrome/Firefox/Edge exports) -- in two places:
      1. BOOKMARKS_IMPORT_DIR, a dedicated staging folder (if you prefer to keep them separate)
      2. SOURCE_DIR itself, recursively -- since in practice it's natural to just drop a
         bookmarks export straight into 2bsorted alongside everything else being sorted.

    Each file found is converted into .url files (placed in the same subfolder it was found
    in, so category sorting still applies per-subfolder as normal), then the source file is
    archived into BOOKMARKS_IMPORTED_DIR (mirroring its relative subpath) so it's never
    re-imported and doesn't clutter 2bsorted going forward.

    Always logs a summary, including the zero case, so a "nothing imported" run is visible
    in the log instead of silent.
    """
    BOOKMARK_EXTENSIONS = ("*.json", "*.html", "*.htm")
    bookmark_files = []

    if BOOKMARKS_IMPORT_DIR.exists():
        for pattern in BOOKMARK_EXTENSIONS:
            bookmark_files += [(bf, None) for bf in BOOKMARKS_IMPORT_DIR.glob(pattern)]

    if SOURCE_DIR.exists():
        for pattern in BOOKMARK_EXTENSIONS:
            bookmark_files += [(bf, bf.relative_to(SOURCE_DIR).parent) for bf in SOURCE_DIR.rglob(pattern)]

    if not bookmark_files:
        logger.info(f"Bookmark import: no .json/.html bookmark export files found in "
                    f"'{BOOKMARKS_IMPORT_DIR}' or '{SOURCE_DIR}'.")
        return 0

    logger.info(f"Bookmark import: found {len(bookmark_files)} bookmark export file(s) to process: "
                f"{', '.join(bf.name for bf, _ in bookmark_files)}")

    total = 0
    for bf, relative_subpath in bookmark_files:
        dest_dir = SOURCE_DIR / relative_subpath if relative_subpath else SOURCE_DIR

        if bf.suffix.lower() == ".json":
            imported_count = import_bookmarks_json(bf, dest_dir)
        else:  # .html / .htm
            imported_count = import_bookmarks_html(bf, dest_dir)
        total += imported_count

        archive_dir = BOOKMARKS_IMPORTED_DIR / relative_subpath if relative_subpath else BOOKMARKS_IMPORTED_DIR
        try:
            archive_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(bf), str(archive_dir / bf.name))
        except OSError as e:
            logger.error(f"Could not archive processed bookmarks file '{bf.name}': {e}")

    logger.info(f"Bookmark import complete: {total} bookmark(s) converted to .url files across {len(bookmark_files)} export file(s).")
    return total

# -------------------- Pushbullet export import (Feature 1) --------------------

# The exporter writes exactly one push per line -- <div class="push-row"> ... single
# line ... </div> -- so this is parsed line-by-line with regex rather than as a DOM tree.
# A real export runs 10MB+/25k+ pushes; building a full BeautifulSoup tree over that is
# drastically slower than the format's own regularity requires, and none of the markup is
# untrusted enough to need real HTML parsing here.
PB_ROW_TITLE_RE = re.compile(r'<p class="push-title">(.*?)</p>')
PB_ROW_URL_RE = re.compile(r'<a class="push-url" href="([^"]*)"')

def parse_pushbullet_viewer_html(html_path: Path):
    """Streams a Pushbullet 'Data Export' viewer.html and yields (title, url) for every push
    that has a URL, de-duplicating URLs within this file. Pushes with no URL (plain notes, or
    file pushes -- those live in the export's files/ folder instead) are skipped here."""
    seen_urls = set()
    with html_path.open('r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if 'class="push-row"' not in line or 'class="push-url"' not in line:
                continue
            url_match = PB_ROW_URL_RE.search(line)
            if not url_match:
                continue
            url = html.unescape(url_match.group(1)).strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            title_match = PB_ROW_TITLE_RE.search(line)
            title = html.unescape(title_match.group(1)).strip() if title_match else ""
            yield title, url

def import_pushbullet_export_zip(zip_path: Path, dest_dir: Path = None) -> int:
    """Pulls viewer.html out of one Pushbullet export zip (without extracting the
    often much larger files/ folder of attachments), converts every link push it contains
    into a .url shortcut in dest_dir (defaults to SOURCE_DIR), then archives the zip so it's
    never reprocessed. Zips that don't actually contain a viewer.html are left in place and
    not archived, since SOURCE_DIR may hold unrelated .zip files that have nothing to do with
    this pipeline."""
    if dest_dir is None:
        dest_dir = SOURCE_DIR

    try:
        with zipfile.ZipFile(zip_path) as zf:
            matches = [n for n in zf.namelist()
                       if n.lower() == "viewer.html" or n.lower().endswith("/viewer.html")]
            if not matches:
                logger.debug(f"'{zip_path.name}' has no viewer.html -- not a Pushbullet export, leaving in place.")
                return 0
            PUSHBULLET_EXPORT_IMPORT_DIR.mkdir(parents=True, exist_ok=True)
            tmp_html = PUSHBULLET_EXPORT_IMPORT_DIR / f"_tmp_{zip_path.stem}_viewer.html"
            with zf.open(matches[0]) as src, open(tmp_html, 'wb') as dst:
                shutil.copyfileobj(src, dst)
    except (zipfile.BadZipFile, OSError) as e:
        logger.error(f"Could not read '{zip_path.name}' as a zip: {e}")
        return 0

    dest_dir.mkdir(parents=True, exist_ok=True)
    used_names = {p.name.lower() for p in SOURCE_DIR.rglob("*.url")} if SOURCE_DIR.exists() else set()

    count = 0
    try:
        for title, url in parse_pushbullet_viewer_html(tmp_html):
            try:
                write_bookmark_shortcut(dest_dir, title, url, used_names)
                count += 1
            except OSError as e:
                logger.error(f"Failed writing shortcut for Pushbullet push '{title or url}': {e}")
    finally:
        tmp_html.unlink(missing_ok=True)

    logger.info(f"Pushbullet export: imported {count} URL push(es) from '{zip_path.name}'.")

    PUSHBULLET_EXPORT_ARCHIVED_DIR.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(zip_path), str(PUSHBULLET_EXPORT_ARCHIVED_DIR / zip_path.name))
    except OSError as e:
        logger.error(f"Could not archive processed Pushbullet export '{zip_path.name}': {e}")

    return count

def import_all_pending_pushbullet_exports() -> int:
    """Scans PUSHBULLET_EXPORT_IMPORT_DIR and SOURCE_DIR (recursively) for Pushbullet export
    zips and imports every one found. Mirrors import_all_pending_bookmark_files()."""
    candidates = []
    if PUSHBULLET_EXPORT_IMPORT_DIR.exists():
        candidates += list(PUSHBULLET_EXPORT_IMPORT_DIR.glob("*.zip"))
    if SOURCE_DIR.exists():
        candidates += list(SOURCE_DIR.rglob("*.zip"))

    if not candidates:
        logger.info(f"Pushbullet export import: no .zip files found in "
                    f"'{PUSHBULLET_EXPORT_IMPORT_DIR}' or '{SOURCE_DIR}'.")
        return 0

    logger.info(f"Pushbullet export import: found {len(candidates)} .zip file(s) to check.")
    total = sum(import_pushbullet_export_zip(zp) for zp in candidates)
    return total

# -------------------- Pushbullet live API sync (Feature 2) --------------------

def load_pushbullet_state() -> dict:
    """Loads the persisted sync cursor (highest 'modified' timestamp seen so far)."""
    if PUSHBULLET_STATE_FILE.exists():
        try:
            return json.loads(PUSHBULLET_STATE_FILE.read_text(encoding='utf-8'))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not read Pushbullet sync state ({e}); starting a fresh sync.")
    return {"last_modified": 0}

def save_pushbullet_state(state: dict) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        PUSHBULLET_STATE_FILE.write_text(json.dumps(state), encoding='utf-8')
    except OSError as e:
        logger.error(f"Could not save Pushbullet sync state: {e}")

def fetch_pushbullet_pushes_page(since_modified: float, cursor: str = None) -> dict:
    """Fetches one page of pushes modified after since_modified via the live API."""
    params = {"modified_after": since_modified, "active": "true", "limit": PUSHBULLET_PAGE_LIMIT}
    if cursor:
        params["cursor"] = cursor
    response = http_session.get(
        PUSHBULLET_API_BASE,
        headers={"Access-Token": PUSHBULLET_API_KEY},
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()

def download_pushbullet_file(push: dict) -> bool:
    """Downloads one 'file'-type push's attachment into PUSHBULLET_FILES_DIR."""
    file_url = push.get("file_url")
    file_name = push.get("file_name") or "unnamed_file"
    if not file_url:
        return False

    PUSHBULLET_FILES_DIR.mkdir(parents=True, exist_ok=True)
    dest_name = safe_dest_filename(PUSHBULLET_FILES_DIR, file_name)
    dest_path = PUSHBULLET_FILES_DIR / dest_name
    stem, ext = os.path.splitext(dest_name)
    n = 1
    while dest_path.exists():
        dest_path = PUSHBULLET_FILES_DIR / f"{stem} ({n}){ext}"
        n += 1

    try:
        with http_session.get(file_url, stream=True, timeout=REQUEST_TIMEOUT * 3) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as out:
                for chunk in r.iter_content(chunk_size=65536):
                    out.write(chunk)
        return True
    except (requests.RequestException, OSError) as e:
        logger.error(f"Failed downloading Pushbullet file '{file_name}': {e}")
        return False

def save_pushbullet_note(push: dict) -> bool:
    """Saves one 'note'-type push (no URL, no file) as a .txt file in PUSHBULLET_NOTES_DIR
    so it isn't silently dropped by a sync that otherwise only cares about URLs/files."""
    title = (push.get("title") or "").strip()
    body = (push.get("body") or "").strip()
    if not title and not body:
        return False

    PUSHBULLET_NOTES_DIR.mkdir(parents=True, exist_ok=True)
    base = sanitize_bookmark_title(title or body[:60], "note")
    dest_path = PUSHBULLET_NOTES_DIR / f"{base}.txt"
    n = 1
    while dest_path.exists():
        dest_path = PUSHBULLET_NOTES_DIR / f"{base} ({n}).txt"
        n += 1

    try:
        dest_path.write_text(f"{title}\n\n{body}\n" if title else body, encoding='utf-8')
        return True
    except OSError as e:
        logger.error(f"Failed saving Pushbullet note '{title}': {e}")
        return False

def sync_pushbullet_pushes(dest_dir: Path = None) -> dict:
    """Pulls every push created/modified since the last successful sync straight from the
    live Pushbullet API -- no export/re-export required -- and routes each by type:
      - link pushes (anything with a 'url')  -> .url shortcuts in dest_dir, so they flow
        through the exact same fetch/dead-check/categorize pipeline as everything else.
      - 'file' pushes                        -> downloaded into PUSHBULLET_FILES_DIR.
      - 'note' pushes (no URL, no file)       -> saved as .txt into PUSHBULLET_NOTES_DIR.

    Progress is tracked by the highest 'modified' timestamp seen (Pushbullet guarantees this
    is monotonically increasing and unique per push) and persisted to PUSHBULLET_STATE_FILE,
    so each run -- including one kicked off on a schedule -- only pulls what's new since the
    last run and never reprocesses the same push twice.
    """
    if dest_dir is None:
        dest_dir = SOURCE_DIR

    stats = {"links": 0, "files": 0, "notes": 0, "skipped": 0, "errors": 0}

    if not PUSHBULLET_API_KEY:
        logger.info("Pushbullet sync: no API key configured, skipping live sync (set the "
                    "PUSHBULLET_API_KEY env var, or put a token in ./pushbullet_api_key.txt -- "
                    "get one from https://www.pushbullet.com/#settings/account).")
        return stats

    state = load_pushbullet_state()
    since_modified = state.get("last_modified", 0)
    highest_modified = since_modified

    dest_dir.mkdir(parents=True, exist_ok=True)
    used_names = {p.name.lower() for p in SOURCE_DIR.rglob("*.url")} if SOURCE_DIR.exists() else set()

    cursor = None
    page_num = 0
    while True:
        page_num += 1
        try:
            data = fetch_pushbullet_pushes_page(since_modified, cursor)
        except requests.RequestException as e:
            logger.error(f"Pushbullet sync: API request failed on page {page_num}: {e}")
            stats["errors"] += 1
            break

        pushes = data.get("pushes", [])
        logger.debug(f"Pushbullet sync: page {page_num} returned {len(pushes)} push(es).")

        for push in pushes:
            modified = push.get("modified", 0)
            highest_modified = max(highest_modified, modified)

            if push.get("dismissed") or not push.get("active", True):
                stats["skipped"] += 1
                continue

            url = push.get("url")
            push_type = push.get("type")

            if url:
                title = push.get("title") or push.get("body") or ""
                try:
                    write_bookmark_shortcut(dest_dir, title, url, used_names)
                    stats["links"] += 1
                except OSError as e:
                    logger.error(f"Pushbullet sync: failed writing shortcut for '{title or url}': {e}")
                    stats["errors"] += 1
            elif push_type == "file":
                stats["files" if download_pushbullet_file(push) else "errors"] += 1
            elif push_type == "note":
                stats["notes" if save_pushbullet_note(push) else "skipped"] += 1
            else:
                stats["skipped"] += 1

        cursor = data.get("cursor")
        if not cursor:
            break

    if highest_modified > since_modified:
        save_pushbullet_state({"last_modified": highest_modified})

    logger.info(f"Pushbullet sync: {stats['links']} link(s) queued, {stats['files']} file(s) "
                f"downloaded, {stats['notes']} note(s) saved, {stats['skipped']} skipped, "
                f"{stats['errors']} error(s). Cursor advanced to modified={highest_modified}.")
    return stats

def extract_loose_url_and_title(content: str, fallback_title: str) -> tuple[str, str] | None:
    """
    Checks whether `content` matches one of the two loose-text shapes described above
    BARE_URL_TXT_PATTERN, and if so returns (title, url). Returns None otherwise.

    - Single non-blank line, entirely a URL -> (fallback_title, url)
    - Exactly two non-blank lines, one a bare URL and the other not -> (that other line, url)
    Anything else (no URL line, more than two non-blank lines, a URL plus real commentary,
    multiple URLs, etc.) is left alone -- deliberately conservative, since SOURCE_DIR may
    contain ordinary files that have nothing to do with this pipeline.
    """
    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if not lines:
        return None

    if len(lines) == 1:
        m = BARE_URL_LINE_PATTERN.match(lines[0])
        return (fallback_title, m.group(1)) if m else None

    if len(lines) == 2:
        for title_line, url_line in ((lines[0], lines[1]), (lines[1], lines[0])):
            url_match = BARE_URL_LINE_PATTERN.match(url_line)
            if url_match and not BARE_URL_LINE_PATTERN.match(title_line):
                return title_line, url_match.group(1)

    return None

def import_url_text_files() -> int:
    """
    Scans SOURCE_DIR recursively for loose (non-.url, non-bookmark-export) files whose
    content is either a bare URL or a title line plus a bare URL line -- e.g. a link pasted
    into Notepad and saved, or a page saved as "Title\\nURL" by a browser extension. Each
    match is converted into a standard .url shortcut, then the source file is archived into
    TEXT_URL_IMPORTED_DIR (mirroring its relative subpath) so it isn't reprocessed.

    Not limited to .txt: files saved with no extension, or with a misleading one (a page
    title containing a dot, e.g. "...Koa.js" or "...Netlify.com", becomes the file's actual
    suffix), are just as valid candidates -- content shape is what's checked, not the name.
    """
    if not SOURCE_DIR.exists():
        return 0

    candidate_files = [
        p for p in SOURCE_DIR.rglob("*")
        if p.is_file() and p.suffix.lower() not in LOOSE_TEXT_SKIP_SUFFIXES
    ]
    if not candidate_files:
        logger.info("URL-text import: no candidate loose files found in source directory.")
        return 0

    used_names = {p.name.lower() for p in SOURCE_DIR.rglob("*.url")}
    converted = 0
    skipped = 0

    for tf in candidate_files:
        try:
            content = tf.read_text(encoding='utf-8', errors='ignore')
        except OSError as e:
            logger.debug(f"Could not read '{tf.name}' as text: {e}")
            skipped += 1
            continue

        result = extract_loose_url_and_title(content, fallback_title=tf.stem)
        if result is None:
            skipped += 1
            continue  # not a bare-URL / title+URL text file; leave it alone

        title, uri = result
        relative_subpath = tf.relative_to(SOURCE_DIR).parent
        dest_dir = SOURCE_DIR / relative_subpath

        try:
            write_bookmark_shortcut(dest_dir, title, uri, used_names)
        except OSError as e:
            logger.error(f"Failed writing shortcut for '{tf.name}': {e}")
            continue

        archive_dir = TEXT_URL_IMPORTED_DIR / relative_subpath
        try:
            archive_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(tf), str(archive_dir / tf.name))
        except OSError as e:
            logger.error(f"Could not archive processed text file '{tf.name}': {e}")
            continue

        converted += 1

    logger.info(f"URL-text import: converted {converted} loose text file(s) into .url shortcuts "
                f"({skipped} file(s) left untouched, no bare URL / title+URL shape detected).")
    return converted

def fetch_webpage_info(url: str, file_name: str) -> tuple[str, str, str]:
    """
    Tests URL status and attempts to retrieve content.
    Returns a tuple of (status_result, title, snippet).
    status_result can be: 'ok', 'protected', or 'dead'

    For 'dead' results, also writes a row to the dead-link audit CSV so verdicts can be
    spot-checked before the moved files are permanently deleted. A dead verdict with an
    unusually small response body is flagged 'suspect', since some bot-mitigation layers
    return a real-looking 404/403 with a tiny generic body instead of a genuine not-found
    page.
    """
    logger.debug(f"Testing URL activity: {url}")
    
    fallback_title = file_name.replace(".url", "").replace("_", " ").replace("-", " ")
    domain_context = f"Domain: {urlparse(url).netloc}"

    try:
        response = http_session.get(
            url, 
            headers=DEFAULT_HEADERS, 
            timeout=REQUEST_TIMEOUT, 
            stream=True, 
            allow_redirects=True
        )
        
        # Explicit status codes for confirmed missing endpoints or true server errors
        if response.status_code in DEAD_STATUS_CODES:
            try:
                body_preview = next(response.iter_content(chunk_size=SUSPECT_BODY_BYTES + 1), b"")
            except Exception:
                body_preview = b""
            body_len = len(body_preview)
            suspect = body_len < SUSPECT_BODY_BYTES
            logger.warning(f"URL returned dead HTTP status {response.status_code}: {url}")
            append_dead_link_audit(file_name, url, response.status_code, body_len, suspect)
            return "dead", "", ""

        # Restricted / anti-bot status codes (401, 403, 405, 429)
        if response.status_code in [401, 403, 405, 429]:
            logger.warning(f"URL returned restricted status {response.status_code}: {url}")
            return "protected", fallback_title, domain_context

        content = b""
        for chunk in response.iter_content(chunk_size=1024):
            content += chunk
            if len(content) > 500_000:
                break

        soup = BeautifulSoup(content, 'html.parser')
        title = soup.title.string.strip() if soup.title and soup.title.string else fallback_title
        
        for script_or_style in soup(['script', 'style', 'nav', 'header', 'footer']):
            script_or_style.decompose()
            
        text_content = soup.get_text(separator=' ', strip=True)
        snippet = text_content[:1500] if text_content else domain_context

        logger.debug(f"Successfully fetched content for {url} | Title: {title[:50]}...")
        return "ok", title, snippet

    # Catch standard timeouts AND retry-wrapped connection drops
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, MaxRetryError, urllib3_TimeoutError) as e:
        logger.warning(f"Network timeout or connection drop for {url} ({type(e).__name__}). Using filename fallback.")
        return "protected", fallback_title, domain_context

    # Reserve 'dead' only for outright URL syntax errors, SSL failures, or explicit invalid protocol errors
    except requests.RequestException as e:
        logger.warning(f"Fatal request exception for {url}: {e}. Treating as dead.")
        append_dead_link_audit(file_name, url, "REQUEST_EXCEPTION", 0, True)
        return "dead", "", ""

def build_few_shot_block() -> str:
    """Renders FEW_SHOT_EXAMPLES as example Input/Output pairs for the classification prompt."""
    blocks = []
    for ex in FEW_SHOT_EXAMPLES:
        blocks.append(
            f"Input:\nTitle: {ex['title']}\nURL: {ex['url']}\nSnippet/Context: {ex['snippet']}\n"
            f"Output:\n{{\"reasoning\": \"{ex['reasoning']}\", \"category\": \"{ex['category']}\"}}"
        )
    return "\n\n".join(blocks)

FEW_SHOT_BLOCK = build_few_shot_block()

# JSON schema passed to Ollama's `format` param. Constraining the model to emit valid JSON
# matching this schema (rather than free text) makes classification results far more
# consistent than parsing a raw text reply, and the enum keeps 'category' locked to a real
# option instead of the model inventing new labels or padding with explanation text.
# 'reasoning' is required and must come first -- requiring the model to state what the
# content is about before naming a category acts as lightweight chain-of-thought, which
# measurably reduces collapse-to-one-default-category on small models. The reasoning text
# itself isn't used for anything downstream, only logged at DEBUG for spot-checking.
CATEGORY_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning": {"type": "string"},
        "category": {
            "type": "string",
            "enum": ALLOWED_CATEGORIES + ["unknown"]
        }
    },
    "required": ["reasoning", "category"]
}

def categorize_content_with_ollama(client: ollama.Client, url: str, title: str, snippet: str, file_name: str) -> str:
    """Sends page metadata to local Ollama client to determine classification.

    Uses few-shot examples plus a JSON-constrained response format. Plain zero-shot
    text prompting was observed to collapse to a single default category (e.g. nearly
    everything coming back "Finance") on low-signal inputs like bare search-result pages
    or SPA shells with no real body text -- few-shot examples covering the
    under-represented categories, combined with a schema-constrained JSON response,
    fixes this far more reliably than prompt wording tweaks alone.
    """
    logger.debug(f"Requesting Ollama classification using model '{OLLAMA_MODEL}'...")

    prompt = f"""You are classifying browser bookmarks into exactly one category.

Available Categories:
{', '.join(ALLOWED_CATEGORIES)}

For each input, first write one short sentence in "reasoning" describing what the page is
about, then pick the single best-matching "category" based on that. Base your decision on
the title, URL structure, domain name, and snippet together -- don't over-weight any single
field alone. If none match well, use "unknown".

Examples:

{FEW_SHOT_BLOCK}

Now classify this one:

Input:
Title: {title if title else 'N/A'}
URL: {url}
Snippet/Context: {snippet if snippet else 'N/A'}
Output:
"""

    try:
        response = client.generate(
            model=OLLAMA_MODEL,
            prompt=prompt,
            format=CATEGORY_JSON_SCHEMA,
            # A small non-zero temperature (rather than pure greedy 0.0) helps avoid the
            # model locking onto one dominant category token across an entire batch --
            # greedy decoding on a small model tends to repeat whatever had the highest
            # prior on the first ambiguous input and then anchor there for everything
            # that follows it in the same style.
            options={"temperature": 0.2}
        )
        raw = response.get('response', '').strip()
        logger.debug(f"Raw Ollama Response: '{raw}'")

        try:
            parsed = json.loads(raw)
            category = str(parsed.get('category', '')).strip()
            reasoning = str(parsed.get('reasoning', '')).strip()
            if reasoning:
                logger.debug(f"Ollama reasoning for {file_name}: {reasoning}")
        except (json.JSONDecodeError, AttributeError):
            # Fallback in case the model/host doesn't honor `format` (older Ollama
            # versions) and returns plain text instead of JSON.
            category = raw

        for cat in ALLOWED_CATEGORIES:
            if cat.lower() == category.lower():
                return cat

        return "unknown"

    except Exception as e:
        logger.error(f"Ollama API call failed: {e}")
        return "unknown"

def stage1_fetch_worker(file_path: Path, stats: dict, total_files: int, counter: list, progress: ProgressTracker):
    """Stage 1 (runs on the large FETCH_WORKERS pool): parse the .url file and test the
    link over the network.

    Returns None if the file was already fully resolved here (no URL found, or link
    confirmed dead -- both get moved immediately and don't need the LLM). Otherwise
    returns a (file_path, target_url, title, snippet) tuple for stage 2 to classify.

    Destination folders are flat by category (categories/<Category>/) -- the file's
    *original* subfolder under SOURCE_DIR is not preserved in the destination path. That
    original folder is just wherever the file happened to be sitting pre-sort, and
    re-nesting it under the category folder produced confusing/redundant paths (e.g. a
    file starting in '2bsorted\\Finance\\' getting classified as Finance landed in
    'categories\\Finance\\Finance\\'). safe_move() already disambiguates filename
    collisions, so flattening here is safe.
    """
    with stats_lock:
        counter[0] += 1
        current_num = counter[0]
        logger.info(f"--- Processing ({current_num}/{total_files}): {file_path.relative_to(SOURCE_DIR)} ---")

    target_url = extract_url_from_file(file_path)
    if not target_url:
        logger.error(f"Could not parse valid URL from {file_path.name}. Moving to 'unknown'.")
        dest_folder = UNKNOWN_DIR
        with stats_lock:
            dest_folder.mkdir(parents=True, exist_ok=True)
            success, err = safe_move(file_path, dest_folder, file_path.name)
            stats["errors"] += 1 if success else 0
            stats["move_failed"] += 0 if success else 1
        progress.tick()
        return None

    status_result, title, snippet = fetch_webpage_info(target_url, file_path.name)

    if status_result == "dead":
        logger.info(f"Link is dead or unreachable. Moving {file_path.name} to 'dead_link'.")
        dest_folder = DEAD_LINK_DIR
        with stats_lock:
            dest_folder.mkdir(parents=True, exist_ok=True)
            success, err = safe_move(file_path, dest_folder, file_path.name)
            stats["dead"] += 1 if success else 0
            stats["move_failed"] += 0 if success else 1
        progress.tick()
        return None

    if status_result == "protected":
        logger.info(f"Link returned anti-bot protection/timeout. Fallback mode active for {file_path.name}.")

    return (file_path, target_url, title, snippet)

def stage2_classify_worker(fetch_result: tuple, client: ollama.Client, stats: dict, progress: ProgressTracker):
    """Stage 2 (runs on the small CLASSIFY_WORKERS pool): send the fetched metadata to
    Ollama and move the file into its category folder."""
    file_path, target_url, title, snippet = fetch_result
    category = categorize_content_with_ollama(client, target_url, title, snippet, file_path.name)

    with stats_lock:
        if category == "unknown":
            target_folder = UNKNOWN_DIR
            logger.info(f"Category returned 'unknown'. Moving {file_path.name} to 'unknown'.")
        else:
            folder_name = sanitize_folder_name(category)
            target_folder = BASE_TARGET_DIR / folder_name
            logger.info(f"Classified as '{category}'. Moving to '{target_folder}'.")

        target_folder.mkdir(parents=True, exist_ok=True)
        success, err = safe_move(file_path, target_folder, file_path.name)

        if not success:
            stats["move_failed"] += 1
        elif category == "unknown":
            stats["unknown"] += 1
        else:
            stats["sorted"] += 1

    progress.tick()

def cleanup_empty_folders(directory: Path):
    """Recursively deletes empty directories remaining in SOURCE_DIR after files are moved."""
    for child in list(directory.iterdir()):
        if child.is_dir():
            cleanup_empty_folders(child)
            try:
                child.rmdir()
                logger.debug(f"Removed empty directory: {child}")
            except OSError:
                pass  # Directory is not empty

def process_url_files():
    """Main process loop to scan, evaluate, classify, and sort shortcuts concurrently."""
    # Import any pending bookmarks JSON exports into SOURCE_DIR as .url files first (this
    # creates SOURCE_DIR if needed), so they're picked up by the scan below and go through
    # the same pipeline as everything else.
    imported = import_all_pending_bookmark_files()
    if imported:
        logger.info(f"Converted {imported} bookmark(s) from JSON/HTML export(s) into '{SOURCE_DIR}'.")

    imported_txt = import_url_text_files()
    if imported_txt:
        logger.info(f"Converted {imported_txt} bare-URL .txt file(s) into '{SOURCE_DIR}'.")

    imported_pb_export = import_all_pending_pushbullet_exports()
    if imported_pb_export:
        logger.info(f"Converted {imported_pb_export} URL push(es) from Pushbullet export(s) into '{SOURCE_DIR}'.")

    sync_pushbullet_pushes()

    if not SOURCE_DIR.exists():
        logger.error(f"Source directory '{SOURCE_DIR}' does not exist.")
        return

    # Pre-Scan Phase
    logger.info(f"Scanning '{SOURCE_DIR}' for .url files...")
    url_files = list(SOURCE_DIR.rglob("*.url"))
    total_files = len(url_files)

    if total_files == 0:
        logger.info("No .url files found to process.")
        return

    logger.info("================ SCAN SUMMARY ================")
    logger.info(f"Found {total_files} file(s) across '{SOURCE_DIR}' and its subfolders.")
    logger.info(f"Multitasking active: {FETCH_WORKERS} fetch threads -> {CLASSIFY_WORKERS} classify threads.")
    logger.info("==============================================")

    DEAD_LINK_DIR.mkdir(parents=True, exist_ok=True)
    UNKNOWN_DIR.mkdir(parents=True, exist_ok=True)
    setup_dead_link_audit()

    client = ollama.Client(host=OLLAMA_HOST)
    stats = {"total": total_files, "sorted": 0, "unknown": 0, "dead": 0, "errors": 0, "move_failed": 0}
    counter = [0]
    progress = ProgressTracker(total_files)

    # Two-stage pipeline: a wide pool for network fetches feeds a narrow pool for Ollama
    # classification. Fetch results are consumed via as_completed() as they arrive (not
    # after all fetches finish), so classification starts overlapping with fetches almost
    # immediately rather than waiting for the whole batch to be tested first.
    with ThreadPoolExecutor(max_workers=CLASSIFY_WORKERS) as classify_executor, \
         ThreadPoolExecutor(max_workers=FETCH_WORKERS) as fetch_executor:

        fetch_futures = {
            fetch_executor.submit(stage1_fetch_worker, file_path, stats, total_files, counter, progress): file_path
            for file_path in url_files
        }
        classify_futures = []

        for future in as_completed(fetch_futures):
            file_path = fetch_futures[future]
            try:
                result = future.result()
            except Exception as e:
                logger.error(f"Unhandled fetch worker error ({file_path.name}): {e}")
                progress.tick()
                continue
            if result is None:
                # Already fully resolved and moved inside stage1 (no URL found, or dead link).
                continue
            classify_futures.append(classify_executor.submit(stage2_classify_worker, result, client, stats, progress))

        for future in as_completed(classify_futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Unhandled classify worker error: {e}")

    # Cleanup leftover empty folders in source
    cleanup_empty_folders(SOURCE_DIR)

    logger.info("================ PROCESSING COMPLETE ================")
    logger.info(f"Total: {stats['total']} | Categorized: {stats['sorted']} | Dead: {stats['dead']} | Unknown: {stats['unknown']} | Errors: {stats['errors']} | Move failed: {stats['move_failed']}")
    if stats['move_failed'] > 0:
        logger.warning(f"{stats['move_failed']} file(s) failed to move (likely path-length or permission issues) and were left in place in '{SOURCE_DIR}'. See ERROR lines above for exact filenames.")
    if stats['dead'] > 0:
        logger.info(f"Dead-link audit written to '{DEAD_LINK_AUDIT_FILE}' -- spot-check rows marked 'suspect' before deleting anything.")

if __name__ == "__main__":
    process_url_files()