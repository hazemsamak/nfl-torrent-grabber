"""
Lightweight Web Dashboard & Status Server for NFL Torrent Grabber.
Uses standard Python library (no external web framework dependencies).
Provides real-time log streaming, manual triggers, diagnostics, and history management.
"""
import collections
import datetime
import html
import http.server
import json
import logging
import os
import sqlite3
import threading
import urllib.parse
from typing import Any, Callable, Deque, Dict, List, Optional

logger = logging.getLogger("NFLGrabber.Web")

# Thread-safe in-memory ring-buffer for logs
log_buffer: Deque[Dict[str, Any]] = collections.deque(maxlen=3000)
_log_lock = threading.Lock()
_log_counter = 0

# Execution coordinator lock
_cycle_lock = threading.Lock()

# Runtime service status metadata
_service_info: Dict[str, Any] = {
    "status": "Running",
    "started_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "next_run": "Calculating...",
    "last_run": "None yet",
    "active_week": "Scanning...",
    "db_path": None,
    "is_cycle_running": False,
}

# Injected service context references
_service_context: Dict[str, Any] = {
    "config": None,
    "state": None,
    "notifier": None,
    "cycle_runner": None,
    "schedule_logger": None,
    "schedule_reloader": None,
}

# Cached active week matches
_cached_matches_lock = threading.Lock()
_cached_week_data: Dict[str, Any] = {
    "week": None,
    "fetched_at": None,
    "matches": [],
}


def update_cached_matches(week: str, matches: List[Dict[str, Any]]):
    """Thread-safe update of cached active week matches."""
    with _cached_matches_lock:
        _cached_week_data["week"] = week
        _cached_week_data["fetched_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _cached_week_data["matches"] = matches


def update_service_status(**kwargs):
    """Thread-safe update of service runtime status."""
    with _log_lock:
        _service_info.update(kwargs)
        _service_info["is_cycle_running"] = _cycle_lock.locked()


def get_total_grabbed_torrents(db_path: Optional[str] = None) -> int:
    """Reads historical torrent count from the SQLite database."""
    target_path = db_path or _service_info.get("db_path")
    if not target_path or not os.path.isfile(target_path):
        return 0
    try:
        conn = sqlite3.connect(target_path, timeout=2.0)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_torrents;")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


def run_cycle_synchronized(
    dry_run: bool = False,
    caller: str = "scheduler",
    favorite_only: Optional[bool] = None,
) -> tuple[bool, str]:
    """
    Executes a grabber cycle under an exclusive lock.
    Prevents overlapping execution between scheduled and manual runs.
    """
    if not _cycle_lock.acquire(blocking=False):
        logger.warning(
            "Cycle trigger skipped: grabber cycle already running (requested by %s).",
            caller,
        )
        return False, "Grabber cycle already in progress"

    try:
        update_service_status(status="Active Cycle Running", is_cycle_running=True)
        runner = _service_context.get("cycle_runner")
        if runner:
            runner(dry_run=dry_run, favorite_only=favorite_only)
        update_service_status(
            status="Running",
            last_run=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            is_cycle_running=False,
        )
        sched = _service_context.get("schedule_logger")
        if sched:
            try:
                sched()
            except Exception:
                pass
        return True, "Grabber cycle completed successfully"
    except Exception as e:
        logger.error("Error during grabber cycle: %s", e)
        update_service_status(status="Error", is_cycle_running=False)
        return False, str(e)
    finally:
        _cycle_lock.release()
        update_service_status(is_cycle_running=False)


def trigger_cycle_async(dry_run: bool = False, favorite_only: bool = False) -> tuple[bool, str]:
    """Spawns an asynchronous grabber cycle in a daemon thread if lock is free."""
    if _cycle_lock.locked():
        return False, "Grabber cycle already in progress"

    t = threading.Thread(
        target=run_cycle_synchronized,
        kwargs={"dry_run": dry_run, "caller": "web_ui", "favorite_only": favorite_only},
        daemon=True,
        name="ManualGrabberCycle",
    )
    t.start()
    mode = "Dry Run (Preview)" if dry_run else "Standard Run"
    return True, f"{mode} initiated in background."


# --------------------------------------------------------------------------
# Diagnostics & Data Helpers
# --------------------------------------------------------------------------

def run_qbit_diagnostic() -> Dict[str, Any]:
    """Tests qBittorrent connection, credentials, and API version."""
    cfg = _service_context.get("config")
    if not cfg:
        return {"ok": False, "error": "Service configuration not loaded"}
    try:
        from src.qbittorrent import QBittorrentClient

        qbit = QBittorrentClient(
            host=cfg.qbit_host,
            api_key=cfg.qbit_api_key,
            username=cfg.qbit_username,
            password=cfg.qbit_password,
            category=cfg.qbit_category,
            tags=cfg.qbit_tags,
            save_path=cfg.qbit_save_path,
        )
        client = qbit.get_client()
        version = getattr(client.app, "version", "unknown")
        web_api = getattr(client.app, "web_api_version", "unknown")
        return {
            "ok": True,
            "version": version,
            "web_api_version": web_api,
            "host": cfg.qbit_host,
            "category": cfg.qbit_category,
            "save_path": cfg.qbit_save_path,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "host": getattr(cfg, "qbit_host", "")}


def run_pier_diagnostic() -> Dict[str, Any]:
    """Validates 720pier forum connection and session cookie freshness."""
    cfg = _service_context.get("config")
    if not cfg:
        return {"ok": False, "error": "Service configuration not loaded"}
    try:
        from bs4 import BeautifulSoup
        from src.scraper import PierScraper

        scraper = PierScraper(
            thread_url=cfg.pier_thread_url,
            username=cfg.pier_username,
            password=cfg.pier_password,
            cookie_string=cfg.pier_cookie,
            proxy_url=cfg.proxy_url,
        )
        html_doc = scraper.fetch_thread_html()
        soup = BeautifulSoup(html_doc, "lxml")
        week_title, _ = scraper.find_latest_week_section(soup)
        return {
            "ok": True,
            "detected_week": week_title,
            "thread_url": cfg.pier_thread_url,
            "has_cookie": bool(cfg.pier_cookie),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "thread_url": getattr(cfg, "pier_thread_url", "")}


def run_alert_diagnostic() -> Dict[str, Any]:
    """Dispatches a test notification to configured alert channels."""
    notifier = _service_context.get("notifier")
    if not notifier:
        return {"ok": False, "error": "Notifier service not initialized"}
    try:
        results = notifier.send_test_notification()
        return {"ok": True, "results": results}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_processed_torrents_data(limit: int = 150) -> List[Dict[str, Any]]:
    """Fetches recently processed torrents from database."""
    state = _service_context.get("state")
    if state and hasattr(state, "get_processed_torrents"):
        return state.get_processed_torrents(limit=limit)

    target_path = _service_info.get("db_path")
    if not target_path or not os.path.isfile(target_path):
        return []
    try:
        conn = sqlite3.connect(target_path, timeout=3.0)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT id, torrent_url, title, week, rating, added_at
            FROM processed_torrents
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def delete_processed_torrent_record(identifier: str) -> bool:
    """Removes a processed torrent from SQLite state."""
    state = _service_context.get("state")
    if state and hasattr(state, "delete_processed_torrent"):
        return state.delete_processed_torrent(identifier)

    target_path = _service_info.get("db_path")
    if not target_path or not os.path.isfile(target_path):
        return False
    try:
        conn = sqlite3.connect(target_path, timeout=3.0)
        with conn:
            if str(identifier).isdigit():
                cur = conn.execute("DELETE FROM processed_torrents WHERE id = ?", (int(identifier),))
                if cur.rowcount > 0:
                    conn.close()
                    return True
            cur = conn.execute("DELETE FROM processed_torrents WHERE torrent_url = ?", (str(identifier),))
            res = cur.rowcount > 0
        conn.close()
        return res
    except Exception:
        return False


def get_runs_data(limit: int = 50) -> List[Dict[str, Any]]:
    """Fetches run history records from SQLite database."""
    state = _service_context.get("state")
    if state and hasattr(state, "get_recent_runs"):
        return state.get_recent_runs(limit=limit)

    target_path = _service_info.get("db_path")
    if not target_path or not os.path.isfile(target_path):
        return []
    try:
        conn = sqlite3.connect(target_path, timeout=3.0)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT id, started_at, completed_at, status, week_title, torrents_added_count, next_run_at, error_message
            FROM runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def fetch_active_week_matches(force_refresh: bool = False) -> Dict[str, Any]:
    """
    Fetches matches for the active week.
    Returns cached list if available unless force_refresh is True.
    Updates each match's is_grabbed status against the database.
    """
    state = _service_context.get("state")
    with _cached_matches_lock:
        if not force_refresh and _cached_week_data.get("matches"):
            week = _cached_week_data.get("week")
            for m in _cached_week_data["matches"]:
                if state:
                    if hasattr(state, "is_match_or_url_processed"):
                        m["is_grabbed"] = state.is_match_or_url_processed(m.get("url", ""), m.get("title", ""), week=week)
                    else:
                        m["is_grabbed"] = state.is_torrent_processed(m.get("url", ""))
            return {
                "ok": True,
                "week": _cached_week_data.get("week", "Active Week"),
                "fetched_at": _cached_week_data.get("fetched_at"),
                "count": len(_cached_week_data["matches"]),
                "matches": list(_cached_week_data["matches"]),
                "source": "cache",
            }

    cfg = _service_context.get("config")
    if not cfg:
        return {"ok": False, "error": "Configuration not loaded", "matches": []}

    try:
        from bs4 import BeautifulSoup
        from src.scraper import PierScraper

        scraper = PierScraper(
            thread_url=cfg.pier_thread_url,
            username=cfg.pier_username,
            password=cfg.pier_password,
            cookie_string=cfg.pier_cookie,
            quality_preference=cfg.quality_preference,
            proxy_url=cfg.proxy_url,
        )
        html_doc = scraper.fetch_thread_html()
        soup = BeautifulSoup(html_doc, "lxml")
        week_title, week_elem = scraper.find_latest_week_section(soup)
        raw_matches = scraper.get_all_matches_in_week(week_elem)

        processed = []
        for m in raw_matches:
            title = m.get("title", "")
            url = m.get("url", "")
            rating = m.get("rating", "N/A")
            is_g = False
            if state:
                if hasattr(state, "is_match_or_url_processed"):
                    is_g = state.is_match_or_url_processed(url, title, week=week_title)
                else:
                    is_g = state.is_torrent_processed(url)
            is_fav = scraper.matches_favorite_team(title, cfg.favorite_teams)
            processed.append({
                "title": title,
                "rating": rating,
                "url": url,
                "is_grabbed": is_g,
                "is_favorite": is_fav,
            })

        update_cached_matches(week_title, processed)
        update_service_status(active_week=week_title)

        with _cached_matches_lock:
            return {
                "ok": True,
                "week": week_title,
                "fetched_at": _cached_week_data["fetched_at"],
                "count": len(processed),
                "matches": list(processed),
                "source": "live",
            }
    except Exception as e:
        logger.error("Failed to fetch active week matches from 720pier: %s", e)
        return {"ok": False, "error": str(e), "matches": []}


def download_single_match(url: str, title: str, rating: Optional[str] = None, week: Optional[str] = None) -> tuple[bool, str]:
    """
    Downloads and forwards a single game release to qBittorrent, records it in SQLite,
    and dispatches a manual grab alert notification.
    """
    cfg = _service_context.get("config")
    state = _service_context.get("state")
    notifier = _service_context.get("notifier")

    if not cfg:
        return False, "Configuration not initialized"

    try:
        from src.scraper import PierScraper
        from src.qbittorrent import QBittorrentClient
        from src.main import format_match_name

        scraper = PierScraper(
            thread_url=cfg.pier_thread_url,
            username=cfg.pier_username,
            password=cfg.pier_password,
            cookie_string=cfg.pier_cookie,
            quality_preference=cfg.quality_preference,
            proxy_url=cfg.proxy_url,
        )

        qbit = QBittorrentClient(
            host=cfg.qbit_host,
            api_key=cfg.qbit_api_key,
            username=cfg.qbit_username,
            password=cfg.qbit_password,
            category=cfg.qbit_category,
            tags=cfg.qbit_tags,
            save_path=cfg.qbit_save_path,
            auto_start=cfg.qbit_auto_start,
        )

        # 1. Resolve game topic page if link is a forum thread
        torrent_url = url
        topic_url = url
        resolved_title = title

        if "viewtopic.php" in url:
            info = scraper._extract_torrent_from_game_topic(url, title)
            if info and info.get("torrent_url"):
                torrent_url = info["torrent_url"]
                resolved_title = info.get("title", title)
            else:
                return False, f"Could not find torrent download link on release page: {url}"

        # 2. Download torrent binary
        fn = f"{resolved_title}.torrent"
        torrent_bytes, filename = scraper.download_torrent(torrent_url, fn, referer=topic_url)

        # 3. Format name
        torrent_meta = {"title": resolved_title, "matchup": title}
        effective_week = week or _service_info.get("active_week") or "NFL"
        custom_name = format_match_name(effective_week, torrent_meta) if cfg.rename_torrent_files else None

        # 4. Push to qBittorrent
        success = qbit.add_torrent_file(torrent_bytes, filename, custom_name=custom_name)
        if not success:
            return False, f"qBittorrent rejected torrent '{filename}'"

        # 5. Record in database
        if state:
            state.record_processed_torrent(
                torrent_url,
                resolved_title,
                effective_week,
                rating=str(rating) if rating and rating != "N/A" else None,
            )

        # 6. Update cache
        with _cached_matches_lock:
            for m in _cached_week_data.get("matches", []):
                if m.get("url") == url or m.get("title") == title:
                    m["is_grabbed"] = True

        # 7. Send notification alert
        if notifier:
            try:
                is_fav = scraper.matches_favorite_team(resolved_title, cfg.favorite_teams)
                item = {
                    "title": resolved_title,
                    "rating": rating or "N/A",
                    "torrent_url": torrent_url,
                    "is_favorite_team": is_fav,
                }
                notifier.notify_torrents_added(
                    effective_week,
                    [item],
                    next_run_str="Manually grabbed via Web UI",
                )
            except Exception as n_err:
                logger.warning("Error dispatching manual grab notification: %s", n_err)

        return True, f"Successfully pushed '{custom_name or resolved_title}' to qBittorrent!"
    except Exception as e:
        logger.exception("Error during manual match download for '%s': %s", title, e)
        return False, str(e)


class MemoryLogHandler(logging.Handler):
    """Logging handler that pipes log records into the in-memory web ring-buffer."""

    def __init__(self, log_file: Optional[str] = None):
        super().__init__()
        self.log_file = log_file
        if self.log_file:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.log_file)), exist_ok=True)
            except Exception:
                pass

    def emit(self, record: logging.LogRecord):
        global _log_counter
        try:
            formatted_msg = self.format(record)
            entry = {
                "id": None,
                "timestamp": datetime.datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "raw": formatted_msg,
            }
            with _log_lock:
                _log_counter += 1
                entry["id"] = _log_counter
                log_buffer.append(entry)

            if self.log_file:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(formatted_msg + "\n")
        except Exception:
            self.handleError(record)


# --------------------------------------------------------------------------
# HTML, CSS & JavaScript Web UI
# --------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NFL Grabber &mdash; Dashboard &amp; Control Center</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;500;600&family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090d16;
      --bg-surface: #111827;
      --bg-surface-elevated: #1a2234;
      --bg-surface-hover: #222d44;
      --bg-input: #0d1322;
      --border: rgba(255, 255, 255, 0.08);
      --border-card: rgba(255, 255, 255, 0.12);
      --border-focus: #38bdf8;
      --text: #f8fafc;
      --text-secondary: #94a3b8;
      --muted: #64748b;
      --cyan: #38bdf8;
      --cyan-glow: rgba(56, 189, 248, 0.2);
      --green: #10b981;
      --green-glow: rgba(16, 185, 129, 0.2);
      --orange: #f59e0b;
      --pink: #ec4899;
      --purple: #8b5cf6;
      --red: #f43f5e;
      --yellow: #eab308;
      --radius-sm: 6px;
      --radius-md: 10px;
      --radius-lg: 14px;
      --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.4);
      --shadow-md: 0 4px 14px rgba(0, 0, 0, 0.5), 0 1px 3px rgba(0, 0, 0, 0.3);
      --shadow-lg: 0 10px 25px -5px rgba(0, 0, 0, 0.6);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      height: 100%;
      height: 100vh;
      height: 100dvh;
      margin: 0;
      padding: 0;
      overflow: hidden;
    }

    body {
      background-color: var(--bg);
      color: var(--text);
      font-family: 'Inter', system-ui, -apple-system, sans-serif;
      display: flex;
      flex-direction: column;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
    }

    /* SVG Icon Helper */
    .icon {
      width: 16px;
      height: 16px;
      vertical-align: middle;
      display: inline-block;
      stroke-width: 2;
      stroke: currentColor;
      fill: none;
      stroke-linecap: round;
      stroke-linejoin: round;
      flex-shrink: 0;
    }
    .icon-sm { width: 14px; height: 14px; }
    .icon-lg { width: 20px; height: 20px; }

    /* Header */
    header {
      flex-shrink: 0;
      background: rgba(17, 24, 39, 0.85);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border-bottom: 1px solid var(--border);
      padding: 0.75rem 1.25rem;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      z-index: 20;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 0.85rem;
    }

    .brand-logo {
      width: 38px;
      height: 38px;
      border-radius: var(--radius-md);
      background: linear-gradient(135deg, #0284c7 0%, #6366f1 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      color: #fff;
      box-shadow: 0 2px 10px rgba(2, 132, 199, 0.35);
    }

    .brand-title {
      display: flex;
      flex-direction: column;
    }

    .brand-title h1 {
      font-size: 1.15rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .brand-title span.tag-pro {
      font-size: 0.65rem;
      font-weight: 700;
      text-transform: uppercase;
      padding: 0.15rem 0.45rem;
      border-radius: 9999px;
      background: rgba(56, 189, 248, 0.15);
      border: 1px solid rgba(56, 189, 248, 0.3);
      color: var(--cyan);
      letter-spacing: 0.05em;
    }

    .brand-title p {
      font-size: 0.73rem;
      color: var(--muted);
      font-weight: 500;
    }

    .badge-status {
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: var(--green);
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      padding: 0.25rem 0.65rem;
      border-radius: 9999px;
      transition: all 0.25s ease;
      letter-spacing: 0.03em;
    }

    .badge-status.running {
      background: rgba(245, 158, 11, 0.15);
      border-color: rgba(245, 158, 11, 0.4);
      color: var(--orange);
    }

    .badge-pulse {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background-color: currentColor;
      box-shadow: 0 0 8px currentColor;
      animation: pulse 2s infinite ease-in-out;
    }

    @keyframes pulse {
      0% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.35; transform: scale(0.85); }
      100% { opacity: 1; transform: scale(1); }
    }

    /* Stats Ribbon (Hero KPI Strip) */
    .stats-ribbon {
      flex-shrink: 0;
      background: #0d121e;
      border-bottom: 1px solid var(--border);
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.75rem;
      padding: 0.65rem 1.25rem;
    }

    .stat-item {
      display: flex;
      align-items: center;
      gap: 0.65rem;
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      padding: 0.45rem 0.85rem;
      transition: border-color 0.15s ease;
    }

    .stat-item:hover {
      border-color: var(--border-card);
    }

    .stat-icon {
      width: 28px;
      height: 28px;
      border-radius: var(--radius-sm);
      background: var(--bg-surface-elevated);
      color: var(--cyan);
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }

    .stat-content {
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .stat-label {
      color: var(--muted);
      font-size: 0.7rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }

    .stat-val {
      color: #fff;
      font-family: 'Fira Code', monospace;
      font-weight: 600;
      font-size: 0.82rem;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    /* Action & Tab Toolbar */
    .action-toolbar {
      flex-shrink: 0;
      background-color: #0b0f19;
      border-bottom: 1px solid var(--border);
      padding: 0.55rem 1.25rem;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
    }

    .tabs-group {
      display: flex;
      gap: 0.35rem;
      background: var(--bg-surface);
      padding: 0.25rem;
      border-radius: var(--radius-md);
      border: 1px solid var(--border);
    }

    .tab-btn {
      background: transparent;
      border: none;
      color: var(--text-secondary);
      padding: 0.4rem 0.85rem;
      font-size: 0.8rem;
      font-weight: 600;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      transition: all 0.15s ease;
    }

    .tab-btn:hover {
      color: #fff;
      background: var(--bg-surface-hover);
    }

    .tab-btn.active {
      background: var(--cyan);
      color: #090d16;
      font-weight: 700;
      box-shadow: 0 2px 8px var(--cyan-glow);
    }

    .tab-btn.active .tab-counter {
      background: rgba(9, 13, 22, 0.25);
      color: #090d16;
    }

    .tab-counter {
      background: var(--bg-surface-elevated);
      color: var(--text-secondary);
      font-size: 0.68rem;
      font-family: 'Fira Code', monospace;
      padding: 0.1rem 0.45rem;
      border-radius: 9999px;
      font-weight: 600;
      transition: all 0.15s ease;
    }

    .actions-right {
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    /* Buttons */
    .btn-diagnostic {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      padding: 0.45rem 0.85rem;
      font-size: 0.78rem;
      font-weight: 600;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      transition: all 0.15s ease;
    }

    .btn-diagnostic:hover {
      border-color: var(--border-card);
      color: #fff;
      background: var(--bg-surface-hover);
    }

    .btn-diagnostic:active {
      transform: scale(0.98);
    }

    /* Split Trigger Button */
    .split-btn {
      display: inline-flex;
      position: relative;
    }

    .btn-main-trigger {
      background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%);
      color: #fff;
      border: 1px solid rgba(255, 255, 255, 0.15);
      border-right: none;
      padding: 0.45rem 0.95rem;
      font-size: 0.8rem;
      font-weight: 700;
      border-top-left-radius: var(--radius-sm);
      border-bottom-left-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.45rem;
      transition: all 0.15s ease;
      box-shadow: 0 2px 8px rgba(2, 132, 199, 0.3);
    }

    .btn-main-trigger:hover:not(:disabled) {
      background: linear-gradient(135deg, #0369a1 0%, #0284c7 100%);
      box-shadow: 0 3px 12px rgba(2, 132, 199, 0.45);
    }

    .btn-main-trigger:disabled {
      opacity: 0.65;
      cursor: not-allowed;
    }

    .btn-dropdown-caret {
      background: #0284c7;
      color: #fff;
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 0.45rem 0.55rem;
      font-size: 0.72rem;
      border-top-right-radius: var(--radius-sm);
      border-bottom-right-radius: var(--radius-sm);
      cursor: pointer;
      transition: all 0.15s ease;
      display: flex;
      align-items: center;
    }

    .btn-dropdown-caret:hover {
      background: #0369a1;
    }

    .dropdown-menu {
      position: absolute;
      top: calc(100% + 6px);
      right: 0;
      background: var(--bg-surface-elevated);
      border: 1px solid var(--border-card);
      border-radius: var(--radius-sm);
      box-shadow: var(--shadow-lg);
      padding: 0.35rem;
      display: none;
      flex-direction: column;
      gap: 0.2rem;
      min-width: 210px;
      z-index: 100;
      animation: fadeIn 0.12s ease-out;
    }

    .dropdown-menu.show {
      display: flex;
    }

    .dropdown-item {
      background: transparent;
      border: none;
      color: var(--text-secondary);
      padding: 0.45rem 0.75rem;
      font-size: 0.78rem;
      font-weight: 500;
      text-align: left;
      border-radius: 4px;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      transition: all 0.12s ease;
    }

    .dropdown-item:hover {
      background: var(--bg-surface-hover);
      color: #fff;
    }

    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(-4px); }
      to { opacity: 1; transform: translateY(0); }
    }

    /* Tab Content Area */
    .tab-content-area {
      flex: 1;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      position: relative;
    }

    .tab-pane {
      display: none;
      flex: 1;
      overflow: hidden;
      flex-direction: column;
      height: 100%;
    }

    .tab-pane.active {
      display: flex;
    }

    /* Sub Toolbar */
    .sub-bar {
      flex-shrink: 0;
      background: #0f1422;
      border-bottom: 1px solid var(--border);
      padding: 0.55rem 1.25rem;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
    }

    .search-box {
      position: relative;
      display: flex;
      align-items: center;
      min-width: 260px;
    }

    .search-box .icon {
      position: absolute;
      left: 0.65rem;
      color: var(--muted);
      pointer-events: none;
    }

    .search-box input {
      width: 100%;
      background: var(--bg-input);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      color: #fff;
      padding: 0.4rem 0.65rem 0.4rem 2rem;
      font-size: 0.8rem;
      outline: none;
      transition: border-color 0.15s ease, box-shadow 0.15s ease;
    }

    .search-box input:focus {
      border-color: var(--cyan);
      box-shadow: 0 0 0 2px var(--cyan-glow);
    }

    .btn-sm {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      padding: 0.35rem 0.75rem;
      font-size: 0.76rem;
      font-weight: 600;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      transition: all 0.15s ease;
    }

    .btn-sm:hover {
      border-color: var(--border-card);
      color: #fff;
      background: var(--bg-surface-hover);
    }

    /* Table Container */
    .table-container {
      flex: 1;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      background: var(--bg);
    }

    .table-scroll-container {
      flex: 1;
      overflow-y: auto;
      overflow-x: auto;
    }

    table.data-table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 0.8rem;
    }

    table.data-table th {
      background: #111827;
      color: var(--text-secondary);
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 0.65rem 1rem;
      border-bottom: 1px solid var(--border-card);
      position: sticky;
      top: 0;
      z-index: 10;
      white-space: nowrap;
    }

    table.data-table td {
      padding: 0.7rem 1rem;
      border-bottom: 1px solid var(--border);
      color: var(--text);
      vertical-align: middle;
      transition: background-color 0.1s ease;
    }

    table.data-table tr:hover td {
      background-color: rgba(255, 255, 255, 0.025);
    }

    /* Badges & Tags */
    .tag-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      padding: 0.15rem 0.55rem;
      border-radius: 9999px;
      font-size: 0.72rem;
      font-weight: 700;
      font-family: 'Fira Code', monospace;
      white-space: nowrap;
    }

    .tag-high {
      background: rgba(16, 185, 129, 0.15);
      border: 1px solid rgba(16, 185, 129, 0.4);
      color: #34d399;
    }

    .tag-med {
      background: rgba(56, 189, 248, 0.15);
      border: 1px solid rgba(56, 189, 248, 0.35);
      color: #38bdf8;
    }

    .tag-low {
      background: rgba(148, 163, 184, 0.12);
      border: 1px solid rgba(148, 163, 184, 0.25);
      color: #94a3b8;
    }

    .tag-fav {
      background: rgba(245, 158, 11, 0.18);
      border: 1px solid rgba(245, 158, 11, 0.45);
      color: #fbbf24;
      font-weight: 800;
    }

    .tag-week {
      background: rgba(139, 92, 246, 0.15);
      border: 1px solid rgba(139, 92, 246, 0.35);
      color: #c084fc;
    }

    .tag-status-success {
      background: rgba(16, 185, 129, 0.15);
      border: 1px solid rgba(16, 185, 129, 0.4);
      color: #34d399;
    }

    .tag-status-error {
      background: rgba(244, 63, 94, 0.15);
      border: 1px solid rgba(244, 63, 94, 0.4);
      color: #fb7185;
    }

    .tag-status-notorrents {
      background: rgba(56, 189, 248, 0.12);
      border: 1px solid rgba(56, 189, 248, 0.3);
      color: #7dd3fc;
    }

    .tag-status-running {
      background: rgba(245, 158, 11, 0.15);
      border: 1px solid rgba(245, 158, 11, 0.4);
      color: #fcd34d;
    }

    .badge-success {
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.35);
      color: #34d399;
      font-size: 0.72rem;
      font-weight: 700;
      padding: 0.25rem 0.6rem;
      border-radius: 9999px;
    }

    /* Matchup Visual Elements */
    .match-card-title {
      font-weight: 600;
      color: #fff;
      margin-bottom: 0.2rem;
    }

    .match-meta-pills {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      align-items: center;
    }

    .meta-pill {
      font-size: 0.68rem;
      padding: 0.08rem 0.45rem;
      border-radius: 4px;
      background: var(--bg-surface-elevated);
      color: var(--muted);
      border: 1px solid var(--border);
      font-family: 'Fira Code', monospace;
    }

    /* Action Buttons in Tables */
    .btn-reset {
      background: rgba(244, 63, 94, 0.12);
      border: 1px solid rgba(244, 63, 94, 0.3);
      color: #fb7185;
      padding: 0.3rem 0.65rem;
      font-size: 0.72rem;
      font-weight: 600;
      border-radius: var(--radius-sm);
      cursor: pointer;
      transition: all 0.15s ease;
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
    }

    .btn-reset:hover {
      background: rgba(244, 63, 94, 0.25);
      border-color: #fb7185;
      color: #fff;
    }

    .btn-grab {
      background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%);
      color: #fff;
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 0.35rem 0.75rem;
      font-size: 0.74rem;
      font-weight: 600;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      transition: all 0.15s ease;
    }

    .btn-grab:hover:not(:disabled) {
      background: linear-gradient(135deg, #0369a1 0%, #0284c7 100%);
      box-shadow: 0 2px 8px var(--cyan-glow);
    }

    .btn-grab:disabled {
      opacity: 0.65;
      cursor: not-allowed;
    }

    .link-action {
      color: var(--cyan);
      text-decoration: none;
      font-size: 0.76rem;
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      transition: color 0.12s ease;
    }

    .link-action:hover {
      color: #7dd3fc;
      text-decoration: underline;
    }

    /* Settings Tab */
    .settings-container {
      flex: 1;
      overflow-y: auto;
      padding: 1.5rem;
      max-width: 1100px;
      margin: 0 auto;
      width: 100%;
    }

    .settings-section {
      background: var(--bg-surface);
      border: 1px solid var(--border-card);
      border-radius: var(--radius-md);
      padding: 1.25rem;
      margin-bottom: 1.25rem;
      box-shadow: var(--shadow-sm);
    }

    .settings-section-title {
      font-size: 0.95rem;
      font-weight: 700;
      color: #fff;
      margin-bottom: 1rem;
      display: flex;
      align-items: center;
      gap: 0.55rem;
      padding-bottom: 0.65rem;
      border-bottom: 1px solid var(--border);
    }

    .settings-section-title .icon {
      color: var(--cyan);
    }

    .settings-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 1.15rem;
    }

    @media (max-width: 768px) {
      .settings-grid {
        grid-template-columns: 1fr;
      }
    }

    .form-group {
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
    }

    .form-group label {
      font-size: 0.76rem;
      font-weight: 600;
      color: var(--text-secondary);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .form-group input[type="text"],
    .form-group input[type="number"],
    .form-group input[type="password"],
    .form-group select,
    .form-group textarea {
      background: var(--bg-input);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      color: #fff;
      padding: 0.5rem 0.75rem;
      font-size: 0.82rem;
      outline: none;
      transition: border-color 0.15s ease, box-shadow 0.15s ease;
      font-family: inherit;
    }

    .form-group textarea {
      font-family: 'Fira Code', monospace;
      font-size: 0.78rem;
      resize: vertical;
    }

    .form-group input:focus,
    .form-group select:focus,
    .form-group textarea:focus {
      border-color: var(--cyan);
      box-shadow: 0 0 0 2px var(--cyan-glow);
    }

    .form-group small {
      font-size: 0.7rem;
      color: var(--muted);
      line-height: 1.35;
    }

    /* Modern Switch & Checkbox */
    .form-switch-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.6rem 0.85rem;
      background: var(--bg-surface-elevated);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      margin-top: 0.35rem;
    }

    .form-switch-info {
      display: flex;
      flex-direction: column;
      gap: 0.15rem;
    }

    .form-switch-info span {
      font-size: 0.8rem;
      font-weight: 600;
      color: #fff;
    }

    .form-switch-info small {
      font-size: 0.7rem;
      color: var(--muted);
    }

    .switch-label {
      position: relative;
      display: inline-block;
      width: 42px;
      height: 24px;
      flex-shrink: 0;
      cursor: pointer;
    }

    .switch-label input {
      opacity: 0;
      width: 0;
      height: 0;
    }

    .switch-slider {
      position: absolute;
      cursor: pointer;
      top: 0; left: 0; right: 0; bottom: 0;
      background-color: var(--border-card);
      transition: .2s;
      border-radius: 24px;
    }

    .switch-slider:before {
      position: absolute;
      content: "";
      height: 18px;
      width: 18px;
      left: 3px;
      bottom: 3px;
      background-color: white;
      transition: .2s;
      border-radius: 50%;
    }

    .switch-label input:checked + .switch-slider {
      background-color: var(--cyan);
    }

    .switch-label input:checked + .switch-slider:before {
      transform: translateX(18px);
    }

    /* Day Chips Selector */
    .day-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      margin-top: 0.25rem;
    }

    .day-chip {
      background: var(--bg-surface-elevated);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      font-size: 0.72rem;
      font-weight: 600;
      padding: 0.25rem 0.55rem;
      border-radius: 4px;
      cursor: pointer;
      transition: all 0.12s ease;
      user-select: none;
    }

    .day-chip:hover {
      color: #fff;
      border-color: var(--border-card);
    }

    .day-chip.active {
      background: rgba(56, 189, 248, 0.18);
      border-color: var(--cyan);
      color: var(--cyan);
    }

    .team-presets {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      margin-top: 0.35rem;
    }

    .team-chip {
      font-size: 0.7rem;
      padding: 0.15rem 0.45rem;
      border-radius: 4px;
      background: var(--bg-surface-elevated);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      cursor: pointer;
      transition: all 0.12s ease;
    }

    .team-chip:hover {
      border-color: var(--cyan);
      color: #fff;
    }

    .team-chip.selected {
      background: rgba(245, 158, 11, 0.18);
      border-color: var(--orange);
      color: #fbbf24;
    }

    .input-with-icon {
      position: relative;
      display: flex;
      align-items: center;
    }

    .input-with-icon input {
      width: 100%;
      padding-right: 2.2rem !important;
    }

    .btn-toggle-pwd {
      position: absolute;
      right: 0.5rem;
      background: transparent;
      border: none;
      color: var(--muted);
      cursor: pointer;
      padding: 0.2rem;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .btn-toggle-pwd:hover {
      color: #fff;
    }

    .btn-save {
      background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%);
      color: #fff;
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 0.55rem 1.4rem;
      font-size: 0.82rem;
      font-weight: 700;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      box-shadow: 0 2px 10px rgba(2, 132, 199, 0.35);
      transition: all 0.15s ease;
    }

    .btn-save:hover {
      background: linear-gradient(135deg, #0369a1 0%, #0284c7 100%);
      box-shadow: 0 4px 14px rgba(2, 132, 199, 0.5);
    }

    .btn-save:disabled {
      opacity: 0.65;
      cursor: not-allowed;
    }

    /* Modal Component */
    .modal-overlay {
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0, 0, 0, 0.7);
      backdrop-filter: blur(4px);
      -webkit-backdrop-filter: blur(4px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 1000;
      padding: 1rem;
    }

    .modal-overlay.show {
      display: flex;
    }

    .modal-card {
      background: var(--bg-surface);
      border: 1px solid var(--border-card);
      border-radius: var(--radius-md);
      box-shadow: var(--shadow-lg);
      max-width: 500px;
      width: 100%;
      overflow: hidden;
      animation: modalScale 0.15s ease-out;
    }

    @keyframes modalScale {
      from { opacity: 0; transform: scale(0.95); }
      to { opacity: 1; transform: scale(1); }
    }

    .modal-header {
      padding: 1rem 1.25rem;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .modal-header h3 {
      font-size: 0.95rem;
      font-weight: 700;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .modal-close {
      background: transparent;
      border: none;
      color: var(--muted);
      cursor: pointer;
      padding: 0.2rem;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .modal-close:hover {
      color: #fff;
    }

    .modal-body {
      padding: 1.25rem;
      font-size: 0.82rem;
      color: var(--text-secondary);
      line-height: 1.5;
    }

    .modal-footer {
      padding: 0.75rem 1.25rem;
      background: var(--bg-surface-elevated);
      border-top: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 0.65rem;
    }

    /* Toasts */
    #toast-container {
      position: fixed;
      bottom: 1.25rem;
      right: 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.55rem;
      z-index: 2000;
      pointer-events: none;
    }

    .toast {
      background: #111827;
      border: 1px solid var(--border-card);
      border-radius: var(--radius-sm);
      box-shadow: var(--shadow-lg);
      color: #fff;
      font-size: 0.8rem;
      padding: 0.75rem 1rem;
      display: flex;
      align-items: center;
      gap: 0.65rem;
      min-width: 280px;
      max-width: 420px;
      pointer-events: auto;
      animation: toastSlide 0.25s cubic-bezier(0.16, 1, 0.3, 1);
    }

    @keyframes toastSlide {
      from { opacity: 0; transform: translateX(40px); }
      to { opacity: 1; transform: translateX(0); }
    }

    .toast-success { border-left: 4px solid var(--green); }
    .toast-error { border-left: 4px solid var(--red); }
    .toast-warning { border-left: 4px solid var(--orange); }
    .toast-info { border-left: 4px solid var(--cyan); }
  </style>
</head>
<body>

  <!-- App Header -->
  <header>
    <div class="brand">
      <div class="brand-logo">
        <svg class="icon icon-lg" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
      </div>
      <div class="brand-title">
        <h1>NFL Grabber <span class="tag-pro">v2.0</span></h1>
        <p>Automated NFL Game Torrent Grabber &amp; Manager</p>
      </div>
      <div class="badge-status" id="badge-status-wrap">
        <div class="badge-pulse"></div>
        <span id="badge-status-text">INITIALIZING</span>
      </div>
    </div>

    <div class="actions-right">
      <!-- Diagnostics Trigger Buttons -->
      <button class="btn-diagnostic" id="btn-diag-qbit" title="Test qBittorrent connectivity &amp; auth">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>
        <span>Test qBit</span>
      </button>
      <button class="btn-diagnostic" id="btn-diag-pier" title="Test 720pier session &amp; thread fetch">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>
        <span>Test 720pier</span>
      </button>
      <button class="btn-diagnostic" id="btn-diag-alert" title="Send a test notification alert">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
        <span>Send Alert</span>
      </button>

      <!-- Execution Split Button -->
      <div class="split-btn">
        <button class="btn-main-trigger" id="btn-run-now" title="Trigger an immediate live grabber check cycle">
          <span id="run-btn-icon"><svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3" fill="currentColor"/></svg></span>
          <span id="run-btn-text">Run Check Now</span>
        </button>
        <button class="btn-dropdown-caret" id="btn-run-caret" title="More trigger options">
          <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="6 9 12 15 18 9"/></svg>
        </button>
        <div class="dropdown-menu" id="run-dropdown-menu">
          <button class="dropdown-item" id="btn-opt-live">
            <svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3" fill="currentColor"/></svg>
            <span>Run Check Now (Live)</span>
          </button>
          <button class="dropdown-item" id="btn-opt-dryrun">
            <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M10 2v7.31L4.69 19.3A2 2 0 0 0 6.34 22h11.32a2 2 0 0 0 1.65-2.7L14 9.31V2"/></svg>
            <span>Dry Run (Preview Only)</span>
          </button>
        </div>
      </div>
    </div>
  </header>

  <!-- Hero KPI Stats Ribbon -->
  <div class="stats-ribbon">
    <div class="stat-item">
      <div class="stat-icon">
        <svg class="icon" viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
      </div>
      <div class="stat-content">
        <span class="stat-label">Active Week</span>
        <span class="stat-val" id="stat-active-week">--</span>
      </div>
    </div>

    <div class="stat-item">
      <div class="stat-icon">
        <svg class="icon" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
      </div>
      <div class="stat-content">
        <span class="stat-label">Total Grabbed</span>
        <span class="stat-val" id="stat-total-torrents">--</span>
      </div>
    </div>

    <div class="stat-item">
      <div class="stat-icon">
        <svg class="icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
      </div>
      <div class="stat-content">
        <span class="stat-label">Next Scheduled Run</span>
        <span class="stat-val" id="stat-next-run">--</span>
      </div>
    </div>

    <div class="stat-item">
      <div class="stat-icon">
        <svg class="icon" viewBox="0 0 24 24"><polyline points="1 4 1 10 7 10"/><polyline points="23 20 23 14 17 14"/><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4l-4.64 4.36A9 9 0 0 1 3.51 15"/></svg>
      </div>
      <div class="stat-content">
        <span class="stat-label">Last Cycle Check</span>
        <span class="stat-val" id="stat-last-run">--</span>
      </div>
    </div>
  </div>

  <!-- Persistent Navigation Toolbar -->
  <div class="action-toolbar">
    <div class="tabs-group">
      <button class="tab-btn active" data-tab="tab-matches">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
        <span>Grabbed Matches</span>
        <span class="tab-counter" id="tab-count-torrents">0</span>
      </button>
      <button class="tab-btn" data-tab="tab-browser">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
        <span>Active Week Games</span>
        <span class="tab-counter" id="tab-count-browser">0</span>
      </button>
      <button class="tab-btn" data-tab="tab-history">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
        <span>Run History</span>
        <span class="tab-counter" id="tab-count-runs">0</span>
      </button>
      <button class="tab-btn" data-tab="tab-settings">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
        <span>Settings</span>
      </button>
    </div>
  </div>

  <!-- Tab Content Area -->
  <div class="tab-content-area">

    <!-- TAB 1: Grabbed Matches -->
    <div class="tab-pane active" id="tab-matches">
      <div class="sub-bar">
        <div class="search-box">
          <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input type="text" id="matches-search" placeholder="Filter matches by team, week, or resolution...">
        </div>
        <div style="font-size: 0.78rem; color: var(--muted);" id="matches-summary">Loading grabbed games...</div>
        <button class="btn-sm" id="btn-refresh-matches">
          <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
          <span>Refresh List</span>
        </button>
      </div>

      <div class="table-container">
        <div class="table-scroll-container">
          <table class="data-table" id="matches-table">
            <thead>
              <tr>
                <th style="width: 60px;">#</th>
                <th style="width: 100px;">Week</th>
                <th style="width: 100px;">Rating</th>
                <th>Matchup / Release Title</th>
                <th style="width: 170px;">Grabbed At</th>
                <th style="width: 120px;">Source</th>
                <th style="width: 110px; text-align: center;">Action</th>
              </tr>
            </thead>
            <tbody id="matches-tbody">
              <tr><td colspan="7" style="text-align: center; color: var(--muted); padding: 2rem;">Loading grabbed matches...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 2: Active Week Match Browser -->
    <div class="tab-pane" id="tab-browser">
      <div class="sub-bar">
        <div class="search-box">
          <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input type="text" id="browser-search" placeholder="Search active week games...">
        </div>
        <div style="font-size: 0.78rem; color: var(--muted);" id="browser-summary">Loading active week games...</div>
        <button class="btn-sm" id="btn-refresh-browser">
          <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
          <span>Refresh from 720pier</span>
        </button>
      </div>

      <div class="table-container">
        <div class="table-scroll-container">
          <table class="data-table" id="browser-table">
            <thead>
              <tr>
                <th style="width: 60px;">#</th>
                <th style="width: 100px;">Rating</th>
                <th>Matchup / Release Title</th>
                <th style="width: 120px; text-align: center;">Favorite</th>
                <th style="width: 130px;">Topic Page</th>
                <th style="width: 170px; text-align: center;">Action</th>
              </tr>
            </thead>
            <tbody id="browser-tbody">
              <tr><td colspan="6" style="text-align: center; color: var(--muted); padding: 2rem;">Loading games for active week...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 3: Run History -->
    <div class="tab-pane" id="tab-history">
      <div class="sub-bar">
        <div style="font-size: 0.78rem; color: var(--muted);" id="history-summary">Recent execution cycles recorded in SQLite</div>
        <button class="btn-sm" id="btn-refresh-history">
          <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
          <span>Refresh History</span>
        </button>
      </div>

      <div class="table-container">
        <div class="table-scroll-container">
          <table class="data-table" id="history-table">
            <thead>
              <tr>
                <th style="width: 70px;">Run #</th>
                <th style="width: 130px;">Status</th>
                <th style="width: 110px;">Week</th>
                <th style="width: 120px;">Torrents Added</th>
                <th style="width: 170px;">Started At</th>
                <th style="width: 170px;">Completed At</th>
                <th style="width: 100px;">Duration</th>
                <th>Error Details</th>
              </tr>
            </thead>
            <tbody id="history-tbody">
              <tr><td colspan="8" style="text-align: center; color: var(--muted); padding: 2rem;">Loading run history...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 4: Settings Management -->
    <div class="tab-pane" id="tab-settings">
      <div class="sub-bar">
        <div>
          <span style="font-size: 0.82rem; font-weight: 700; color: #fff;">Application Configuration</span>
          <span style="font-size: 0.75rem; color: var(--muted); margin-left: 0.5rem;">Saved to disk (.env) and hot-reloaded immediately</span>
        </div>
        <button class="btn-sm" id="btn-reload-settings">
          <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="1 4 1 10 7 10"/><polyline points="23 20 23 14 17 14"/><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4l-4.64 4.36A9 9 0 0 1 3.51 15"/></svg>
          <span>Reset Form</span>
        </button>
      </div>

      <div class="settings-container">
        <form id="settings-form" onsubmit="event.preventDefault(); saveSettings();">

          <!-- Section 1: Match & Grabber Preferences -->
          <div class="settings-section">
            <div class="settings-section-title">
              <svg class="icon" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
              <span>Match &amp; Grabber Preferences</span>
            </div>
            <div class="settings-grid">
              <div class="form-group" style="grid-column: span 2;">
                <label>Favorite Teams (comma-separated)</label>
                <input type="text" id="cfg-FAVORITE_TEAMS" placeholder="Kansas City Chiefs, KC, Chiefs">
                <small>Matches involving these teams are always grabbed even if outside top rated limit</small>
                <div class="team-presets">
                  <span style="font-size: 0.7rem; color: var(--muted); align-self: center;">Quick Add:</span>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('Kansas City Chiefs')">+ Chiefs</button>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('San Francisco 49ers')">+ 49ers</button>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('Detroit Lions')">+ Lions</button>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('Baltimore Ravens')">+ Ravens</button>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('Buffalo Bills')">+ Bills</button>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('Philadelphia Eagles')">+ Eagles</button>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('Green Bay Packers')">+ Packers</button>
                  <button type="button" class="team-chip" onclick="toggleTeamPreset('Dallas Cowboys')">+ Cowboys</button>
                </div>
              </div>
              <div class="form-group">
                <label>Top Matches Limit</label>
                <input type="number" id="cfg-TOP_MATCHES_LIMIT" min="1" max="10" placeholder="1">
                <small>Number of highest-rated matches to grab per week (1 = top match only)</small>
              </div>
              <div class="form-group">
                <label>Quality Preference</label>
                <select id="cfg-QUALITY_PREFERENCE">
                  <option value="">Any (first available)</option>
                  <option value="1080p">1080p</option>
                  <option value="720p">720p</option>
                </select>
                <small>Filter torrents by resolution when multiple releases exist</small>
              </div>
              <div class="form-group" style="grid-column: span 2;">
                <div class="form-switch-row">
                  <div class="form-switch-info">
                    <span>Standardize Torrent File Names</span>
                    <small>Formats downloaded files as "Week 08 - Team A at Team B (2026)" in qBittorrent</small>
                  </div>
                  <label class="switch-label">
                    <input type="checkbox" id="cfg-RENAME_TORRENT_FILES">
                    <span class="switch-slider"></span>
                  </label>
                </div>
                <div class="form-switch-row">
                  <div class="form-switch-info">
                    <span>Auto-start Downloads in qBittorrent</span>
                    <small>Starts torrent immediately upon queuing instead of adding in paused state</small>
                  </div>
                  <label class="switch-label">
                    <input type="checkbox" id="cfg-QBIT_AUTO_START">
                    <span class="switch-slider"></span>
                  </label>
                </div>
              </div>
            </div>
          </div>

          <!-- Section 2: Service Schedule & Timing -->
          <div class="settings-section">
            <div class="settings-section-title">
              <svg class="icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
              <span>Service Schedule &amp; Timing</span>
            </div>
            <div class="settings-grid">
              <div class="form-group" style="grid-column: span 2;">
                <label>Schedule Days</label>
                <input type="text" id="cfg-SCHEDULE_DAYS" placeholder="Mon,Tue,Wed,Thu,Fri,Sat,Sun">
                <div class="day-chips" id="schedule-day-chips">
                  <span class="day-chip" data-day="Mon" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Mon', this)">Mon</span>
                  <span class="day-chip" data-day="Tue" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Tue', this)">Tue</span>
                  <span class="day-chip" data-day="Wed" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Wed', this)">Wed</span>
                  <span class="day-chip" data-day="Thu" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Thu', this)">Thu</span>
                  <span class="day-chip" data-day="Fri" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Fri', this)">Fri</span>
                  <span class="day-chip" data-day="Sat" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Sat', this)">Sat</span>
                  <span class="day-chip" data-day="Sun" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Sun', this)">Sun</span>
                </div>
              </div>
              <div class="form-group">
                <label>Schedule Times (24h or 12h)</label>
                <input type="text" id="cfg-SCHEDULE_TIMES" placeholder="09:00,15:00,21:00">
                <small>Specific times of day to trigger grabber checks (comma-separated)</small>
              </div>
              <div class="form-group">
                <label>Top Matches Days</label>
                <input type="text" id="cfg-TOP_MATCHES_DAYS" placeholder="Mon,Tue,Wed">
                <div class="day-chips" id="top-day-chips">
                  <span class="day-chip" data-day="Mon" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Mon', this)">Mon</span>
                  <span class="day-chip" data-day="Tue" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Tue', this)">Tue</span>
                  <span class="day-chip" data-day="Wed" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Wed', this)">Wed</span>
                  <span class="day-chip" data-day="Thu" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Thu', this)">Thu</span>
                  <span class="day-chip" data-day="Fri" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Fri', this)">Fri</span>
                  <span class="day-chip" data-day="Sat" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Sat', this)">Sat</span>
                  <span class="day-chip" data-day="Sun" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Sun', this)">Sun</span>
                </div>
              </div>
              <div class="form-group">
                <label>Fallback Interval Hours</label>
                <input type="number" id="cfg-CHECK_INTERVAL_HOURS" step="0.5" min="0.5" placeholder="6">
                <small>Interval between checks when Schedule Times is blank</small>
              </div>
            </div>
          </div>

          <!-- Section 3: 720pier Connection -->
          <div class="settings-section">
            <div class="settings-section-title">
              <svg class="icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>
              <span>720pier Tracker Connection</span>
            </div>
            <div class="settings-grid">
              <div class="form-group" style="grid-column: span 2;">
                <label>Thread URL</label>
                <input type="text" id="cfg-PIER_THREAD_URL" placeholder="https://720pier.ru/viewtopic.php?t=76272">
              </div>
              <div class="form-group">
                <label>Username</label>
                <input type="text" id="cfg-PIER_USERNAME" placeholder="720pier forum username">
              </div>
              <div class="form-group">
                <label>Password</label>
                <div class="input-with-icon">
                  <input type="password" id="cfg-PIER_PASSWORD" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-toggle-pwd" onclick="togglePasswordVisibility('cfg-PIER_PASSWORD')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>
              <div class="form-group" style="grid-column: span 2;">
                <label>Session Cookie (PIER_COOKIE)</label>
                <textarea id="cfg-PIER_COOKIE" rows="2" placeholder="Leave unchanged to keep existing or paste updated browser cookie"></textarea>
                <small>Used to authenticate if Cloudflare or CAPTCHA challenge is active</small>
              </div>
              <div class="form-group" style="grid-column: span 2;">
                <label>Proxy URL (Optional)</label>
                <input type="text" id="cfg-PROXY_URL" placeholder="socks5://127.0.0.1:1080 or http://proxy:8080">
              </div>
            </div>
          </div>

          <!-- Section 4: qBittorrent Configuration -->
          <div class="settings-section">
            <div class="settings-section-title">
              <svg class="icon" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
              <span>qBittorrent Configuration</span>
            </div>
            <div class="settings-grid">
              <div class="form-group">
                <label>Host URL</label>
                <input type="text" id="cfg-QBIT_HOST" placeholder="http://qbittorrent:8087">
              </div>
              <div class="form-group">
                <label>API Key (qBittorrent v5.2+)</label>
                <div class="input-with-icon">
                  <input type="password" id="cfg-QBIT_API_KEY" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-toggle-pwd" onclick="togglePasswordVisibility('cfg-QBIT_API_KEY')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>
              <div class="form-group">
                <label>Username (Fallback)</label>
                <input type="text" id="cfg-QBIT_USERNAME" placeholder="admin">
              </div>
              <div class="form-group">
                <label>Password (Fallback)</label>
                <div class="input-with-icon">
                  <input type="password" id="cfg-QBIT_PASSWORD" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-toggle-pwd" onclick="togglePasswordVisibility('cfg-QBIT_PASSWORD')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>
              <div class="form-group">
                <label>Save Path</label>
                <input type="text" id="cfg-QBIT_SAVE_PATH" placeholder="/nfl">
              </div>
              <div class="form-group">
                <label>Category</label>
                <input type="text" id="cfg-QBIT_CATEGORY" placeholder="NFL">
              </div>
              <div class="form-group" style="grid-column: span 2;">
                <label>Tags (comma-separated)</label>
                <input type="text" id="cfg-QBIT_TAGS" placeholder="720pier,thuuz">
              </div>
            </div>
          </div>

          <!-- Section 5: Notifications -->
          <div class="settings-section">
            <div class="settings-section-title">
              <svg class="icon" viewBox="0 0 24 24"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
              <span>Notifications (Discord &amp; Telegram)</span>
            </div>
            <div class="settings-grid">
              <div class="form-group" style="grid-column: span 2;">
                <label>Discord Webhook URL</label>
                <div class="input-with-icon">
                  <input type="password" id="cfg-DISCORD_WEBHOOK_URL" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-toggle-pwd" onclick="togglePasswordVisibility('cfg-DISCORD_WEBHOOK_URL')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>
              <div class="form-group">
                <label>Telegram Bot Token</label>
                <div class="input-with-icon">
                  <input type="password" id="cfg-TELEGRAM_BOT_TOKEN" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-toggle-pwd" onclick="togglePasswordVisibility('cfg-TELEGRAM_BOT_TOKEN')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>
              <div class="form-group">
                <label>Telegram Chat ID</label>
                <input type="text" id="cfg-TELEGRAM_CHAT_ID" placeholder="e.g. 123456789">
              </div>
            </div>
          </div>

          <!-- Section 6: Security -->
          <div class="settings-section">
            <div class="settings-section-title">
              <svg class="icon" viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
              <span>Dashboard Security (HTTP Basic Auth)</span>
            </div>
            <div class="settings-grid">
              <div class="form-group">
                <label>Web Dashboard Username</label>
                <input type="text" id="cfg-WEB_USERNAME" placeholder="Leave blank to disable authentication">
                <small>Requires browser login prompt when accessing dashboard</small>
              </div>
              <div class="form-group">
                <label>Web Dashboard Password</label>
                <div class="input-with-icon">
                  <input type="password" id="cfg-WEB_PASSWORD" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-toggle-pwd" onclick="togglePasswordVisibility('cfg-WEB_PASSWORD')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>
            </div>
          </div>

          <!-- Save Button Bar -->
          <div style="display: flex; align-items: center; justify-content: flex-end; gap: 1rem; padding: 0.5rem 0 2.5rem;">
            <span id="settings-status" style="font-size: 0.8rem; color: var(--green);"></span>
            <button type="submit" class="btn-save" id="btn-save-settings">
              <svg class="icon" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
              <span>Save &amp; Apply Settings</span>
            </button>
          </div>
        </form>
      </div>
    </div>

  </div>

  <!-- Confirmation Modal -->
  <div class="modal-overlay" id="confirm-modal">
    <div class="modal-card">
      <div class="modal-header">
        <h3 id="confirm-modal-title">
          <svg class="icon" style="color: var(--red);" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
          <span>Forget Match from Memory?</span>
        </h3>
        <button class="modal-close" onclick="closeConfirmModal()">
          <svg class="icon" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
        </button>
      </div>
      <div class="modal-body" id="confirm-modal-body">
        Are you sure you want to forget this match?
      </div>
      <div class="modal-footer">
        <button class="btn-sm" onclick="closeConfirmModal()">Cancel</button>
        <button class="btn-reset" id="confirm-modal-btn">Forget Match</button>
      </div>
    </div>
  </div>

  <!-- Toast Alerts Container -->
  <div id="toast-container"></div>

  <script>
    // State variables
    let matchesData = [];
    let browserData = [];
    let previousCycleRunning = false;
    let pendingConfirmAction = null;

    // Elements
    const runBtn = document.getElementById('btn-run-now');
    const runCaret = document.getElementById('btn-run-caret');
    const runDropdown = document.getElementById('run-dropdown-menu');
    const toastContainer = document.getElementById('toast-container');

    function escapeHtml(str) {
      if (!str) return '';
      return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function showToast(message, type = 'info', duration = 4000) {
      const toast = document.createElement('div');
      toast.className = `toast toast-${type}`;
      let iconSvg = '';
      if (type === 'success') {
        iconSvg = '<svg class="icon icon-sm" style="color: var(--green);" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg>';
      } else if (type === 'error') {
        iconSvg = '<svg class="icon icon-sm" style="color: var(--red);" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>';
      } else if (type === 'warning') {
        iconSvg = '<svg class="icon icon-sm" style="color: var(--orange);" viewBox="0 0 24 24"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>';
      } else {
        iconSvg = '<svg class="icon icon-sm" style="color: var(--cyan);" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';
      }
      toast.innerHTML = `${iconSvg}<div style="flex:1;">${escapeHtml(message)}</div>`;
      toastContainer.appendChild(toast);
      setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        toast.style.transition = 'all 0.3s ease-out';
        setTimeout(() => toast.remove(), 300);
      }, duration);
    }

    // Modal Helpers
    function openConfirmModal(title, message, onConfirm) {
      document.getElementById('confirm-modal-body').textContent = message;
      pendingConfirmAction = onConfirm;
      document.getElementById('confirm-modal').classList.add('show');
    }

    function closeConfirmModal() {
      document.getElementById('confirm-modal').classList.remove('show');
      pendingConfirmAction = null;
    }

    document.getElementById('confirm-modal-btn').addEventListener('click', () => {
      if (pendingConfirmAction) pendingConfirmAction();
      closeConfirmModal();
    });

    // Tab Switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));

        btn.classList.add('active');
        const targetId = btn.dataset.tab;
        const targetPane = document.getElementById(targetId);
        if (targetPane) targetPane.classList.add('active');

        if (targetId === 'tab-matches') fetchMatches();
        if (targetId === 'tab-browser') fetchBrowserMatches();
        if (targetId === 'tab-history') fetchHistory();
        if (targetId === 'tab-settings') loadSettings();
      });
    });

    // Keyboard Shortcuts
    document.addEventListener('keydown', (e) => {
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
      if (e.key === '1') document.querySelector('.tab-btn[data-tab="tab-matches"]')?.click();
      if (e.key === '2') document.querySelector('.tab-btn[data-tab="tab-browser"]')?.click();
      if (e.key === '3') document.querySelector('.tab-btn[data-tab="tab-history"]')?.click();
      if (e.key === '4') document.querySelector('.tab-btn[data-tab="tab-settings"]')?.click();
      if (e.key === 'r') {
        fetchStatus();
        fetchMatches();
        fetchBrowserMatches();
        fetchHistory();
      }
      if (e.key === '/') {
        e.preventDefault();
        const activeTab = document.querySelector('.tab-pane.active')?.id;
        if (activeTab === 'tab-matches') document.getElementById('matches-search')?.focus();
        if (activeTab === 'tab-browser') document.getElementById('browser-search')?.focus();
      }
    });

    // Execution Dropdown & Triggers
    runCaret.addEventListener('click', (e) => {
      e.stopPropagation();
      runDropdown.classList.toggle('show');
    });

    document.addEventListener('click', () => {
      runDropdown.classList.remove('show');
    });

    async function triggerCycle(dryRun = false) {
      runDropdown.classList.remove('show');
      runBtn.disabled = true;
      document.getElementById('run-btn-text').textContent = dryRun ? 'Starting Dry Run...' : 'Starting Cycle...';

      try {
        const res = await fetch(`/api/trigger?dry_run=${dryRun ? '1' : '0'}`, { method: 'POST' });
        const data = await res.json();
        if (res.status === 202) {
          showToast(data.message || 'Cycle successfully started in background!', 'success');
        } else if (res.status === 409) {
          showToast(data.error || 'A grabber cycle is already in progress.', 'warning');
        } else {
          showToast(data.error || 'Failed to trigger cycle.', 'error');
        }
      } catch (err) {
        showToast('Network error triggering grabber cycle: ' + err.message, 'error');
      } finally {
        setTimeout(() => {
          fetchStatus();
          fetchMatches();
          fetchHistory();
        }, 600);
      }
    }

    runBtn.addEventListener('click', () => triggerCycle(false));
    document.getElementById('btn-opt-live').addEventListener('click', () => triggerCycle(false));
    document.getElementById('btn-opt-dryrun').addEventListener('click', () => triggerCycle(true));

    // Diagnostics
    document.getElementById('btn-diag-qbit').addEventListener('click', async () => {
      const btn = document.getElementById('btn-diag-qbit');
      btn.disabled = true;
      btn.innerHTML = '<span>Testing...</span>';
      try {
        const res = await fetch('/api/test/qbit', { method: 'POST' });
        const data = await res.json();
        if (data.ok) {
          showToast(`qBittorrent Connected! Version: ${data.version} (API: ${data.web_api_version})`, 'success', 5000);
        } else {
          showToast(`qBittorrent Test Failed: ${data.error}`, 'error', 6000);
        }
      } catch (e) {
        showToast('qBittorrent connection request failed: ' + e.message, 'error');
      } finally {
        btn.disabled = false;
        btn.innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg><span>Test qBit</span>';
      }
    });

    document.getElementById('btn-diag-pier').addEventListener('click', async () => {
      const btn = document.getElementById('btn-diag-pier');
      btn.disabled = true;
      btn.innerHTML = '<span>Testing...</span>';
      try {
        const res = await fetch('/api/test/pier', { method: 'POST' });
        const data = await res.json();
        if (data.ok) {
          showToast(`720pier Session Active! Detected: ${data.detected_week}`, 'success', 5000);
        } else {
          showToast(`720pier Session Error: ${data.error}`, 'error', 6000);
        }
      } catch (e) {
        showToast('720pier test request failed: ' + e.message, 'error');
      } finally {
        btn.disabled = false;
        btn.innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg><span>Test 720pier</span>';
      }
    });

    document.getElementById('btn-diag-alert').addEventListener('click', async () => {
      const btn = document.getElementById('btn-diag-alert');
      btn.disabled = true;
      btn.innerHTML = '<span>Sending...</span>';
      try {
        const res = await fetch('/api/test/alert', { method: 'POST' });
        const data = await res.json();
        if (data.ok) {
          const discord = data.results.discord === true ? 'Discord: Sent' : data.results.discord;
          const telegram = data.results.telegram === true ? 'Telegram: Sent' : data.results.telegram;
          showToast(`Test Alert Dispatched! (${discord}, ${telegram})`, 'success', 5000);
        } else {
          showToast(`Alert Test Failed: ${data.error}`, 'error', 6000);
        }
      } catch (e) {
        showToast('Alert test request failed: ' + e.message, 'error');
      } finally {
        btn.disabled = false;
        btn.innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg><span>Send Alert</span>';
      }
    });

    // Helper to parse title details
    function parseReleaseTitle(rawTitle) {
      if (!rawTitle) return { matchup: 'Unknown Matchup', resolution: '', format: '', network: '' };
      let matchup = rawTitle;
      let resolution = '';
      let format = '';
      let network = '';

      // Matchup extracted from standard NFL / Week / Date / Matchup [extra]
      const parts = rawTitle.split('/');
      if (parts.length >= 4) {
        const potential = parts[3].trim();
        const bracketIdx = potential.indexOf('[');
        if (bracketIdx !== -1) {
          matchup = potential.substring(0, bracketIdx).trim();
        } else {
          matchup = potential;
        }
      }

      if (/1080p/i.test(rawTitle)) resolution = '1080p';
      else if (/720p/i.test(rawTitle)) resolution = '720p';

      if (/60fps/i.test(rawTitle)) format = '60fps';

      const netMatch = rawTitle.match(/EN\/(FOX|CBS|ESPN|NBC|ABC|AMZN|NFLN|PEACOCK)/i);
      if (netMatch) network = netMatch[1].toUpperCase();

      return { matchup, resolution, format, network };
    }

    // Grabbed Matches Management
    async function fetchMatches() {
      const tbody = document.getElementById('matches-tbody');
      try {
        const res = await fetch('/api/torrents');
        const data = await res.json();
        matchesData = data.torrents || [];
        renderMatches();
      } catch (err) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--red); padding: 2rem;">Error loading matches: ${escapeHtml(err.message)}</td></tr>`;
      }
    }

    function renderMatches() {
      const tbody = document.getElementById('matches-tbody');
      const query = (document.getElementById('matches-search').value || '').toLowerCase().trim();
      const filtered = matchesData.filter(m => {
        const text = `${m.title} ${m.week} ${m.rating || ''}`.toLowerCase();
        return !query || text.includes(query);
      });

      document.getElementById('matches-summary').textContent = `Showing ${filtered.length} of ${matchesData.length} match(es)`;
      document.getElementById('tab-count-torrents').textContent = matchesData.length;

      if (filtered.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--muted); padding: 3rem 1rem;">
          <svg class="icon icon-lg" style="margin-bottom: 0.5rem; opacity: 0.4;" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
          <div>No grabbed matches match your search criteria.</div>
        </td></tr>`;
        return;
      }

      tbody.innerHTML = filtered.map((m) => {
        const ratingVal = parseInt(m.rating, 10);
        let tagClass = 'tag-high';
        if (isNaN(ratingVal)) tagClass = 'tag-med';
        else if (ratingVal < 90) tagClass = 'tag-med';

        const isFav = (m.title || '').includes('Chiefs') || (m.title || '').includes('KC');
        const favBadge = isFav
          ? '<span class="tag-badge tag-fav" style="margin-left: 4px;"><svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" fill="currentColor"/></svg> KC</span>' : '';

        const ratingBadge = m.rating
          ? `<span class="tag-badge ${tagClass}">${escapeHtml(m.rating)} pts</span>${favBadge}`
          : '<span class="tag-badge tag-med">N/A</span>';

        const parsed = parseReleaseTitle(m.title);

        let pillsHtml = '';
        if (parsed.resolution) pillsHtml += `<span class="meta-pill">${escapeHtml(parsed.resolution)}</span>`;
        if (parsed.format) pillsHtml += `<span class="meta-pill">${escapeHtml(parsed.format)}</span>`;
        if (parsed.network) pillsHtml += `<span class="meta-pill">${escapeHtml(parsed.network)}</span>`;

        return `<tr>
          <td style="color: var(--muted); font-family: 'Fira Code', monospace; font-size: 0.75rem;">#${m.id}</td>
          <td><span class="tag-badge tag-week">${escapeHtml(m.week)}</span></td>
          <td>${ratingBadge}</td>
          <td>
            <div class="match-card-title">${escapeHtml(parsed.matchup)}</div>
            <div class="match-meta-pills" style="margin-top: 0.25rem;">
              ${pillsHtml}
              <span style="font-size: 0.7rem; color: var(--muted); overflow: hidden; text-overflow: ellipsis; max-width: 450px; white-space: nowrap;">${escapeHtml(m.title)}</span>
            </div>
          </td>
          <td style="color: var(--muted); font-size: 0.75rem; font-family: 'Fira Code', monospace;">${escapeHtml(m.added_at ? m.added_at.replace('T', ' ').substring(0, 19) : '--')}</td>
          <td>
            <a href="${escapeHtml(m.torrent_url)}" target="_blank" class="link-action">
              <span>Open Link</span>
              <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </a>
          </td>
          <td style="text-align: center;">
            <button class="btn-reset" onclick="confirmResetTorrent(${m.id}, '${escapeHtml(parsed.matchup).replace(/'/g, "\\'")}')">
              <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
              <span>Forget</span>
            </button>
          </td>
        </tr>`;
      }).join('');
    }

    function confirmResetTorrent(id, title) {
      openConfirmModal(
        'Forget Match Record?',
        `Are you sure you want to remove "${title}" from the grabber database? This will allow the grabber to re-evaluate and re-download it in future cycles.`,
        () => resetTorrent(id, title)
      );
    }

    async function resetTorrent(id, title) {
      try {
        const res = await fetch('/api/torrents/reset', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id: id })
        });
        const data = await res.json();
        if (data.ok) {
          showToast(`Match forgotten: "${title}"`, 'success');
          fetchMatches();
          fetchStatus();
        } else {
          showToast('Failed to reset match: ' + (data.error || 'Unknown error'), 'error');
        }
      } catch (err) {
        showToast('Network error resetting match: ' + err.message, 'error');
      }
    }

    document.getElementById('btn-refresh-matches').addEventListener('click', fetchMatches);
    document.getElementById('matches-search').addEventListener('input', renderMatches);

    // Run History Management
    async function fetchHistory() {
      const tbody = document.getElementById('history-tbody');
      try {
        const res = await fetch('/api/runs');
        const data = await res.json();
        const runs = data.runs || [];
        document.getElementById('tab-count-runs').textContent = runs.length;
        renderHistory(runs);
      } catch (err) {
        tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; color: var(--red); padding: 2rem;">Error loading run history: ${escapeHtml(err.message)}</td></tr>`;
      }
    }

    function renderHistory(runs) {
      const tbody = document.getElementById('history-tbody');
      if (runs.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; color: var(--muted); padding: 3rem 1rem;">
          <svg class="icon icon-lg" style="margin-bottom: 0.5rem; opacity: 0.4;" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          <div>No cycle executions recorded in history yet.</div>
        </td></tr>`;
        return;
      }

      tbody.innerHTML = runs.map(r => {
        let statusTag = 'tag-status-success';
        if (r.status === 'ERROR') statusTag = 'tag-status-error';
        else if (r.status === 'NO_TORRENTS') statusTag = 'tag-status-notorrents';
        else if (r.status === 'RUNNING') statusTag = 'tag-status-running';

        let durationStr = '--';
        if (r.started_at && r.completed_at) {
          const s = new Date(r.started_at).getTime();
          const e = new Date(r.completed_at).getTime();
          if (!isNaN(s) && !isNaN(e) && e >= s) {
            durationStr = `${Math.round((e - s) / 1000)}s`;
          }
        }

        const errText = r.error_message
          ? `<span style="color: var(--red); font-size: 0.75rem;">${escapeHtml(r.error_message)}</span>`
          : '<span style="color: var(--muted); font-size: 0.75rem;">None</span>';

        return `<tr>
          <td style="color: var(--muted); font-family: 'Fira Code', monospace; font-size: 0.75rem;">#${r.id}</td>
          <td><span class="tag-badge ${statusTag}">${escapeHtml(r.status)}</span></td>
          <td><strong style="color: var(--cyan);">${escapeHtml(r.week_title || '--')}</strong></td>
          <td><span style="font-weight: 700; color: ${r.torrents_added_count > 0 ? 'var(--green)' : 'var(--text-secondary)'}; font-family: 'Fira Code', monospace;">${r.torrents_added_count || 0}</span></td>
          <td style="color: var(--muted); font-size: 0.75rem; font-family: 'Fira Code', monospace;">${escapeHtml(r.started_at ? r.started_at.replace('T', ' ').substring(0, 19) : '--')}</td>
          <td style="color: var(--muted); font-size: 0.75rem; font-family: 'Fira Code', monospace;">${escapeHtml(r.completed_at ? r.completed_at.replace('T', ' ').substring(0, 19) : '--')}</td>
          <td style="font-family: 'Fira Code', monospace; font-size: 0.75rem;">${durationStr}</td>
          <td>${errText}</td>
        </tr>`;
      }).join('');
    }

    document.getElementById('btn-refresh-history').addEventListener('click', fetchHistory);

    // Active Week Match Browser Management
    async function fetchBrowserMatches(forceRefresh = false) {
      const tbody = document.getElementById('browser-tbody');
      const summary = document.getElementById('browser-summary');
      const refreshBtn = document.getElementById('btn-refresh-browser');
      if (forceRefresh) {
        refreshBtn.disabled = true;
        refreshBtn.innerHTML = '<span>Scraping 720pier...</span>';
      }
      summary.textContent = forceRefresh ? 'Scraping 720pier for active week matches...' : 'Loading active week matches...';

      try {
        const url = forceRefresh ? '/api/matches/refresh' : '/api/matches';
        const opts = forceRefresh ? { method: 'POST' } : { method: 'GET' };
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || 'Failed to fetch matches');

        browserData = data.matches || [];
        document.getElementById('tab-count-browser').textContent = browserData.length;
        renderBrowserMatches(data.week || 'Active Week');
        if (forceRefresh) {
          showToast(`Refreshed ${browserData.length} games for ${data.week || 'Active Week'}!`, 'success');
        }
      } catch (err) {
        summary.textContent = 'Error fetching active week games.';
        tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: var(--red); padding: 2rem;">Error: ${escapeHtml(err.message)}</td></tr>`;
        if (forceRefresh) showToast('Failed to refresh matches: ' + err.message, 'error');
      } finally {
        refreshBtn.disabled = false;
        refreshBtn.innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg><span>Refresh from 720pier</span>';
      }
    }

    function renderBrowserMatches(weekTitle = 'Active Week') {
      const tbody = document.getElementById('browser-tbody');
      const query = (document.getElementById('browser-search').value || '').toLowerCase().trim();
      const filtered = browserData.filter(m => {
        const text = `${m.title} ${m.rating || ''}`.toLowerCase();
        return !query || text.includes(query);
      });

      document.getElementById('browser-summary').textContent = `${weekTitle} — Showing ${filtered.length} of ${browserData.length} match(es)`;

      if (filtered.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; color: var(--muted); padding: 3rem 1rem;">
          <svg class="icon icon-lg" style="margin-bottom: 0.5rem; opacity: 0.4;" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
          <div>No games match your search criteria.</div>
        </td></tr>`;
        return;
      }

      tbody.innerHTML = filtered.map((m, idx) => {
        const ratingVal = parseInt(m.rating, 10);
        let tagClass = 'tag-high';
        if (isNaN(ratingVal)) tagClass = 'tag-low';
        else if (ratingVal < 70) tagClass = 'tag-low';
        else if (ratingVal < 90) tagClass = 'tag-med';

        const favBadge = m.is_favorite
          ? '<span class="tag-badge tag-fav"><svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" fill="currentColor"/></svg> KC Chiefs</span>'
          : '<span style="color: var(--muted); font-size: 0.72rem;">--</span>';

        const ratingBadge = m.rating && m.rating !== 'N/A'
          ? `<span class="tag-badge ${tagClass}">${escapeHtml(m.rating)} pts</span>`
          : '<span class="tag-badge tag-low">N/A</span>';

        let actionCol = '';
        if (m.is_grabbed) {
          actionCol = '<span class="badge-success"><svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg> Grabbed</span>';
        } else {
          const safeUrl = encodeURIComponent(m.url);
          const safeTitle = encodeURIComponent(m.title);
          const safeRating = encodeURIComponent(m.rating || '');
          const safeWeek = encodeURIComponent(weekTitle);
          actionCol = `<button class="btn-grab" id="grab-btn-${idx}" onclick="manualDownloadMatch('${safeUrl}', '${safeTitle}', '${safeRating}', '${safeWeek}', 'grab-btn-${idx}')">
            <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
            <span>Download to qBit</span>
          </button>`;
        }

        return `<tr>
          <td style="color: var(--muted); font-family: 'Fira Code', monospace; font-size: 0.75rem;">#${idx + 1}</td>
          <td>${ratingBadge}</td>
          <td><strong style="font-weight: 600; color: #fff;">${escapeHtml(m.title)}</strong></td>
          <td style="text-align: center;">${favBadge}</td>
          <td>
            <a href="${escapeHtml(m.url)}" target="_blank" class="link-action">
              <span>View Topic</span>
              <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </a>
          </td>
          <td style="text-align: center;">${actionCol}</td>
        </tr>`;
      }).join('');
    }

    async function manualDownloadMatch(encUrl, encTitle, encRating, encWeek, btnId) {
      const url = decodeURIComponent(encUrl);
      const title = decodeURIComponent(encTitle);
      const rating = decodeURIComponent(encRating);
      const week = decodeURIComponent(encWeek);
      const btn = document.getElementById(btnId);

      if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span>Grabbing...</span>';
      }

      try {
        const res = await fetch('/api/matches/download', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url, title, rating, week })
        });
        const data = await res.json();
        if (data.ok) {
          showToast(data.message || `Successfully sent "${title}" to qBittorrent!`, 'success', 5000);
          if (btn) {
            btn.outerHTML = '<span class="badge-success"><svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg> Grabbed</span>';
          }
          const item = browserData.find(x => x.url === url || x.title === title);
          if (item) item.is_grabbed = true;
          fetchMatches();
          fetchStatus();
        } else {
          showToast(`Download failed: ${data.error}`, 'error', 6000);
          if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg><span>Download to qBit</span>';
          }
        }
      } catch (err) {
        showToast('Network error triggering download: ' + err.message, 'error');
        if (btn) {
          btn.disabled = false;
          btn.innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg><span>Download to qBit</span>';
        }
      }
    }

    document.getElementById('btn-refresh-browser').addEventListener('click', () => fetchBrowserMatches(true));
    document.getElementById('browser-search').addEventListener('input', () => renderBrowserMatches());

    // Settings Management & Day Chip Helpers
    function toggleDay(inputId, day, el) {
      const input = document.getElementById(inputId);
      let days = input.value.split(',').map(d => d.trim()).filter(Boolean);
      const idx = days.findIndex(d => d.toLowerCase() === day.toLowerCase());
      if (idx !== -1) {
        days.splice(idx, 1);
        el.classList.remove('active');
      } else {
        days.push(day);
        el.classList.add('active');
      }
      input.value = days.join(', ');
    }

    function syncDayChips(containerId, inputId) {
      const container = document.getElementById(containerId);
      const input = document.getElementById(inputId);
      if (!container || !input) return;
      const days = input.value.split(',').map(d => d.trim().toLowerCase()).filter(Boolean);
      container.querySelectorAll('.day-chip').forEach(chip => {
        const d = (chip.dataset.day || '').toLowerCase();
        chip.classList.toggle('active', days.includes(d));
      });
    }

    function toggleTeamPreset(teamName) {
      const input = document.getElementById('cfg-FAVORITE_TEAMS');
      let teams = input.value.split(',').map(t => t.trim()).filter(Boolean);
      const idx = teams.findIndex(t => t.toLowerCase() === teamName.toLowerCase());
      if (idx !== -1) {
        teams.splice(idx, 1);
      } else {
        teams.push(teamName);
      }
      input.value = teams.join(', ');
    }

    function togglePasswordVisibility(inputId) {
      const input = document.getElementById(inputId);
      if (!input) return;
      input.type = input.type === 'password' ? 'text' : 'password';
    }

    async function loadSettings() {
      const status = document.getElementById('settings-status');
      status.textContent = 'Loading settings...';
      try {
        const res = await fetch('/api/settings');
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || 'Failed to load settings');
        const s = data.settings || {};

        document.getElementById('cfg-FAVORITE_TEAMS').value = s.favorite_teams || '';
        document.getElementById('cfg-TOP_MATCHES_LIMIT').value = s.top_matches_limit ?? 1;
        document.getElementById('cfg-QUALITY_PREFERENCE').value = s.quality_preference || '';
        document.getElementById('cfg-RENAME_TORRENT_FILES').checked = !!s.rename_torrent_files;
        document.getElementById('cfg-QBIT_AUTO_START').checked = !!s.qbit_auto_start;

        document.getElementById('cfg-SCHEDULE_DAYS').value = s.schedule_days || '';
        document.getElementById('cfg-SCHEDULE_TIMES').value = s.schedule_times || '';
        document.getElementById('cfg-TOP_MATCHES_DAYS').value = s.top_matches_days || '';
        document.getElementById('cfg-CHECK_INTERVAL_HOURS').value = s.check_interval_hours ?? 6;

        syncDayChips('schedule-day-chips', 'cfg-SCHEDULE_DAYS');
        syncDayChips('top-day-chips', 'cfg-TOP_MATCHES_DAYS');

        document.getElementById('cfg-PIER_THREAD_URL').value = s.pier_thread_url || '';
        document.getElementById('cfg-PIER_USERNAME').value = s.pier_username || '';
        document.getElementById('cfg-PIER_PASSWORD').value = s.has_pier_password ? '********' : '';
        document.getElementById('cfg-PIER_COOKIE').value = s.has_pier_cookie ? '********' : '';
        document.getElementById('cfg-PROXY_URL').value = s.proxy_url || '';

        document.getElementById('cfg-QBIT_HOST').value = s.qbit_host || '';
        document.getElementById('cfg-QBIT_API_KEY').value = s.has_qbit_api_key ? '********' : '';
        document.getElementById('cfg-QBIT_USERNAME').value = s.qbit_username || '';
        document.getElementById('cfg-QBIT_PASSWORD').value = s.has_qbit_password ? '********' : '';
        document.getElementById('cfg-QBIT_SAVE_PATH').value = s.qbit_save_path || '/nfl';
        document.getElementById('cfg-QBIT_CATEGORY').value = s.qbit_category || 'NFL';
        document.getElementById('cfg-QBIT_TAGS').value = s.qbit_tags || '720pier,thuuz';

        document.getElementById('cfg-DISCORD_WEBHOOK_URL').value = s.has_discord_webhook ? '********' : '';
        document.getElementById('cfg-TELEGRAM_BOT_TOKEN').value = s.has_telegram_bot ? '********' : '';
        document.getElementById('cfg-TELEGRAM_CHAT_ID').value = s.telegram_chat_id || '';

        document.getElementById('cfg-WEB_USERNAME').value = s.web_username || '';
        document.getElementById('cfg-WEB_PASSWORD').value = s.has_web_auth ? '********' : '';

        status.style.color = 'var(--green)';
        status.textContent = 'Configuration loaded from .env';
        setTimeout(() => { status.textContent = ''; }, 3000);
      } catch (err) {
        status.style.color = 'var(--red)';
        status.textContent = 'Error loading configuration: ' + err.message;
        showToast('Error loading configuration: ' + err.message, 'error');
      }
    }

    async function saveSettings() {
      const status = document.getElementById('settings-status');
      const saveBtn = document.getElementById('btn-save-settings');
      saveBtn.disabled = true;
      saveBtn.innerHTML = '<span>Saving &amp; Reloading...</span>';
      status.style.color = 'var(--cyan)';
      status.textContent = 'Saving settings to disk...';

      const payload = {
        FAVORITE_TEAMS: document.getElementById('cfg-FAVORITE_TEAMS').value,
        TOP_MATCHES_LIMIT: document.getElementById('cfg-TOP_MATCHES_LIMIT').value,
        QUALITY_PREFERENCE: document.getElementById('cfg-QUALITY_PREFERENCE').value,
        RENAME_TORRENT_FILES: document.getElementById('cfg-RENAME_TORRENT_FILES').checked,
        QBIT_AUTO_START: document.getElementById('cfg-QBIT_AUTO_START').checked,

        SCHEDULE_DAYS: document.getElementById('cfg-SCHEDULE_DAYS').value,
        SCHEDULE_TIMES: document.getElementById('cfg-SCHEDULE_TIMES').value,
        TOP_MATCHES_DAYS: document.getElementById('cfg-TOP_MATCHES_DAYS').value,
        CHECK_INTERVAL_HOURS: document.getElementById('cfg-CHECK_INTERVAL_HOURS').value,

        PIER_THREAD_URL: document.getElementById('cfg-PIER_THREAD_URL').value,
        PIER_USERNAME: document.getElementById('cfg-PIER_USERNAME').value,
        PIER_PASSWORD: document.getElementById('cfg-PIER_PASSWORD').value,
        PIER_COOKIE: document.getElementById('cfg-PIER_COOKIE').value,
        PROXY_URL: document.getElementById('cfg-PROXY_URL').value,

        QBIT_HOST: document.getElementById('cfg-QBIT_HOST').value,
        QBIT_API_KEY: document.getElementById('cfg-QBIT_API_KEY').value,
        QBIT_USERNAME: document.getElementById('cfg-QBIT_USERNAME').value,
        QBIT_PASSWORD: document.getElementById('cfg-QBIT_PASSWORD').value,
        QBIT_SAVE_PATH: document.getElementById('cfg-QBIT_SAVE_PATH').value,
        QBIT_CATEGORY: document.getElementById('cfg-QBIT_CATEGORY').value,
        QBIT_TAGS: document.getElementById('cfg-QBIT_TAGS').value,

        DISCORD_WEBHOOK_URL: document.getElementById('cfg-DISCORD_WEBHOOK_URL').value,
        TELEGRAM_BOT_TOKEN: document.getElementById('cfg-TELEGRAM_BOT_TOKEN').value,
        TELEGRAM_CHAT_ID: document.getElementById('cfg-TELEGRAM_CHAT_ID').value,

        WEB_USERNAME: document.getElementById('cfg-WEB_USERNAME').value,
        WEB_PASSWORD: document.getElementById('cfg-WEB_PASSWORD').value,
      };

      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (data.ok) {
          showToast(data.message || 'Settings saved and applied successfully!', 'success', 5000);
          status.style.color = 'var(--green)';
          status.textContent = 'Settings saved and applied successfully!';
          loadSettings();
          fetchStatus();
        } else {
          showToast('Error saving settings: ' + (data.error || 'Unknown error'), 'error', 6000);
          status.style.color = 'var(--red)';
          status.textContent = 'Error: ' + data.error;
        }
      } catch (err) {
        showToast('Network error saving settings: ' + err.message, 'error');
        status.style.color = 'var(--red)';
        status.textContent = 'Network error: ' + err.message;
      } finally {
        saveBtn.disabled = false;
        saveBtn.innerHTML = '<svg class="icon" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg><span>Save &amp; Apply Settings</span>';
      }
    }

    document.getElementById('btn-reload-settings').addEventListener('click', loadSettings);

    // Status Polling
    async function fetchStatus() {
      try {
        const res = await fetch('/api/status');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();

        const statusText = (data.status || 'Running').toUpperCase();
        document.getElementById('badge-status-text').textContent = statusText;
        const badgeWrap = document.getElementById('badge-status-wrap');
        const isCycleRunning = data.is_cycle_running || statusText.includes('ACTIVE') || statusText.includes('RUNNING CYCLE');
        badgeWrap.classList.toggle('running', isCycleRunning);

        if (isCycleRunning) {
          runBtn.disabled = true;
          document.getElementById('run-btn-icon').innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>';
          document.getElementById('run-btn-text').textContent = 'Cycle Running...';
        } else {
          runBtn.disabled = false;
          document.getElementById('run-btn-icon').innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3" fill="currentColor"/></svg>';
          document.getElementById('run-btn-text').textContent = 'Run Check Now';
        }

        if (data.next_run) document.getElementById('stat-next-run').textContent = data.next_run;
        if (data.last_run) document.getElementById('stat-last-run').textContent = data.last_run;
        if (data.active_week) document.getElementById('stat-active-week').textContent = data.active_week;
        if (data.total_torrents !== undefined) {
          document.getElementById('stat-total-torrents').textContent = data.total_torrents;
          document.getElementById('tab-count-torrents').textContent = data.total_torrents;
        }

        if (previousCycleRunning && !isCycleRunning) {
          fetchMatches();
          fetchHistory();
        }
        previousCycleRunning = isCycleRunning;
      } catch (err) {
        // Silently keep current display on transient connection error
      }
    }

    // Initial load and periodic status polling (every 3s)
    fetchStatus();
    fetchMatches();
    fetchBrowserMatches();
    fetchHistory();
    setInterval(fetchStatus, 3000);
  </script>
</body>
</html>
"""


# --------------------------------------------------------------------------
# HTTP Request Handler & REST API
# --------------------------------------------------------------------------

class LogRequestHandler(http.server.BaseHTTPRequestHandler):
    """Handles HTTP requests for Dashboard UI, REST endpoints, and log exports."""

    def log_request(self, code="-", size="-"):
        pass

    def log_message(self, format, *args):
        # Silence default standard server request logging to avoid recursive log growth
        pass

    def check_auth(self) -> bool:
        """Verifies HTTP Basic Auth if web credentials are configured."""
        cfg = _service_context.get("config")
        if not cfg or not getattr(cfg, "web_username", None) or not getattr(cfg, "web_password", None):
            return True

        auth_header = self.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Basic "):
            self.send_auth_challenge()
            return False

        try:
            import base64
            encoded = auth_header.split(" ", 1)[1].strip()
            decoded = base64.b64decode(encoded).decode("utf-8")
            if ":" in decoded:
                user, pwd = decoded.split(":", 1)
                if user == cfg.web_username and pwd == cfg.web_password:
                    return True
        except Exception:
            pass

        self.send_auth_challenge()
        return False

    def send_auth_challenge(self):
        """Sends HTTP 401 challenge requesting Basic authentication."""
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="NFL Grabber Dashboard"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": False, "error": "Unauthorized"}).encode("utf-8"))

    def _send_json(self, status_code: int, data: Dict[str, Any]):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self.check_auth():
            return

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            body = HTML_TEMPLATE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif path == "/api/status":
            with _log_lock:
                status_copy = dict(_service_info)
                status_copy["is_cycle_running"] = _cycle_lock.locked()

            total_torrents = get_total_grabbed_torrents(status_copy.get("db_path"))

            payload = {
                "status": status_copy.get("status", "Running"),
                "is_cycle_running": status_copy.get("is_cycle_running", False),
                "started_at": status_copy.get("started_at"),
                "next_run": status_copy.get("next_run"),
                "last_run": status_copy.get("last_run"),
                "active_week": status_copy.get("active_week"),
                "total_torrents": total_torrents,
            }
            self._send_json(200, payload)

        elif path == "/api/matches":
            force_refresh = "refresh" in params and params["refresh"][0].lower() in ("1", "true", "yes")
            res = fetch_active_week_matches(force_refresh=force_refresh)
            self._send_json(200 if res.get("ok", True) else 500, res)

        elif path == "/api/settings":
            cfg = _service_context.get("config")
            if cfg:
                from src.config import get_masked_settings
                self._send_json(200, {"ok": True, "settings": get_masked_settings(cfg)})
            else:
                self._send_json(500, {"ok": False, "error": "Configuration not loaded"})

        elif path == "/api/logs":
            with _log_lock:
                status_copy = dict(_service_info)
                status_copy["is_cycle_running"] = _cycle_lock.locked()

            total_torrents = get_total_grabbed_torrents(status_copy.get("db_path"))

            payload = {
                "status": status_copy.get("status", "Running"),
                "is_cycle_running": status_copy.get("is_cycle_running", False),
                "started_at": status_copy.get("started_at"),
                "next_run": status_copy.get("next_run"),
                "last_run": status_copy.get("last_run"),
                "active_week": status_copy.get("active_week"),
                "total_torrents": total_torrents,
                "total_logs": 0,
                "logs": [],
            }
            self._send_json(200, payload)

        elif path == "/api/torrents":
            limit = 150
            if "limit" in params:
                try:
                    limit = int(params["limit"][0])
                except ValueError:
                    pass
            torrents = get_processed_torrents_data(limit=limit)
            self._send_json(200, {"ok": True, "count": len(torrents), "torrents": torrents})

        elif path == "/api/runs":
            limit = 50
            if "limit" in params:
                try:
                    limit = int(params["limit"][0])
                except ValueError:
                    pass
            runs = get_runs_data(limit=limit)
            self._send_json(200, {"ok": True, "count": len(runs), "runs": runs})

        elif path == "/logs":
            msg = "Logs are available via standard container output ('docker logs nfl-torrent-grabber').\n"
            encoded = msg.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"404 Not Found")

    def do_POST(self):
        if not self.check_auth():
            return

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        # Parse JSON body if present
        content_length = int(self.headers.get("Content-Length", 0))
        body_data = {}
        if content_length > 0:
            try:
                body_bytes = self.rfile.read(content_length)
                body_data = json.loads(body_bytes.decode("utf-8"))
            except Exception:
                body_data = {}

        if path == "/api/trigger":
            dry_run = False
            if "dry_run" in params and params["dry_run"][0].lower() in ("1", "true", "yes"):
                dry_run = True
            elif str(body_data.get("dry_run", "")).lower() in ("1", "true", "yes"):
                dry_run = True

            started, msg = trigger_cycle_async(dry_run=dry_run, favorite_only=False)
            if not started:
                self._send_json(409, {"ok": False, "error": msg})
            else:
                self._send_json(202, {"ok": True, "message": msg, "dry_run": dry_run})

        elif path == "/api/matches/refresh":
            res = fetch_active_week_matches(force_refresh=True)
            self._send_json(200 if res.get("ok", True) else 500, res)

        elif path == "/api/matches/download":
            url = body_data.get("url") or params.get("url", [""])[0]
            title = body_data.get("title") or params.get("title", [""])[0]
            rating = body_data.get("rating") or params.get("rating", [""])[0]
            week = body_data.get("week") or params.get("week", [""])[0]

            if not url or not title:
                self._send_json(400, {"ok": False, "error": "Missing required 'url' or 'title' parameter."})
                return

            success, msg = download_single_match(url, title, rating=rating, week=week)
            self._send_json(200 if success else 500, {"ok": success, "message": msg, "error": msg if not success else None})

        elif path == "/api/settings":
            from src.config import Config, update_env_file, get_masked_settings
            if not body_data:
                self._send_json(400, {"ok": False, "error": "No settings provided in request body."})
                return

            success = update_env_file(body_data)
            if success:
                new_cfg = Config.load()
                _service_context["config"] = new_cfg
                reloader = _service_context.get("schedule_reloader")
                if reloader:
                    try:
                        reloader()
                    except Exception as r_err:
                        logger.warning("Notice updating scheduler: %s", r_err)

                self._send_json(200, {
                    "ok": True,
                    "message": "Settings saved to .env and applied in-memory!",
                    "settings": get_masked_settings(new_cfg),
                })
            else:
                self._send_json(500, {"ok": False, "error": "Failed to update .env configuration file."})

        elif path == "/api/test/qbit":
            res = run_qbit_diagnostic()
            self._send_json(200, res)

        elif path == "/api/test/pier":
            res = run_pier_diagnostic()
            self._send_json(200, res)

        elif path == "/api/test/alert":
            res = run_alert_diagnostic()
            self._send_json(200, res)

        elif path == "/api/torrents/reset":
            ident = body_data.get("id") or body_data.get("torrent_url") or params.get("id", [""])[0] or params.get("torrent_url", [""])[0]
            if not ident:
                self._send_json(400, {"ok": False, "error": "Missing required 'id' or 'torrent_url' field."})
                return

            success = delete_processed_torrent_record(str(ident))
            if success:
                self._send_json(200, {"ok": True, "message": "Torrent record removed from history", "identifier": ident})
            else:
                self._send_json(404, {"ok": False, "error": "Torrent record not found in database"})

        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"404 Not Found")


def start_web_server(
    port: int = 5020,
    db_path: Optional[str] = None,
    log_file: Optional[str] = None,
    config: Any = None,
    state: Any = None,
    notifier: Any = None,
    cycle_runner: Optional[Callable[..., Any]] = None,
    schedule_logger: Optional[Callable[[], Any]] = None,
    schedule_reloader: Optional[Callable[[], Any]] = None,
):
    """
    Spins up the lightweight Web UI server in a daemon thread.
    Registers service coordinator callbacks for manual triggers, diagnostics, and data operations.
    """
    update_service_status(db_path=db_path)
    _service_context["config"] = config
    _service_context["state"] = state
    _service_context["notifier"] = notifier
    _service_context["cycle_runner"] = cycle_runner
    _service_context["schedule_logger"] = schedule_logger
    _service_context["schedule_reloader"] = schedule_reloader

    class ReusableThreadingServer(http.server.ThreadingHTTPServer):
        allow_reuse_address = True

    try:
        server = ReusableThreadingServer(("0.0.0.0", port), LogRequestHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True, name="NFLGrabberWebServer")
        t.start()
        logging.getLogger("NFLGrabber").info("Dashboard & Control Web UI listening at http://0.0.0.0:%d", port)
        return server
    except Exception as e:
        logging.getLogger("NFLGrabber").error("Failed to start web server on port %d: %s", port, e)
        return None
