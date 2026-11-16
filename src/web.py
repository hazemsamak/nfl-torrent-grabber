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
  <title>NFL Grabber &mdash; Automation &amp; Control Center</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;500;600&family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #080b12;
      --bg-surface: #0f1523;
      --bg-card: #141c2e;
      --bg-card-hover: #19233a;
      --bg-input: #0a0f1a;
      --border: rgba(255, 255, 255, 0.08);
      --border-card: rgba(255, 255, 255, 0.12);
      --border-hover: rgba(56, 189, 248, 0.4);
      --cyan: #38bdf8;
      --cyan-glow: rgba(56, 189, 248, 0.25);
      --indigo: #6366f1;
      --green: #10b981;
      --green-glow: rgba(16, 185, 129, 0.2);
      --amber: #f59e0b;
      --amber-glow: rgba(245, 158, 11, 0.2);
      --rose: #f43f5e;
      --purple: #a855f7;
      --text: #f8fafc;
      --text-secondary: #94a3b8;
      --muted: #64748b;
      --radius-sm: 6px;
      --radius-md: 10px;
      --radius-lg: 14px;
      --radius-xl: 18px;
      --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.4);
      --shadow-md: 0 4px 14px rgba(0, 0, 0, 0.4), 0 1px 3px rgba(0, 0, 0, 0.2);
      --shadow-lg: 0 12px 30px rgba(0, 0, 0, 0.55);
      --shadow-glow: 0 0 20px rgba(56, 189, 248, 0.15);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      min-height: 100%;
      background-color: var(--bg);
      color: var(--text);
      font-family: 'Inter', system-ui, -apple-system, sans-serif;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
    }

    body {
      display: flex;
      flex-direction: column;
      overflow-x: hidden;
      overflow-y: auto;
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

    /* Sticky Unified App Header */
    header.app-header {
      position: sticky;
      top: 0;
      z-index: 50;
      background: rgba(11, 15, 25, 0.88);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      border-bottom: 1px solid var(--border);
      padding: 0.65rem 1.5rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35);
    }

    .brand-section {
      display: flex;
      align-items: center;
      gap: 0.85rem;
    }

    .brand-emblem {
      width: 36px;
      height: 36px;
      border-radius: var(--radius-md);
      background: linear-gradient(135deg, #0284c7 0%, #4f46e5 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      color: #fff;
      box-shadow: 0 2px 10px rgba(2, 132, 199, 0.35);
    }

    .brand-meta {
      display: flex;
      flex-direction: column;
    }

    .brand-title {
      font-size: 1.1rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 0.45rem;
    }

    .brand-badge {
      font-size: 0.65rem;
      font-weight: 700;
      text-transform: uppercase;
      padding: 0.12rem 0.45rem;
      border-radius: 9999px;
      background: rgba(56, 189, 248, 0.15);
      border: 1px solid rgba(56, 189, 248, 0.3);
      color: var(--cyan);
      letter-spacing: 0.04em;
    }

    .badge-status {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: var(--green);
      font-size: 0.7rem;
      font-weight: 700;
      text-transform: uppercase;
      padding: 0.2rem 0.55rem;
      border-radius: 9999px;
      transition: all 0.25s ease;
      letter-spacing: 0.03em;
    }

    .badge-status.running {
      background: rgba(245, 158, 11, 0.15);
      border-color: rgba(245, 158, 11, 0.4);
      color: var(--amber);
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

    /* Navigation Segmented Tabs */
    .tabs-group {
      display: flex;
      gap: 0.3rem;
      background: rgba(15, 21, 35, 0.7);
      padding: 0.25rem;
      border-radius: var(--radius-md);
      border: 1px solid var(--border);
    }

    .tab-btn {
      background: transparent;
      border: none;
      color: var(--text-secondary);
      padding: 0.45rem 1rem;
      font-size: 0.82rem;
      font-weight: 600;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      transition: all 0.15s ease;
    }

    .tab-btn:hover {
      color: #fff;
      background: rgba(255, 255, 255, 0.05);
    }

    .tab-btn.active {
      background: var(--cyan);
      color: #080b12;
      font-weight: 700;
      box-shadow: 0 2px 10px var(--cyan-glow);
    }

    .tab-btn.active .tab-counter {
      background: rgba(8, 11, 18, 0.25);
      color: #080b12;
    }

    .tab-counter {
      background: var(--bg-card);
      color: var(--text-secondary);
      font-size: 0.7rem;
      font-family: 'Fira Code', monospace;
      padding: 0.1rem 0.45rem;
      border-radius: 9999px;
      font-weight: 600;
      transition: all 0.15s ease;
    }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 0.65rem;
    }

    .btn-header {
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

    .btn-header:hover {
      border-color: var(--border-card);
      color: #fff;
      background: var(--bg-card);
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
      box-shadow: 0 2px 10px rgba(2, 132, 199, 0.35);
    }

    .btn-main-trigger:hover:not(:disabled) {
      background: linear-gradient(135deg, #0369a1 0%, #0284c7 100%);
      box-shadow: 0 4px 14px rgba(2, 132, 199, 0.5);
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
      background: var(--bg-card);
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
      background: rgba(255, 255, 255, 0.08);
      color: #fff;
    }

    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(-4px); }
      to { opacity: 1; transform: translateY(0); }
    }

    /* Main Container */
    main.app-main {
      flex: 1;
      display: flex;
      flex-direction: column;
      padding: 1.25rem 1.5rem;
      max-width: 1400px;
      width: 100%;
      margin: 0 auto;
    }

    /* KPI Metrics Banner */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
      margin-bottom: 1.25rem;
    }

    .kpi-card {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
      padding: 0.85rem 1rem;
      display: flex;
      align-items: center;
      gap: 0.85rem;
      transition: all 0.2s ease;
      position: relative;
      overflow: hidden;
    }

    .kpi-card:hover {
      border-color: var(--border-card);
      transform: translateY(-2px);
      box-shadow: var(--shadow-md);
    }

    .kpi-icon {
      width: 38px;
      height: 38px;
      border-radius: var(--radius-sm);
      background: var(--bg-card);
      color: var(--cyan);
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }

    .kpi-details {
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .kpi-label {
      font-size: 0.72rem;
      font-weight: 600;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .kpi-val {
      font-size: 0.95rem;
      font-weight: 700;
      color: #fff;
      font-family: 'Fira Code', monospace;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      margin-top: 0.15rem;
    }

    /* Tab Content Panes */
    .tab-pane {
      display: none;
      flex-direction: column;
      flex: 1;
    }

    .tab-pane.active {
      display: flex;
    }

    /* Control Bar */
    .control-bar {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
      padding: 0.75rem 1rem;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 0.85rem;
      margin-bottom: 1.25rem;
    }

    .control-left {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      flex: 1;
      min-width: 280px;
    }

    .search-input-wrap {
      position: relative;
      display: flex;
      align-items: center;
      flex: 1;
      max-width: 380px;
    }

    .search-input-wrap .icon {
      position: absolute;
      left: 0.75rem;
      color: var(--muted);
      pointer-events: none;
    }

    .search-input-wrap input {
      width: 100%;
      background: var(--bg-input);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      color: #fff;
      padding: 0.45rem 0.75rem 0.45rem 2.2rem;
      font-size: 0.82rem;
      outline: none;
      transition: all 0.15s ease;
    }

    .search-input-wrap input:focus {
      border-color: var(--cyan);
      box-shadow: 0 0 0 2px var(--cyan-glow);
    }

    .filter-chips {
      display: flex;
      align-items: center;
      gap: 0.35rem;
      flex-wrap: wrap;
    }

    .filter-chip {
      background: var(--bg-card);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      font-size: 0.74rem;
      font-weight: 600;
      padding: 0.25rem 0.65rem;
      border-radius: 9999px;
      cursor: pointer;
      transition: all 0.15s ease;
      user-select: none;
    }

    .filter-chip:hover {
      color: #fff;
      border-color: var(--border-card);
    }

    .filter-chip.active {
      background: rgba(56, 189, 248, 0.18);
      border-color: var(--cyan);
      color: var(--cyan);
    }

    .control-right {
      display: flex;
      align-items: center;
      gap: 0.65rem;
    }

    .view-mode-toggle {
      display: flex;
      background: var(--bg-card);
      padding: 0.2rem;
      border-radius: var(--radius-sm);
      border: 1px solid var(--border);
    }

    .view-toggle-btn {
      background: transparent;
      border: none;
      color: var(--muted);
      padding: 0.3rem 0.55rem;
      border-radius: 4px;
      cursor: pointer;
      display: flex;
      align-items: center;
      transition: all 0.12s ease;
    }

    .view-toggle-btn.active {
      background: var(--bg-surface);
      color: var(--cyan);
      box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
    }

    /* Cards Grid View (Media Showcase) */
    .matches-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
      gap: 1.15rem;
      margin-bottom: 2rem;
    }

    .game-card {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      padding: 1.15rem;
      display: flex;
      flex-direction: column;
      gap: 0.85rem;
      position: relative;
      transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      box-shadow: var(--shadow-sm);
    }

    .game-card:hover {
      border-color: var(--border-hover);
      transform: translateY(-3px);
      box-shadow: var(--shadow-lg), var(--shadow-glow);
    }

    .game-card.is-favorite {
      border-color: rgba(245, 158, 11, 0.35);
      background: linear-gradient(180deg, rgba(245, 158, 11, 0.04) 0%, var(--bg-surface) 100%);
    }

    .card-top-bar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.5rem;
    }

    .week-pill {
      font-size: 0.72rem;
      font-weight: 700;
      padding: 0.2rem 0.6rem;
      border-radius: 9999px;
      background: rgba(99, 102, 241, 0.15);
      border: 1px solid rgba(99, 102, 241, 0.35);
      color: #a5b4fc;
      letter-spacing: 0.03em;
    }

    .score-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      font-size: 0.74rem;
      font-weight: 800;
      font-family: 'Fira Code', monospace;
      padding: 0.2rem 0.65rem;
      border-radius: 9999px;
    }

    .score-high {
      background: rgba(16, 185, 129, 0.15);
      border: 1px solid rgba(16, 185, 129, 0.4);
      color: #34d399;
    }

    .score-med {
      background: rgba(56, 189, 248, 0.15);
      border: 1px solid rgba(56, 189, 248, 0.4);
      color: #38bdf8;
    }

    .score-low {
      background: rgba(148, 163, 184, 0.12);
      border: 1px solid rgba(148, 163, 184, 0.25);
      color: #94a3b8;
    }

    /* Matchup Showcase */
    .matchup-box {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
      padding: 0.85rem 1rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
    }

    .team-item {
      display: flex;
      align-items: center;
      gap: 0.65rem;
      flex: 1;
      overflow: hidden;
    }

    .team-item.home {
      justify-content: flex-end;
      text-align: right;
    }

    .team-badge {
      width: 32px;
      height: 32px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 800;
      font-size: 0.72rem;
      color: #fff;
      flex-shrink: 0;
      box-shadow: 0 2px 6px rgba(0, 0, 0, 0.4);
      border: 1px solid rgba(255, 255, 255, 0.2);
    }

    .team-name {
      font-size: 0.86rem;
      font-weight: 700;
      color: #fff;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .versus-badge {
      font-size: 0.75rem;
      font-weight: 800;
      color: var(--muted);
      text-transform: uppercase;
      padding: 0.2rem 0.4rem;
      border-radius: 4px;
      background: rgba(0, 0, 0, 0.25);
      flex-shrink: 0;
    }

    /* Release Specs & Metadata */
    .card-meta-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 0.35rem;
    }

    .meta-tag {
      font-size: 0.68rem;
      font-family: 'Fira Code', monospace;
      font-weight: 600;
      padding: 0.12rem 0.5rem;
      border-radius: 4px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      color: var(--text-secondary);
    }

    .meta-tag.res-1080p {
      border-color: rgba(56, 189, 248, 0.35);
      color: #38bdf8;
      background: rgba(56, 189, 248, 0.1);
    }

    .meta-tag.res-720p {
      border-color: rgba(99, 102, 241, 0.35);
      color: #a5b4fc;
      background: rgba(99, 102, 241, 0.1);
    }

    .meta-tag.network {
      border-color: rgba(245, 158, 11, 0.35);
      color: #fbbf24;
      background: rgba(245, 158, 11, 0.1);
    }

    .card-footer {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
      margin-top: auto;
      padding-top: 0.65rem;
      border-top: 1px solid var(--border);
    }

    .added-date {
      font-size: 0.72rem;
      color: var(--muted);
      font-family: 'Fira Code', monospace;
    }

    .card-action-group {
      display: flex;
      align-items: center;
      gap: 0.45rem;
    }

    /* Compact Table View */
    .table-view-container {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      overflow: hidden;
      box-shadow: var(--shadow-sm);
      margin-bottom: 2rem;
    }

    table.data-table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 0.82rem;
    }

    table.data-table th {
      background: var(--bg-card);
      color: var(--text-secondary);
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 0.75rem 1.25rem;
      border-bottom: 1px solid var(--border-card);
    }

    table.data-table td {
      padding: 0.85rem 1.25rem;
      border-bottom: 1px solid var(--border);
      color: var(--text);
      vertical-align: middle;
      transition: background-color 0.1s ease;
    }

    table.data-table tr:hover td {
      background-color: rgba(255, 255, 255, 0.025);
    }

    /* Buttons */
    .btn-action {
      background: var(--bg-card);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      padding: 0.35rem 0.75rem;
      font-size: 0.74rem;
      font-weight: 600;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      text-decoration: none;
      transition: all 0.15s ease;
    }

    .btn-action:hover {
      border-color: var(--border-card);
      color: #fff;
      background: var(--bg-card-hover);
    }

    .btn-forget {
      background: rgba(244, 63, 94, 0.12);
      border: 1px solid rgba(244, 63, 94, 0.3);
      color: #fb7185;
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

    .btn-forget:hover {
      background: rgba(244, 63, 94, 0.25);
      border-color: #fb7185;
      color: #fff;
    }

    .btn-grab {
      background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%);
      color: #fff;
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 0.4rem 0.85rem;
      font-size: 0.76rem;
      font-weight: 700;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      transition: all 0.15s ease;
      box-shadow: 0 2px 8px rgba(2, 132, 199, 0.3);
    }

    .btn-grab:hover:not(:disabled) {
      background: linear-gradient(135deg, #0369a1 0%, #0284c7 100%);
      box-shadow: 0 4px 12px var(--cyan-glow);
    }

    .btn-grab:disabled {
      opacity: 0.65;
      cursor: not-allowed;
    }

    .badge-grabbed {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.35);
      color: #34d399;
      font-size: 0.74rem;
      font-weight: 700;
      padding: 0.35rem 0.75rem;
      border-radius: var(--radius-sm);
    }

    /* Settings Tab Style */
    .settings-wrapper {
      max-width: 1050px;
      margin: 0 auto;
      width: 100%;
      display: flex;
      flex-direction: column;
      gap: 1.25rem;
      margin-bottom: 3rem;
    }

    .settings-panel {
      background: var(--bg-surface);
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      padding: 1.5rem;
      box-shadow: var(--shadow-sm);
    }

    .panel-header {
      display: flex;
      align-items: center;
      gap: 0.65rem;
      padding-bottom: 0.85rem;
      border-bottom: 1px solid var(--border);
      margin-bottom: 1.25rem;
    }

    .panel-header h3 {
      font-size: 1rem;
      font-weight: 700;
      color: #fff;
    }

    .panel-header .icon {
      color: var(--cyan);
    }

    .form-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 1.25rem;
    }

    @media (max-width: 768px) {
      .form-grid { grid-template-columns: 1fr; }
    }

    .form-field {
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
    }

    .form-field label {
      font-size: 0.78rem;
      font-weight: 600;
      color: var(--text-secondary);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .form-field input[type="text"],
    .form-field input[type="number"],
    .form-field input[type="password"],
    .form-field select,
    .form-field textarea {
      background: var(--bg-input);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      color: #fff;
      padding: 0.55rem 0.85rem;
      font-size: 0.84rem;
      outline: none;
      transition: all 0.15s ease;
      font-family: inherit;
    }

    .form-field textarea {
      font-family: 'Fira Code', monospace;
      font-size: 0.78rem;
      resize: vertical;
    }

    .form-field input:focus,
    .form-field select:focus,
    .form-field textarea:focus {
      border-color: var(--cyan);
      box-shadow: 0 0 0 2px var(--cyan-glow);
    }

    .form-field small {
      font-size: 0.72rem;
      color: var(--muted);
      line-height: 1.4;
    }

    /* Toggle Switch Row */
    .switch-box {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.75rem 1rem;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
    }

    .switch-info {
      display: flex;
      flex-direction: column;
      gap: 0.2rem;
    }

    .switch-info span {
      font-size: 0.82rem;
      font-weight: 600;
      color: #fff;
    }

    .switch-info small {
      font-size: 0.72rem;
      color: var(--muted);
    }

    .switch-label {
      position: relative;
      display: inline-block;
      width: 44px;
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
      background-color: rgba(255, 255, 255, 0.15);
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
      transform: translateX(20px);
    }

    /* Day Chips */
    .day-chips-row {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      margin-top: 0.35rem;
    }

    .day-toggle-chip {
      background: var(--bg-card);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      font-size: 0.74rem;
      font-weight: 600;
      padding: 0.3rem 0.65rem;
      border-radius: var(--radius-sm);
      cursor: pointer;
      transition: all 0.15s ease;
      user-select: none;
    }

    .day-toggle-chip:hover {
      color: #fff;
      border-color: var(--border-card);
    }

    .day-toggle-chip.active {
      background: rgba(56, 189, 248, 0.18);
      border-color: var(--cyan);
      color: var(--cyan);
      font-weight: 700;
    }

    /* Team Preset Tags */
    .team-presets-row {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      margin-top: 0.45rem;
    }

    .team-preset-tag {
      font-size: 0.72rem;
      padding: 0.2rem 0.55rem;
      border-radius: 9999px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      color: var(--text-secondary);
      cursor: pointer;
      transition: all 0.15s ease;
    }

    .team-preset-tag:hover {
      border-color: var(--cyan);
      color: #fff;
    }

    .input-wrapper {
      position: relative;
      display: flex;
      align-items: center;
    }

    .input-wrapper input {
      width: 100%;
      padding-right: 2.2rem !important;
    }

    .btn-eye {
      position: absolute;
      right: 0.65rem;
      background: transparent;
      border: none;
      color: var(--muted);
      cursor: pointer;
      display: flex;
      align-items: center;
    }

    .btn-eye:hover { color: #fff; }

    /* Sticky Save Toolbar */
    .save-toolbar {
      position: sticky;
      bottom: 1.5rem;
      z-index: 40;
      background: rgba(15, 21, 35, 0.92);
      backdrop-filter: blur(12px);
      border: 1px solid var(--border-card);
      border-radius: var(--radius-lg);
      padding: 0.85rem 1.5rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      box-shadow: var(--shadow-lg);
      margin-top: 1.5rem;
    }

    .btn-save-primary {
      background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%);
      color: #fff;
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 0.6rem 1.5rem;
      font-size: 0.84rem;
      font-weight: 700;
      border-radius: var(--radius-sm);
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      box-shadow: 0 2px 10px rgba(2, 132, 199, 0.4);
      transition: all 0.15s ease;
    }

    .btn-save-primary:hover:not(:disabled) {
      background: linear-gradient(135deg, #0369a1 0%, #0284c7 100%);
      box-shadow: 0 4px 16px rgba(2, 132, 199, 0.6);
    }

    .btn-save-primary:disabled {
      opacity: 0.65;
      cursor: not-allowed;
    }

    /* Modals */
    .modal-overlay {
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0, 0, 0, 0.75);
      backdrop-filter: blur(8px);
      -webkit-backdrop-filter: blur(8px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 1000;
      padding: 1.5rem;
    }

    .modal-overlay.show {
      display: flex;
    }

    .modal-box {
      background: var(--bg-surface);
      border: 1px solid var(--border-card);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow-lg);
      max-width: 550px;
      width: 100%;
      overflow: hidden;
      animation: modalScale 0.18s cubic-bezier(0.16, 1, 0.3, 1);
    }

    @keyframes modalScale {
      from { opacity: 0; transform: scale(0.96); }
      to { opacity: 1; transform: scale(1); }
    }

    .modal-header {
      padding: 1.15rem 1.5rem;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .modal-header h3 {
      font-size: 1rem;
      font-weight: 700;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 0.55rem;
    }

    .modal-close-btn {
      background: transparent;
      border: none;
      color: var(--muted);
      cursor: pointer;
      display: flex;
      align-items: center;
      padding: 0.25rem;
    }

    .modal-close-btn:hover { color: #fff; }

    .modal-body {
      padding: 1.5rem;
      font-size: 0.86rem;
      color: var(--text-secondary);
      line-height: 1.55;
    }

    .modal-footer {
      padding: 0.85rem 1.5rem;
      background: var(--bg-card);
      border-top: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 0.75rem;
    }

    /* Diagnostics Cards inside Modal */
    .diag-card-list {
      display: flex;
      flex-direction: column;
      gap: 0.85rem;
    }

    .diag-item {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
      padding: 0.85rem 1rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
    }

    .diag-meta {
      display: flex;
      flex-direction: column;
      gap: 0.2rem;
    }

    .diag-name {
      font-size: 0.85rem;
      font-weight: 700;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 0.45rem;
    }

    .diag-desc {
      font-size: 0.72rem;
      color: var(--muted);
    }

    /* Toast Container */
    #toast-container {
      position: fixed;
      bottom: 1.5rem;
      right: 1.5rem;
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
      z-index: 2000;
      pointer-events: none;
    }

    .toast {
      background: #0f1523;
      border: 1px solid var(--border-card);
      border-radius: var(--radius-md);
      box-shadow: var(--shadow-lg);
      color: #fff;
      font-size: 0.82rem;
      padding: 0.85rem 1.15rem;
      display: flex;
      align-items: center;
      gap: 0.75rem;
      min-width: 300px;
      max-width: 450px;
      pointer-events: auto;
      animation: toastSlide 0.25s cubic-bezier(0.16, 1, 0.3, 1);
    }

    @keyframes toastSlide {
      from { opacity: 0; transform: translateX(50px); }
      to { opacity: 1; transform: translateX(0); }
    }

    .toast-success { border-left: 4px solid var(--green); }
    .toast-error { border-left: 4px solid var(--rose); }
    .toast-warning { border-left: 4px solid var(--amber); }
    .toast-info { border-left: 4px solid var(--cyan); }
  </style>
</head>
<body>

  <!-- Sticky Unified Header -->
  <header class="app-header">
    <div class="brand-section">
      <div class="brand-emblem">
        <svg class="icon icon-lg" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
      </div>
      <div class="brand-meta">
        <div class="brand-title">
          <span>NFL Grabber</span>
          <span class="brand-badge">PRO</span>
        </div>
      </div>
      <div class="badge-status" id="badge-status-wrap">
        <div class="badge-pulse"></div>
        <span id="badge-status-text">INITIALIZING</span>
      </div>
    </div>

    <!-- Navigation Tabs -->
    <nav class="tabs-group">
      <button class="tab-btn active" data-tab="tab-matches">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
        <span>Grabbed Matches</span>
        <span class="tab-counter" id="tab-count-torrents">0</span>
      </button>
      <button class="tab-btn" data-tab="tab-browser">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
        <span>Active Week</span>
        <span class="tab-counter" id="tab-count-browser">0</span>
      </button>
      <button class="tab-btn" data-tab="tab-settings">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
        <span>Settings</span>
      </button>
    </nav>

    <!-- Header Actions -->
    <div class="header-actions">
      <!-- Diagnostics Modal Trigger -->
      <button class="btn-header" onclick="openDiagModal()" title="Run connectivity diagnostics">
        <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
        <span>Diagnostics</span>
      </button>

      <!-- Execution Split Button -->
      <div class="split-btn">
        <button class="btn-main-trigger" id="btn-run-now" title="Trigger immediate grabber check cycle">
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

  <!-- App Main Content Area -->
  <main class="app-main">

    <!-- KPI Metric Summary Tiles -->
    <section class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-icon">
          <svg class="icon" viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
        </div>
        <div class="kpi-details">
          <span class="kpi-label">Active Week Target</span>
          <span class="kpi-val" id="stat-active-week">--</span>
        </div>
      </div>

      <div class="kpi-card">
        <div class="kpi-icon" style="color: var(--green);">
          <svg class="icon" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        </div>
        <div class="kpi-details">
          <span class="kpi-label">Grabbed Torrents</span>
          <span class="kpi-val" id="stat-total-torrents">--</span>
        </div>
      </div>

      <div class="kpi-card">
        <div class="kpi-icon" style="color: var(--amber);">
          <svg class="icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
        </div>
        <div class="kpi-details">
          <span class="kpi-label">Next Scheduled Run</span>
          <span class="kpi-val" id="stat-next-run">--</span>
        </div>
      </div>

      <div class="kpi-card">
        <div class="kpi-icon" style="color: var(--purple);">
          <svg class="icon" viewBox="0 0 24 24"><polyline points="1 4 1 10 7 10"/><polyline points="23 20 23 14 17 14"/><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4l-4.64 4.36A9 9 0 0 1 3.51 15"/></svg>
        </div>
        <div class="kpi-details">
          <span class="kpi-label">Last Cycle Check</span>
          <span class="kpi-val" id="stat-last-run">--</span>
        </div>
      </div>
    </section>

    <!-- TAB 1: Grabbed Matches -->
    <section class="tab-pane active" id="tab-matches">
      <div class="control-bar">
        <div class="control-left">
          <div class="search-input-wrap">
            <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
            <input type="text" id="matches-search" placeholder="Filter games by team, week, or resolution...">
          </div>
          <div class="filter-chips" id="week-filter-chips"></div>
        </div>
        <div class="control-right">
          <span style="font-size: 0.78rem; color: var(--muted);" id="matches-summary">Loading games...</span>
          <div class="view-mode-toggle">
            <button class="view-toggle-btn active" id="btn-view-grid" onclick="setViewMode('grid')" title="Card Grid View">
              <svg class="icon icon-sm" viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>
            </button>
            <button class="view-toggle-btn" id="btn-view-table" onclick="setViewMode('table')" title="Compact Table View">
              <svg class="icon icon-sm" viewBox="0 0 24 24"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>
            </button>
          </div>
          <button class="btn-action" id="btn-refresh-matches">
            <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
            <span>Refresh</span>
          </button>
        </div>
      </div>

      <!-- Card Grid View Container -->
      <div class="matches-grid" id="matches-cards-container"></div>

      <!-- Table View Container -->
      <div class="table-view-container" id="matches-table-container" style="display: none;">
        <table class="data-table" id="matches-table">
          <thead>
            <tr>
              <th style="width: 50px;">#</th>
              <th style="width: 100px;">Week</th>
              <th style="width: 110px;">Rating</th>
              <th>Matchup / Release Details</th>
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
    </section>

    <!-- TAB 2: Active Week Games Browser -->
    <section class="tab-pane" id="tab-browser">
      <div class="control-bar">
        <div class="control-left">
          <div class="search-input-wrap">
            <svg class="icon icon-sm" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
            <input type="text" id="browser-search" placeholder="Search active week matchups...">
          </div>
        </div>
        <div class="control-right">
          <span style="font-size: 0.78rem; color: var(--muted);" id="browser-summary">Loading active week games...</span>
          <button class="btn-action" id="btn-refresh-browser">
            <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
            <span>Refresh from 720pier</span>
          </button>
        </div>
      </div>

      <!-- Browser Cards Grid -->
      <div class="matches-grid" id="browser-cards-container"></div>

      <!-- Hidden standard table for tests/compatibility -->
      <table class="data-table" id="browser-table" style="display: none;">
        <tbody id="browser-tbody"></tbody>
      </table>
    </section>

    <!-- TAB 3: Settings Management -->
    <section class="tab-pane" id="tab-settings">
      <div class="settings-wrapper">
        <form id="settings-form" onsubmit="event.preventDefault(); saveSettings();">

          <!-- Section 1: Match & Grabber Preferences -->
          <div class="settings-panel">
            <div class="panel-header">
              <svg class="icon" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
              <h3>Match &amp; Grabber Preferences</h3>
            </div>
            <div class="form-grid">
              <div class="form-field" style="grid-column: span 2;">
                <label>Favorite Teams (comma-separated)</label>
                <input type="text" id="cfg-FAVORITE_TEAMS" placeholder="Kansas City Chiefs, KC, Chiefs">
                <small>Matches involving favorite teams are always downloaded, even outside top rating limits</small>
                <div class="team-presets-row">
                  <span style="font-size: 0.72rem; color: var(--muted); align-self: center;">Quick Add:</span>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('Kansas City Chiefs')">+ Chiefs</button>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('San Francisco 49ers')">+ 49ers</button>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('Detroit Lions')">+ Lions</button>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('Baltimore Ravens')">+ Ravens</button>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('Buffalo Bills')">+ Bills</button>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('Philadelphia Eagles')">+ Eagles</button>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('Green Bay Packers')">+ Packers</button>
                  <button type="button" class="team-preset-tag" onclick="toggleTeamPreset('Dallas Cowboys')">+ Cowboys</button>
                </div>
              </div>

              <div class="form-field">
                <label>Top Matches Limit</label>
                <input type="number" id="cfg-TOP_MATCHES_LIMIT" min="1" max="10" placeholder="1">
                <small>Number of highest-rated matches grabbed each week (1 = top match only)</small>
              </div>

              <div class="form-field">
                <label>Quality Preference</label>
                <select id="cfg-QUALITY_PREFERENCE">
                  <option value="">Any (first available)</option>
                  <option value="1080p">1080p</option>
                  <option value="720p">720p</option>
                </select>
                <small>Prioritize resolution when multiple releases are available</small>
              </div>

              <div class="form-field" style="grid-column: span 2;">
                <div class="switch-box">
                  <div class="switch-info">
                    <span>Standardize Release File Names</span>
                    <small>Formats downloaded files as "Week 08 - Team A at Team B (2026)" inside qBittorrent</small>
                  </div>
                  <label class="switch-label">
                    <input type="checkbox" id="cfg-RENAME_TORRENT_FILES">
                    <span class="switch-slider"></span>
                  </label>
                </div>
              </div>

              <div class="form-field" style="grid-column: span 2;">
                <div class="switch-box">
                  <div class="switch-info">
                    <span>Auto-start Downloads in qBittorrent</span>
                    <small>Queues torrent directly in active downloading state rather than paused</small>
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
          <div class="settings-panel">
            <div class="panel-header">
              <svg class="icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
              <h3>Service Schedule &amp; Timing</h3>
            </div>
            <div class="form-grid">
              <div class="form-field" style="grid-column: span 2;">
                <label>Schedule Active Days</label>
                <input type="text" id="cfg-SCHEDULE_DAYS" placeholder="Mon,Tue,Wed,Thu,Fri,Sat,Sun">
                <div class="day-chips-row" id="schedule-day-chips">
                  <span class="day-toggle-chip" data-day="Mon" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Mon', this)">Mon</span>
                  <span class="day-toggle-chip" data-day="Tue" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Tue', this)">Tue</span>
                  <span class="day-toggle-chip" data-day="Wed" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Wed', this)">Wed</span>
                  <span class="day-toggle-chip" data-day="Thu" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Thu', this)">Thu</span>
                  <span class="day-toggle-chip" data-day="Fri" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Fri', this)">Fri</span>
                  <span class="day-toggle-chip" data-day="Sat" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Sat', this)">Sat</span>
                  <span class="day-toggle-chip" data-day="Sun" onclick="toggleDay('cfg-SCHEDULE_DAYS', 'Sun', this)">Sun</span>
                </div>
              </div>

              <div class="form-field">
                <label>Schedule Times (24h or 12h)</label>
                <input type="text" id="cfg-SCHEDULE_TIMES" placeholder="09:00,15:00,21:00">
                <small>Times of day to execute automatic grabber checks (comma-separated)</small>
              </div>

              <div class="form-field">
                <label>Top Matches Days</label>
                <input type="text" id="cfg-TOP_MATCHES_DAYS" placeholder="Mon,Tue,Wed">
                <div class="day-chips-row" id="top-day-chips">
                  <span class="day-toggle-chip" data-day="Mon" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Mon', this)">Mon</span>
                  <span class="day-toggle-chip" data-day="Tue" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Tue', this)">Tue</span>
                  <span class="day-toggle-chip" data-day="Wed" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Wed', this)">Wed</span>
                  <span class="day-toggle-chip" data-day="Thu" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Thu', this)">Thu</span>
                  <span class="day-toggle-chip" data-day="Fri" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Fri', this)">Fri</span>
                  <span class="day-toggle-chip" data-day="Sat" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Sat', this)">Sat</span>
                  <span class="day-toggle-chip" data-day="Sun" onclick="toggleDay('cfg-TOP_MATCHES_DAYS', 'Sun', this)">Sun</span>
                </div>
              </div>

              <div class="form-field">
                <label>Interval Hours (Fallback)</label>
                <input type="number" id="cfg-CHECK_INTERVAL_HOURS" step="0.5" min="0.5" placeholder="6">
                <small>Used only when Schedule Times field is left empty</small>
              </div>
            </div>
          </div>

          <!-- Section 3: 720pier Tracker -->
          <div class="settings-panel">
            <div class="panel-header">
              <svg class="icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>
              <h3>720pier Forum Connection</h3>
            </div>
            <div class="form-grid">
              <div class="form-field" style="grid-column: span 2;">
                <label>Thread URL</label>
                <input type="text" id="cfg-PIER_THREAD_URL" placeholder="https://720pier.ru/viewtopic.php?t=76272">
              </div>

              <div class="form-field">
                <label>Username</label>
                <input type="text" id="cfg-PIER_USERNAME" placeholder="720pier forum username">
              </div>

              <div class="form-field">
                <label>Password</label>
                <div class="input-wrapper">
                  <input type="password" id="cfg-PIER_PASSWORD" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-eye" onclick="togglePasswordVisibility('cfg-PIER_PASSWORD')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>

              <div class="form-field" style="grid-column: span 2;">
                <label>Session Cookie (PIER_COOKIE)</label>
                <textarea id="cfg-PIER_COOKIE" rows="2" placeholder="Leave unchanged to keep existing or paste updated browser cookie"></textarea>
                <small>Session cookie used if CAPTCHA challenge is active on 720pier</small>
              </div>

              <div class="form-field" style="grid-column: span 2;">
                <label>Proxy URL (Optional)</label>
                <input type="text" id="cfg-PROXY_URL" placeholder="socks5://127.0.0.1:1080 or http://proxy:8080">
              </div>
            </div>
          </div>

          <!-- Section 4: qBittorrent Configuration -->
          <div class="settings-panel">
            <div class="panel-header">
              <svg class="icon" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
              <h3>qBittorrent Configuration</h3>
            </div>
            <div class="form-grid">
              <div class="form-field">
                <label>Host URL</label>
                <input type="text" id="cfg-QBIT_HOST" placeholder="http://qbittorrent:8087">
              </div>

              <div class="form-field">
                <label>API Key (qBittorrent v5.2+)</label>
                <div class="input-wrapper">
                  <input type="password" id="cfg-QBIT_API_KEY" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-eye" onclick="togglePasswordVisibility('cfg-QBIT_API_KEY')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>

              <div class="form-field">
                <label>Username (Fallback)</label>
                <input type="text" id="cfg-QBIT_USERNAME" placeholder="admin">
              </div>

              <div class="form-field">
                <label>Password (Fallback)</label>
                <div class="input-wrapper">
                  <input type="password" id="cfg-QBIT_PASSWORD" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-eye" onclick="togglePasswordVisibility('cfg-QBIT_PASSWORD')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>

              <div class="form-field">
                <label>Save Path</label>
                <input type="text" id="cfg-QBIT_SAVE_PATH" placeholder="/nfl">
              </div>

              <div class="form-field">
                <label>Category</label>
                <input type="text" id="cfg-QBIT_CATEGORY" placeholder="NFL">
              </div>

              <div class="form-field" style="grid-column: span 2;">
                <label>Tags (comma-separated)</label>
                <input type="text" id="cfg-QBIT_TAGS" placeholder="720pier,thuuz">
              </div>
            </div>
          </div>

          <!-- Section 5: Notifications -->
          <div class="settings-panel">
            <div class="panel-header">
              <svg class="icon" viewBox="0 0 24 24"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
              <h3>Notification Dispatchers</h3>
            </div>
            <div class="form-grid">
              <div class="form-field" style="grid-column: span 2;">
                <label>Discord Webhook URL</label>
                <div class="input-wrapper">
                  <input type="password" id="cfg-DISCORD_WEBHOOK_URL" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-eye" onclick="togglePasswordVisibility('cfg-DISCORD_WEBHOOK_URL')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>

              <div class="form-field">
                <label>Telegram Bot Token</label>
                <div class="input-wrapper">
                  <input type="password" id="cfg-TELEGRAM_BOT_TOKEN" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-eye" onclick="togglePasswordVisibility('cfg-TELEGRAM_BOT_TOKEN')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>

              <div class="form-field">
                <label>Telegram Chat ID</label>
                <input type="text" id="cfg-TELEGRAM_CHAT_ID" placeholder="e.g. 123456789">
              </div>
            </div>
          </div>

          <!-- Section 6: Security -->
          <div class="settings-panel">
            <div class="panel-header">
              <svg class="icon" viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
              <h3>Web Dashboard Security (HTTP Basic Auth)</h3>
            </div>
            <div class="form-grid">
              <div class="form-field">
                <label>Web Dashboard Username</label>
                <input type="text" id="cfg-WEB_USERNAME" placeholder="Leave blank to disable authentication">
                <small>Requires browser login prompt when accessing dashboard</small>
              </div>

              <div class="form-field">
                <label>Web Dashboard Password</label>
                <div class="input-wrapper">
                  <input type="password" id="cfg-WEB_PASSWORD" placeholder="Leave unchanged to keep existing">
                  <button type="button" class="btn-eye" onclick="togglePasswordVisibility('cfg-WEB_PASSWORD')">
                    <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                  </button>
                </div>
              </div>
            </div>
          </div>

          <!-- Sticky Save Bar -->
          <div class="save-toolbar">
            <div style="display: flex; align-items: center; gap: 0.75rem;">
              <span id="settings-status" style="font-size: 0.84rem; font-weight: 600; color: var(--green);"></span>
            </div>
            <div style="display: flex; align-items: center; gap: 0.75rem;">
              <button type="button" class="btn-header" id="btn-reload-settings">Reset Changes</button>
              <button type="submit" class="btn-save-primary" id="btn-save-settings">
                <svg class="icon" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
                <span>Save &amp; Hot-Reload</span>
              </button>
            </div>
          </div>
        </form>
      </div>
    </section>

  </main>

  <!-- Diagnostics Modal -->
  <div class="modal-overlay" id="diag-modal">
    <div class="modal-box">
      <div class="modal-header">
        <h3>
          <svg class="icon" style="color: var(--cyan);" viewBox="0 0 24 24"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
          <span>Subsystem Diagnostics</span>
        </h3>
        <button class="modal-close-btn" onclick="closeDiagModal()">
          <svg class="icon" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
        </button>
      </div>
      <div class="modal-body">
        <p style="margin-bottom: 1rem; font-size: 0.8rem; color: var(--muted);">Test live connectivity and authentication with downstream services:</p>
        <div class="diag-card-list">
          <div class="diag-item">
            <div class="diag-meta">
              <span class="diag-name">
                <svg class="icon icon-sm" style="color: var(--cyan);" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
                <span>qBittorrent Client</span>
              </span>
              <span class="diag-desc">Verifies host URL, API credentials, and Web API version</span>
            </div>
            <button class="btn-action" id="btn-diag-qbit">Run Test</button>
          </div>

          <div class="diag-item">
            <div class="diag-meta">
              <span class="diag-name">
                <svg class="icon icon-sm" style="color: var(--green);" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>
                <span>720pier Tracker</span>
              </span>
              <span class="diag-desc">Tests forum access, session cookie, and active week parsing</span>
            </div>
            <button class="btn-action" id="btn-diag-pier">Run Test</button>
          </div>

          <div class="diag-item">
            <div class="diag-meta">
              <span class="diag-name">
                <svg class="icon icon-sm" style="color: var(--amber);" viewBox="0 0 24 24"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
                <span>Alert Notifications</span>
              </span>
              <span class="diag-desc">Dispatches a test notification message to Discord &amp; Telegram</span>
            </div>
            <button class="btn-action" id="btn-diag-alert">Send Alert</button>
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <button class="btn-action" onclick="closeDiagModal()">Close</button>
      </div>
    </div>
  </div>

  <!-- Forget Confirmation Modal -->
  <div class="modal-overlay" id="confirm-modal">
    <div class="modal-box">
      <div class="modal-header">
        <h3>
          <svg class="icon" style="color: var(--rose);" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
          <span>Forget Match from History?</span>
        </h3>
        <button class="modal-close-btn" onclick="closeConfirmModal()">
          <svg class="icon" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
        </button>
      </div>
      <div class="modal-body" id="confirm-modal-body">
        Are you sure you want to forget this match?
      </div>
      <div class="modal-footer">
        <button class="btn-action" onclick="closeConfirmModal()">Cancel</button>
        <button class="btn-forget" id="confirm-modal-btn">Forget Record</button>
      </div>
    </div>
  </div>

  <!-- Toast Container -->
  <div id="toast-container"></div>

  <script>
    // NFL Team Directory with official branding colors & abbreviations
    const NFL_TEAMS = {
      'cardinals': { code: 'ARI', name: 'Arizona Cardinals', color: '#97233F' },
      'falcons': { code: 'ATL', name: 'Atlanta Falcons', color: '#A71930' },
      'ravens': { code: 'BAL', name: 'Baltimore Ravens', color: '#241773' },
      'bills': { code: 'BUF', name: 'Buffalo Bills', color: '#00338D' },
      'panthers': { code: 'CAR', name: 'Carolina Panthers', color: '#0085CA' },
      'bears': { code: 'CHI', name: 'Chicago Bears', color: '#0B162A' },
      'bengals': { code: 'CIN', name: 'Cincinnati Bengals', color: '#FB4F14' },
      'browns': { code: 'CLE', name: 'Cleveland Browns', color: '#311D00' },
      'cowboys': { code: 'DAL', name: 'Dallas Cowboys', color: '#003594' },
      'broncos': { code: 'DEN', name: 'Denver Broncos', color: '#FB4F14' },
      'lions': { code: 'DET', name: 'Detroit Lions', color: '#0076B6' },
      'packers': { code: 'GB', name: 'Green Bay Packers', color: '#203731' },
      'texans': { code: 'HOU', name: 'Houston Texans', color: '#03202F' },
      'colts': { code: 'IND', name: 'Indianapolis Colts', color: '#002C5F' },
      'jaguars': { code: 'JAX', name: 'Jacksonville Jaguars', color: '#006778' },
      'chiefs': { code: 'KC', name: 'Kansas City Chiefs', color: '#E31837' },
      'raiders': { code: 'LV', name: 'Las Vegas Raiders', color: '#000000' },
      'chargers': { code: 'LAC', name: 'Los Angeles Chargers', color: '#0080C6' },
      'rams': { code: 'LAR', name: 'Los Angeles Rams', color: '#003594' },
      'dolphins': { code: 'MIA', name: 'Miami Dolphins', color: '#008E97' },
      'vikings': { code: 'MIN', name: 'Minnesota Vikings', color: '#4F2683' },
      'patriots': { code: 'NE', name: 'New England Patriots', color: '#002244' },
      'saints': { code: 'NO', name: 'New Orleans Saints', color: '#D3BC8D' },
      'giants': { code: 'NYG', name: 'New York Giants', color: '#0B2265' },
      'jets': { code: 'NYJ', name: 'New York Jets', color: '#125740' },
      'eagles': { code: 'PHI', name: 'Philadelphia Eagles', color: '#004C54' },
      'steelers': { code: 'PIT', name: 'Pittsburgh Steelers', color: '#FFB612' },
      '49ers': { code: 'SF', name: 'San Francisco 49ers', color: '#AA0000' },
      'seahawks': { code: 'SEA', name: 'Seattle Seahawks', color: '#002244' },
      'buccaneers': { code: 'TB', name: 'Tampa Bay Buccaneers', color: '#D50A0A' },
      'titans': { code: 'TEN', name: 'Tennessee Titans', color: '#0C2340' },
      'commanders': { code: 'WAS', name: 'Washington Commanders', color: '#5A1414' },
    };

    function resolveTeam(str) {
      if (!str) return { code: 'NFL', name: 'NFL Team', color: '#1e293b' };
      const s = str.toLowerCase();
      for (const [key, t] of Object.entries(NFL_TEAMS)) {
        if (s.includes(key) || s.includes(t.code.toLowerCase()) || s.includes(t.name.toLowerCase())) {
          return t;
        }
      }
      const words = str.trim().split(/\s+/);
      const code = words.length > 1 ? (words[0][0] + words[1][0]).toUpperCase() : str.substring(0, 3).toUpperCase();
      return { code, name: str.trim(), color: '#1e293b' };
    }

    // State
    let matchesData = [];
    let browserData = [];
    let activeWeekFilter = 'all';
    let currentViewMode = 'grid'; // 'grid' | 'table'
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
        iconSvg = '<svg class="icon icon-sm" style="color: var(--rose);" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>';
      } else if (type === 'warning') {
        iconSvg = '<svg class="icon icon-sm" style="color: var(--amber);" viewBox="0 0 24 24"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>';
      } else {
        iconSvg = '<svg class="icon icon-sm" style="color: var(--cyan);" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';
      }
      toast.innerHTML = `${iconSvg}<div style="flex:1;">${escapeHtml(message)}</div>`;
      toastContainer.appendChild(toast);
      setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(50px)';
        toast.style.transition = 'all 0.3s ease-out';
        setTimeout(() => toast.remove(), 300);
      }, duration);
    }

    // Modal Helpers
    function openDiagModal() { document.getElementById('diag-modal').classList.add('show'); }
    function closeDiagModal() { document.getElementById('diag-modal').classList.remove('show'); }

    function openConfirmModal(message, onConfirm) {
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

    // View Mode Toggle (Grid vs Table)
    function setViewMode(mode) {
      currentViewMode = mode;
      const gridBtn = document.getElementById('btn-view-grid');
      const tableBtn = document.getElementById('btn-view-table');
      const gridContainer = document.getElementById('matches-cards-container');
      const tableContainer = document.getElementById('matches-table-container');

      if (mode === 'grid') {
        gridBtn.classList.add('active');
        tableBtn.classList.remove('active');
        gridContainer.style.display = 'grid';
        tableContainer.style.display = 'none';
      } else {
        tableBtn.classList.add('active');
        gridBtn.classList.remove('active');
        gridContainer.style.display = 'none';
        tableContainer.style.display = 'block';
      }
    }

    // Tab Navigation
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
        if (targetId === 'tab-settings') loadSettings();
      });
    });

    // Keyboard Shortcuts
    document.addEventListener('keydown', (e) => {
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
      if (e.key === '1') document.querySelector('.tab-btn[data-tab="tab-matches"]')?.click();
      if (e.key === '2') document.querySelector('.tab-btn[data-tab="tab-browser"]')?.click();
      if (e.key === '3') document.querySelector('.tab-btn[data-tab="tab-settings"]')?.click();
      if (e.key === 'g') setViewMode(currentViewMode === 'grid' ? 'table' : 'grid');
      if (e.key === 'r') { fetchStatus(); fetchMatches(); fetchBrowserMatches(); }
      if (e.key === '/') {
        e.preventDefault();
        const activeTab = document.querySelector('.tab-pane.active')?.id;
        if (activeTab === 'tab-matches') document.getElementById('matches-search')?.focus();
        if (activeTab === 'tab-browser') document.getElementById('browser-search')?.focus();
      }
      if (e.key === 'Escape') { closeDiagModal(); closeConfirmModal(); }
    });

    // Run Trigger Actions
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
      document.getElementById('run-btn-text').textContent = dryRun ? 'Starting Dry Run...' : 'Running Cycle...';

      try {
        const res = await fetch(`/api/trigger?dry_run=${dryRun ? '1' : '0'}`, { method: 'POST' });
        const data = await res.json();
        if (res.status === 202) {
          showToast(data.message || 'Grabber cycle started successfully!', 'success');
        } else if (res.status === 409) {
          showToast(data.error || 'A grabber cycle is already in progress.', 'warning');
        } else {
          showToast(data.error || 'Failed to trigger cycle.', 'error');
        }
      } catch (err) {
        showToast('Network error triggering cycle: ' + err.message, 'error');
      } finally {
        setTimeout(() => {
          fetchStatus();
          fetchMatches();
        }, 600);
      }
    }

    runBtn.addEventListener('click', () => triggerCycle(false));
    document.getElementById('btn-opt-live').addEventListener('click', () => triggerCycle(false));
    document.getElementById('btn-opt-dryrun').addEventListener('click', () => triggerCycle(true));

    // Diagnostics Handlers
    document.getElementById('btn-diag-qbit').addEventListener('click', async () => {
      const btn = document.getElementById('btn-diag-qbit');
      btn.disabled = true;
      btn.textContent = 'Testing...';
      try {
        const res = await fetch('/api/test/qbit', { method: 'POST' });
        const data = await res.json();
        if (data.ok) {
          showToast(`qBittorrent Connected! Version: ${data.version} (API: ${data.web_api_version})`, 'success', 5000);
        } else {
          showToast(`qBit Test Failed: ${data.error}`, 'error', 6000);
        }
      } catch (e) {
        showToast('qBit connection error: ' + e.message, 'error');
      } finally {
        btn.disabled = false;
        btn.textContent = 'Run Test';
      }
    });

    document.getElementById('btn-diag-pier').addEventListener('click', async () => {
      const btn = document.getElementById('btn-diag-pier');
      btn.disabled = true;
      btn.textContent = 'Testing...';
      try {
        const res = await fetch('/api/test/pier', { method: 'POST' });
        const data = await res.json();
        if (data.ok) {
          showToast(`720pier Session Active! Active Week: ${data.detected_week}`, 'success', 5000);
        } else {
          showToast(`720pier Test Failed: ${data.error}`, 'error', 6000);
        }
      } catch (e) {
        showToast('720pier test error: ' + e.message, 'error');
      } finally {
        btn.disabled = false;
        btn.textContent = 'Run Test';
      }
    });

    document.getElementById('btn-diag-alert').addEventListener('click', async () => {
      const btn = document.getElementById('btn-diag-alert');
      btn.disabled = true;
      btn.textContent = 'Sending...';
      try {
        const res = await fetch('/api/test/alert', { method: 'POST' });
        const data = await res.json();
        if (data.ok) {
          const discord = data.results.discord === true ? 'Discord: Sent' : data.results.discord;
          const telegram = data.results.telegram === true ? 'Telegram: Sent' : data.results.telegram;
          showToast(`Test Dispatched! (${discord}, ${telegram})`, 'success', 5000);
        } else {
          showToast(`Alert Test Failed: ${data.error}`, 'error', 6000);
        }
      } catch (e) {
        showToast('Alert test error: ' + e.message, 'error');
      } finally {
        btn.disabled = false;
        btn.textContent = 'Send Alert';
      }
    });

    // Helper: Parse match title details
    function parseGameRelease(rawTitle) {
      if (!rawTitle) return { awayStr: '', homeStr: '', resolution: '', fps: '', network: '', format: 'MKV', date: '' };
      let clean = rawTitle;
      let date = '';
      let awayStr = '';
      let homeStr = '';

      const parts = rawTitle.split('/');
      if (parts.length >= 4) {
        date = parts[2].trim();
        const matchupPart = parts[3].split('[')[0].trim();
        const splitBy = matchupPart.includes('@') ? '@' : (matchupPart.includes(' vs ') ? ' vs ' : (matchupPart.includes(' at ') ? ' at ' : null));
        if (splitBy) {
          const sides = matchupPart.split(splitBy);
          awayStr = sides[0].trim();
          homeStr = sides[1].trim();
        } else {
          awayStr = matchupPart;
        }
      } else {
        const splitBy = clean.includes('@') ? '@' : (clean.includes(' vs ') ? ' vs ' : (clean.includes(' at ') ? ' at ' : null));
        if (splitBy) {
          const sides = clean.split(splitBy);
          awayStr = sides[0].trim();
          homeStr = sides[1].trim();
        } else {
          awayStr = clean;
        }
      }

      let resolution = '';
      if (/1080p/i.test(rawTitle)) resolution = '1080p';
      else if (/720p/i.test(rawTitle)) resolution = '720p';

      let fps = /60fps/i.test(rawTitle) ? '60 FPS' : '';

      let network = '';
      const net = rawTitle.match(/EN\/(FOX|CBS|ESPN|NBC|ABC|AMZN|NFLN|PEACOCK)/i);
      if (net) network = net[1].toUpperCase();

      return { awayStr, homeStr, resolution, fps, network, format: 'MKV', date };
    }

    // Grabbed Matches Management
    async function fetchMatches() {
      try {
        const res = await fetch('/api/torrents');
        const data = await res.json();
        matchesData = data.torrents || [];
        updateWeekFilterChips();
        renderMatches();
      } catch (err) {
        document.getElementById('matches-summary').textContent = 'Error loading matches: ' + err.message;
      }
    }

    function updateWeekFilterChips() {
      const container = document.getElementById('week-filter-chips');
      const weeks = [...new Set(matchesData.map(m => m.week).filter(Boolean))];
      if (weeks.length <= 1) {
        container.innerHTML = '';
        return;
      }

      let html = `<button class="filter-chip ${activeWeekFilter === 'all' ? 'active' : ''}" onclick="filterByWeek('all')">All Weeks</button>`;
      weeks.forEach(w => {
        html += `<button class="filter-chip ${activeWeekFilter === w ? 'active' : ''}" onclick="filterByWeek('${escapeHtml(w)}')">${escapeHtml(w)}</button>`;
      });
      container.innerHTML = html;
    }

    function filterByWeek(w) {
      activeWeekFilter = w;
      updateWeekFilterChips();
      renderMatches();
    }

    function renderMatches() {
      const cardsContainer = document.getElementById('matches-cards-container');
      const tbody = document.getElementById('matches-tbody');
      const query = (document.getElementById('matches-search').value || '').toLowerCase().trim();

      const filtered = matchesData.filter(m => {
        if (activeWeekFilter !== 'all' && m.week !== activeWeekFilter) return false;
        const text = `${m.title} ${m.week} ${m.rating || ''}`.toLowerCase();
        return !query || text.includes(query);
      });

      document.getElementById('matches-summary').textContent = `Showing ${filtered.length} of ${matchesData.length} match(es)`;
      document.getElementById('tab-count-torrents').textContent = matchesData.length;

      if (filtered.length === 0) {
        const emptyHtml = `<div style="grid-column: 1/-1; text-align: center; color: var(--muted); padding: 4rem 1rem;">
          <svg class="icon icon-lg" style="margin-bottom: 0.65rem; opacity: 0.35;" viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
          <div style="font-weight: 600; font-size: 0.95rem; color: #fff;">No Grabbed Matches Found</div>
          <p style="font-size: 0.78rem; margin-top: 0.25rem;">Adjust search filters or trigger a live grabber check.</p>
        </div>`;
        cardsContainer.innerHTML = emptyHtml;
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--muted); padding: 3rem;">No grabbed matches match your search criteria.</td></tr>`;
        return;
      }

      // Render Cards View
      cardsContainer.innerHTML = filtered.map(m => {
        const parsed = parseGameRelease(m.title);
        const awayTeam = resolveTeam(parsed.awayStr);
        const homeTeam = resolveTeam(parsed.homeStr);

        const ratingVal = parseInt(m.rating, 10);
        let scoreClass = 'score-high';
        if (isNaN(ratingVal)) scoreClass = 'score-med';
        else if (ratingVal < 90) scoreClass = 'score-med';

        const isFav = (m.title || '').includes('Chiefs') || (m.title || '').includes('KC');
        const favBadge = isFav ? `<span class="score-badge" style="background: rgba(245, 158, 11, 0.2); border: 1px solid rgba(245, 158, 11, 0.5); color: #fbbf24;">⭐ KC Chiefs</span>` : '';

        const scorePill = m.rating
          ? `<span class="score-badge ${scoreClass}"><svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" fill="currentColor"/></svg> ${escapeHtml(m.rating)} pts</span>`
          : `<span class="score-badge score-low">N/A</span>`;

        let metaHtml = '';
        if (parsed.resolution) metaHtml += `<span class="meta-tag res-${parsed.resolution}">${escapeHtml(parsed.resolution)}</span>`;
        if (parsed.fps) metaHtml += `<span class="meta-tag">${escapeHtml(parsed.fps)}</span>`;
        if (parsed.network) metaHtml += `<span class="meta-tag network">${escapeHtml(parsed.network)}</span>`;
        if (parsed.format) metaHtml += `<span class="meta-tag">${escapeHtml(parsed.format)}</span>`;

        const dateStr = m.added_at ? m.added_at.replace('T', ' ').substring(0, 16) : '--';

        return `<div class="game-card ${isFav ? 'is-favorite' : ''}">
          <div class="card-top-bar">
            <span class="week-pill">${escapeHtml(m.week)}</span>
            <div style="display: flex; gap: 0.35rem; align-items: center;">
              ${favBadge}
              ${scorePill}
            </div>
          </div>

          <div class="matchup-box">
            <div class="team-item">
              <div class="team-badge" style="background-color: ${awayTeam.color};">${awayTeam.code}</div>
              <span class="team-name" title="${escapeHtml(awayTeam.name)}">${escapeHtml(awayTeam.name)}</span>
            </div>
            <span class="versus-badge">@</span>
            <div class="team-item home">
              <span class="team-name" title="${escapeHtml(homeTeam.name)}">${escapeHtml(homeTeam.name)}</span>
              <div class="team-badge" style="background-color: ${homeTeam.color};">${homeTeam.code}</div>
            </div>
          </div>

          <div class="card-meta-row">${metaHtml}</div>

          <div class="card-footer">
            <span class="added-date">Added ${dateStr}</span>
            <div class="card-action-group">
              <a href="${escapeHtml(m.torrent_url)}" target="_blank" class="btn-action">
                <span>Torrent</span>
                <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
              </a>
              <button class="btn-forget" onclick="confirmResetTorrent(${m.id}, '${escapeHtml(awayTeam.name)} @ ${escapeHtml(homeTeam.name)}')">
                <svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                <span>Forget</span>
              </button>
            </div>
          </div>
        </div>`;
      }).join('');

      // Render Table View
      tbody.innerHTML = filtered.map(m => {
        const ratingVal = parseInt(m.rating, 10);
        let scoreClass = 'score-high';
        if (isNaN(ratingVal)) scoreClass = 'score-med';
        else if (ratingVal < 90) scoreClass = 'score-med';

        return `<tr>
          <td style="color: var(--muted); font-family: 'Fira Code', monospace; font-size: 0.75rem;">#${m.id}</td>
          <td><span class="week-pill">${escapeHtml(m.week)}</span></td>
          <td><span class="score-badge ${scoreClass}">${escapeHtml(m.rating || 'N/A')} pts</span></td>
          <td><strong style="color: #fff;">${escapeHtml(m.title)}</strong></td>
          <td style="color: var(--muted); font-size: 0.75rem; font-family: 'Fira Code', monospace;">${escapeHtml(m.added_at ? m.added_at.replace('T', ' ').substring(0, 16) : '--')}</td>
          <td>
            <a href="${escapeHtml(m.torrent_url)}" target="_blank" class="btn-action">
              <span>Open Link</span>
              <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </a>
          </td>
          <td style="text-align: center;">
            <button class="btn-forget" onclick="confirmResetTorrent(${m.id}, '${escapeHtml(m.title).replace(/'/g, "\\'")}')">Forget</button>
          </td>
        </tr>`;
      }).join('');
    }

    function confirmResetTorrent(id, title) {
      openConfirmModal(
        `Are you sure you want to remove "${title}" from download history? This allows the grabber to re-evaluate and re-download it in future cycles.`,
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

    // Active Week Match Browser Management
    async function fetchBrowserMatches(forceRefresh = false) {
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
        document.getElementById('browser-cards-container').innerHTML = `<div style="grid-column: 1/-1; text-align: center; color: var(--rose); padding: 3rem;">Error: ${escapeHtml(err.message)}</div>`;
        if (forceRefresh) showToast('Failed to refresh matches: ' + err.message, 'error');
      } finally {
        refreshBtn.disabled = false;
        refreshBtn.innerHTML = '<svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg><span>Refresh from 720pier</span>';
      }
    }

    function renderBrowserMatches(weekTitle = 'Active Week') {
      const container = document.getElementById('browser-cards-container');
      const tbody = document.getElementById('browser-tbody');
      const query = (document.getElementById('browser-search').value || '').toLowerCase().trim();

      const filtered = browserData.filter(m => {
        const text = `${m.title} ${m.rating || ''}`.toLowerCase();
        return !query || text.includes(query);
      });

      document.getElementById('browser-summary').textContent = `${weekTitle} &mdash; Showing ${filtered.length} of ${browserData.length} games`;

      if (filtered.length === 0) {
        container.innerHTML = `<div style="grid-column: 1/-1; text-align: center; color: var(--muted); padding: 4rem 1rem;">
          <svg class="icon icon-lg" style="margin-bottom: 0.5rem; opacity: 0.4;" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
          <div>No active week matchups match your search.</div>
        </div>`;
        tbody.innerHTML = '';
        return;
      }

      container.innerHTML = filtered.map((m, idx) => {
        const ratingVal = parseInt(m.rating, 10);
        let scoreClass = 'score-high';
        if (isNaN(ratingVal)) scoreClass = 'score-low';
        else if (ratingVal < 70) scoreClass = 'score-low';
        else if (ratingVal < 90) scoreClass = 'score-med';

        const parsed = parseGameRelease(m.title);
        const awayTeam = resolveTeam(parsed.awayStr);
        const homeTeam = resolveTeam(parsed.homeStr);

        const favBadge = m.is_favorite
          ? `<span class="score-badge" style="background: rgba(245, 158, 11, 0.2); border: 1px solid rgba(245, 158, 11, 0.5); color: #fbbf24;">⭐ KC Chiefs</span>` : '';

        const scorePill = m.rating && m.rating !== 'N/A'
          ? `<span class="score-badge ${scoreClass}"><svg class="icon icon-sm" viewBox="0 0 24 24"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" fill="currentColor"/></svg> ${escapeHtml(m.rating)} pts</span>`
          : `<span class="score-badge score-low">N/A</span>`;

        let actionCol = '';
        if (m.is_grabbed) {
          actionCol = `<span class="badge-grabbed"><svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg> Grabbed</span>`;
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

        return `<div class="game-card ${m.is_favorite ? 'is-favorite' : ''}">
          <div class="card-top-bar">
            <span class="week-pill">${escapeHtml(weekTitle)}</span>
            <div style="display: flex; gap: 0.35rem; align-items: center;">
              ${favBadge}
              ${scorePill}
            </div>
          </div>

          <div class="matchup-box">
            <div class="team-item">
              <div class="team-badge" style="background-color: ${awayTeam.color};">${awayTeam.code}</div>
              <span class="team-name" title="${escapeHtml(awayTeam.name)}">${escapeHtml(awayTeam.name)}</span>
            </div>
            <span class="versus-badge">@</span>
            <div class="team-item home">
              <span class="team-name" title="${escapeHtml(homeTeam.name)}">${escapeHtml(homeTeam.name)}</span>
              <div class="team-badge" style="background-color: ${homeTeam.color};">${homeTeam.code}</div>
            </div>
          </div>

          <div style="font-size: 0.78rem; font-weight: 500; color: #fff;">${escapeHtml(m.title)}</div>

          <div class="card-footer">
            <a href="${escapeHtml(m.url)}" target="_blank" class="btn-action">
              <span>View Topic</span>
              <svg class="icon icon-sm" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
            </a>
            ${actionCol}
          </div>
        </div>`;
      }).join('');

      tbody.innerHTML = filtered.map((m, idx) => {
        const actionHtml = m.is_grabbed
          ? `<span class="badge-grabbed">Grabbed</span>`
          : `<button class="btn-grab" onclick="manualDownloadMatch('${encodeURIComponent(m.url)}', '${encodeURIComponent(m.title)}', '${encodeURIComponent(m.rating || '')}', '${encodeURIComponent(weekTitle)}', '')">Download</button>`;
        return `<tr>
          <td>#${idx + 1}</td>
          <td>${escapeHtml(m.rating || 'N/A')}</td>
          <td>${escapeHtml(m.title)}</td>
          <td>${m.is_favorite ? 'KC Chiefs' : '--'}</td>
          <td><a href="${escapeHtml(m.url)}" target="_blank">View Topic</a></td>
          <td>${actionHtml}</td>
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
          showToast(data.message || `Sent "${title}" to qBittorrent!`, 'success', 5000);
          if (btn) {
            btn.outerHTML = `<span class="badge-grabbed"><svg class="icon icon-sm" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg> Grabbed</span>`;
          }
          const item = browserData.find(x => x.url === url || x.title === title);
          if (item) item.is_grabbed = true;
          fetchMatches();
          fetchStatus();
        } else {
          showToast(`Download failed: ${data.error}`, 'error', 6000);
          if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<span>Download to qBit</span>';
          }
        }
      } catch (err) {
        showToast('Network error triggering download: ' + err.message, 'error');
        if (btn) {
          btn.disabled = false;
          btn.innerHTML = '<span>Download to qBit</span>';
        }
      }
    }

    document.getElementById('btn-refresh-browser').addEventListener('click', () => fetchBrowserMatches(true));
    document.getElementById('browser-search').addEventListener('input', () => renderBrowserMatches());

    // Settings Management
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
      container.querySelectorAll('.day-toggle-chip').forEach(chip => {
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
        status.style.color = 'var(--rose)';
        status.textContent = 'Error loading settings: ' + err.message;
        showToast('Error loading configuration: ' + err.message, 'error');
      }
    }

    async function saveSettings() {
      const status = document.getElementById('settings-status');
      const saveBtn = document.getElementById('btn-save-settings');
      saveBtn.disabled = true;
      saveBtn.innerHTML = '<span>Saving...</span>';
      status.style.color = 'var(--cyan)';
      status.textContent = 'Saving configuration...';

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
          showToast(data.message || 'Settings saved and applied!', 'success', 5000);
          status.style.color = 'var(--green)';
          status.textContent = 'Settings saved and applied successfully!';
          loadSettings();
          fetchStatus();
        } else {
          showToast('Error saving settings: ' + (data.error || 'Unknown error'), 'error', 6000);
          status.style.color = 'var(--rose)';
          status.textContent = 'Error: ' + data.error;
        }
      } catch (err) {
        showToast('Network error saving settings: ' + err.message, 'error');
        status.style.color = 'var(--rose)';
        status.textContent = 'Network error: ' + err.message;
      } finally {
        saveBtn.disabled = false;
        saveBtn.innerHTML = '<svg class="icon" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg><span>Save &amp; Hot-Reload</span>';
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
        }
        previousCycleRunning = isCycleRunning;
      } catch (err) {}
    }

    // Initialize
    fetchStatus();
    fetchMatches();
    fetchBrowserMatches();
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
