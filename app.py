import collections
import json
import logging
import os
import re
import sqlite3
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

class _RingHandler(logging.Handler):
    def __init__(self, maxlen=500):
        super().__init__()
        self.records = collections.deque(maxlen=maxlen)
    def emit(self, record):
        self.records.appendleft(self.format(record))

_ring = _RingHandler()
_ring.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logging.getLogger().addHandler(_ring)
log = logging.getLogger(__name__)

app = Flask(__name__)

_DEFAULTS = {
    'whisparr_url':       'http://localhost:6969',
    'whisparr_key':       '526a79d17104440ebb97ab39e0623a8a',
    'prowlarr_url':       'http://localhost:9696',
    'prowlarr_key':       'd49c266757bb46e2a0174c70dd6e9a04',
    'sabnzbd_url':        'http://localhost:8085',
    'sabnzbd_key':        '0420e3541c5e41ccb0b79bda7df93204',
    'qbittorrent_url':    'http://localhost:8080',
    'qbittorrent_user':   'admin',
    'qbittorrent_pass':   '',
    'performer_tag':      '',
    'auto_snatch':        True,
    'snatch_interval_h':  1,
    'retry_not_found_d':  7,
    'ytdlp_enabled':      True,
    'ytdlp_min_res':      720,
    'ytdlp_dl_dir':       '',
    'backup_roots':       ['/mnt/nas/backups', '/mnt/nas/backups-2', '/mnt/nas/backups-3'],
    'ignored_paths':      [],
}
BUILD_SHA   = os.environ.get('BUILD_SHA', 'dev')
WHISPARR_DB = os.environ.get('WHISPARR_DB', '/portainer/files/appdata/config/whisparrv3/whisparr3.db')
_DATA_DIR   = Path(os.environ.get('SNATCHARR_DATA', str(Path(__file__).parent)))
STATE_FILE  = _DATA_DIR / 'state.json'
CONFIG_FILE = _DATA_DIR / 'config.json'

# Runtime-mutable globals — updated by _reload_config()
WHISPARR_URL = _DEFAULTS['whisparr_url']
WHISPARR_KEY = _DEFAULTS['whisparr_key']
PROWLARR_URL = _DEFAULTS['prowlarr_url']
PROWLARR_KEY = _DEFAULTS['prowlarr_key']
SABNZBD_URL      = _DEFAULTS['sabnzbd_url']
SABNZBD_KEY      = _DEFAULTS['sabnzbd_key']
QBITTORRENT_URL  = _DEFAULTS['qbittorrent_url']
QBITTORRENT_USER = _DEFAULTS['qbittorrent_user']
QBITTORRENT_PASS = _DEFAULTS['qbittorrent_pass']
PERFORMER_TAG    = _DEFAULTS['performer_tag']

def _reload_config(cfg=None):
    global WHISPARR_URL, WHISPARR_KEY, PROWLARR_URL, PROWLARR_KEY
    global SABNZBD_URL, SABNZBD_KEY, PERFORMER_TAG
    global QBITTORRENT_URL, QBITTORRENT_USER, QBITTORRENT_PASS
    global AUTO_SNATCH, SNATCH_INTERVAL_H, RETRY_NOT_FOUND_D
    global YTDLP_ENABLED, YTDLP_MIN_RES, YTDLP_DL_DIR
    global BACKUP_ROOTS, IGNORED_PATHS
    if cfg is None:
        cfg = load_config()
    WHISPARR_URL      = cfg.get('whisparr_url',       _DEFAULTS['whisparr_url'])
    WHISPARR_KEY      = cfg.get('whisparr_key',       _DEFAULTS['whisparr_key'])
    PROWLARR_URL      = cfg.get('prowlarr_url',       _DEFAULTS['prowlarr_url'])
    PROWLARR_KEY      = cfg.get('prowlarr_key',       _DEFAULTS['prowlarr_key'])
    SABNZBD_URL       = cfg.get('sabnzbd_url',        _DEFAULTS['sabnzbd_url'])
    SABNZBD_KEY       = cfg.get('sabnzbd_key',        _DEFAULTS['sabnzbd_key'])
    QBITTORRENT_URL   = cfg.get('qbittorrent_url',   _DEFAULTS['qbittorrent_url'])
    QBITTORRENT_USER  = cfg.get('qbittorrent_user',  _DEFAULTS['qbittorrent_user'])
    QBITTORRENT_PASS  = cfg.get('qbittorrent_pass',  _DEFAULTS['qbittorrent_pass'])
    PERFORMER_TAG     = cfg.get('performer_tag',      _DEFAULTS['performer_tag'])
    AUTO_SNATCH       = cfg.get('auto_snatch',        _DEFAULTS['auto_snatch'])
    SNATCH_INTERVAL_H = cfg.get('snatch_interval_h',  _DEFAULTS['snatch_interval_h'])
    RETRY_NOT_FOUND_D = cfg.get('retry_not_found_d',  _DEFAULTS['retry_not_found_d'])
    YTDLP_ENABLED     = cfg.get('ytdlp_enabled',      _DEFAULTS['ytdlp_enabled'])
    YTDLP_MIN_RES     = int(cfg.get('ytdlp_min_res',  _DEFAULTS['ytdlp_min_res']))
    YTDLP_DL_DIR      = cfg.get('ytdlp_dl_dir',       _DEFAULTS['ytdlp_dl_dir'])
    raw_roots         = cfg.get('backup_roots',        _DEFAULTS['backup_roots'])
    BACKUP_ROOTS      = [p for p in raw_roots if Path(p).exists()]
    IGNORED_PATHS     = set(cfg.get('ignored_paths',   _DEFAULTS['ignored_paths']))

AUTO_SNATCH = True
SNATCH_INTERVAL_H = 4
RETRY_NOT_FOUND_D = 7
YTDLP_ENABLED = True
YTDLP_MIN_RES = 720
YTDLP_DL_DIR  = ''

NEWZNAB_CATS = '6000,6010,6020,6030,6040,6050'

_nzb_lock = threading.Lock()
_nzb_last = 0.0
_NZB_INTERVAL = 5  # minimum seconds between NZB download attempts globally
_snatch_running = threading.Lock()  # prevent concurrent snatch runs

def get_prowlarr_indexers():
    try:
        r = requests.get(f'{PROWLARR_URL}/api/v1/indexer',
                         headers={'X-Api-Key': PROWLARR_KEY}, timeout=10)
        return [i['id'] for i in r.json() if i.get('enable', True)]
    except Exception:
        return []
CODE_RE           = re.compile(r'^(?:[A-Za-z]{1,4}\d{2,6}|\d{4,7})$')
CODE_SEARCH_RE    = re.compile(r'(?<![A-Za-z0-9])([A-Z]{1,4}\d{2,6})(?![A-Za-z0-9])', re.IGNORECASE)
VIDEO_EXTS        = {'.mkv', '.mp4', '.avi', '.m4v', '.mov'}
# Codec/tech tokens that look like scene codes but aren't
_CODE_EXCLUDE     = {
    'X264','X265','H264','H265','HEVC','AVC','AAC','MP4','MKV','AVI',
    'WRB','PRT','XXX','SD','HD','UHD','FHD','TS','WEB','DL','DC',
    'PART','VOL','EP','DVD','BLU','RAY','FPS','KHZ','MB','GB',
}
BACKUP_ROOTS:   list = []  # populated by _reload_config()
IGNORED_PATHS:  set  = set()  # populated by _reload_config()

# Scan state — shared between the background thread and the SSE stream
_scan_lock    = threading.Lock()
_scan_results = None   # None = never run; list = last results
_scan_running = False
_scan_progress = {'done': 0, 'total': 0, 'status': ''}

# Local import log — in-memory, newest first, capped at 200 entries
_local_import_log = collections.deque(maxlen=200)

# Ingest jobs — keyed by job_id string
_ingest_jobs: dict = {}

# ── Persistent scan database ──────────────────────────────────────────────────
SCAN_DB = _DATA_DIR / 'scan.db'

def _scan_db_connect():
    conn = sqlite3.connect(str(SCAN_DB), timeout=15)
    conn.row_factory = sqlite3.Row
    return conn

def _scan_db_init():
    conn = _scan_db_connect()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS backup_files (
            path      TEXT PRIMARY KEY,
            performer TEXT,
            root      TEXT,
            code      TEXT,
            size_mb   INTEGER,
            last_seen REAL
        );
        CREATE TABLE IF NOT EXISTS scan_meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        -- Global scene registry (populated from Whisparr + LP scrapes)
        CREATE TABLE IF NOT EXISTS scenes (
            code       TEXT PRIMARY KEY,
            title      TEXT,
            lp_title   TEXT,
            wid        INTEGER,
            has_file   INTEGER DEFAULT 0,
            monitored  INTEGER DEFAULT 0,
            updated_at REAL
        );
        -- Per-scrape performer→code links (keyed by source URL)
        CREATE TABLE IF NOT EXISTS performer_scrapes (
            url         TEXT PRIMARY KEY,
            label       TEXT,
            scraped_at  REAL,
            scene_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS performer_scenes (
            url  TEXT,
            code TEXT,
            PRIMARY KEY (url, code)
        );
    ''')
    conn.commit()
    conn.close()

def _scan_db_upsert(records):
    """Upsert list of (path, performer, root, code, size_mb) into backup_files."""
    conn = _scan_db_connect()
    now  = time.time()
    conn.executemany('''
        INSERT INTO backup_files (path, performer, root, code, size_mb, last_seen)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            performer = excluded.performer,
            root      = excluded.root,
            code      = excluded.code,
            size_mb   = excluded.size_mb,
            last_seen = excluded.last_seen
    ''', [(p, perf, root, code, size, now) for p, perf, root, code, size in records])
    conn.execute("INSERT OR REPLACE INTO scan_meta (key, value) VALUES ('last_scan', ?)",
                 (str(now),))
    conn.commit()
    conn.close()

def _scan_db_purge_missing():
    """Delete rows for files that no longer exist on disk. Returns count removed."""
    conn  = _scan_db_connect()
    rows  = conn.execute('SELECT path FROM backup_files').fetchall()
    gone  = [r['path'] for r in rows if not Path(r['path']).exists()]
    if gone:
        conn.executemany('DELETE FROM backup_files WHERE path = ?', [(p,) for p in gone])
        conn.commit()
        log.info('scan db: purged %d missing files', len(gone))
    conn.close()
    return len(gone)

def _compute_scan_results():
    """Load file index from DB + current Whisparr state → results list."""
    try:
        whisparr = _load_whisparr_index()
    except Exception as e:
        log.warning('compute_scan_results: whisparr index failed — %s', e)
        whisparr = {}
    conn  = _scan_db_connect()
    rows  = conn.execute(
        'SELECT path, performer, root, code, size_mb FROM backup_files'
    ).fetchall()
    conn.close()
    results = []
    for r in rows:
        path = r['path']
        if path in IGNORED_PATHS:
            continue
        code = r['code']
        entry = {
            'performer':   r['performer'] or '?',
            'root':        r['root'] or '',
            'video':       path,
            'rel':         path,
            'code':        code,
            'size_mb':     r['size_mb'] or 0,
            'existing_mb': None,
            'title':       None,
            'wid':         None,
            'status':      'no_code',
        }
        try:
            if r['root']:
                entry['rel'] = str(Path(path).relative_to(r['root']))
        except ValueError:
            pass
        if code and code in whisparr:
            wid, title, has_file, existing_mb, monitored = whisparr[code]
            entry['wid']         = wid
            entry['title']       = title
            entry['existing_mb'] = existing_mb if has_file else None
            if not monitored:
                entry['status'] = 'unmonitored'
            elif has_file:
                entry['status'] = 'dupe'
            else:
                entry['status'] = 'importable'
        elif code:
            entry['status'] = 'not_monitored'
        results.append(entry)
    return results

def _sync_scenes_from_whisparr():
    """Pull all scenes + performer links from Whisparr DB into scan.db scenes table."""
    try:
        wconn = db_connect()
        rows  = wconn.execute('''
            SELECT m.Id as wid, m.MovieFileId, m.Monitored,
                   mm.Code, mm.Title,
                   p.Name as performer_name
            FROM Credits c
            JOIN MovieMetadata mm ON mm.Id = c.MovieMetadataId
            JOIN Movies m         ON m.MovieMetadataId = mm.Id
            JOIN Performers p     ON p.ForeignId = c.PerformerForeignId
            WHERE mm.Code IS NOT NULL AND mm.Code != ''
        ''').fetchall()
        wconn.close()
    except Exception as e:
        log.warning('sync_scenes: whisparr query failed — %s', e)
        return

    now         = time.time()
    scene_map   = {}
    for r in rows:
        code = (r['Code'] or '').strip().upper()
        if not code or not CODE_RE.match(code) or code in _CODE_EXCLUDE:
            continue
        if code not in scene_map:
            scene_map[code] = (r['Title'], r['wid'], bool(r['MovieFileId']), bool(r['Monitored']))

    sconn = _scan_db_connect()
    sconn.executemany('''
        INSERT INTO scenes (code, title, wid, has_file, monitored, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            title      = excluded.title,
            wid        = excluded.wid,
            has_file   = excluded.has_file,
            monitored  = excluded.monitored,
            updated_at = excluded.updated_at
    ''', [(code, title, wid, int(hf), int(mon), now)
          for code, (title, wid, hf, mon) in scene_map.items()])
    sconn.commit()
    sconn.close()
    log.info('sync_scenes: upserted %d scenes from Whisparr', len(scene_map))


def _scene_sync_loop():
    _sync_scenes_from_whisparr()
    while True:
        time.sleep(1800)
        try:
            _sync_scenes_from_whisparr()
        except Exception:
            log.exception('scene sync loop error')


def _startup_load_scan():
    """Called once at startup: populate _scan_results from persistent DB (no disk walk)."""
    global _scan_results, _scan_running, _scan_progress
    try:
        conn  = _scan_db_connect()
        count = conn.execute('SELECT COUNT(*) FROM backup_files').fetchone()[0]
        conn.close()
        if count == 0:
            log.info('scan db: empty — waiting for first manual scan')
            return
        with _scan_lock:
            if _scan_running:
                return
            _scan_running  = True
            _scan_progress = {'done': 0, 'total': count, 'status': 'Loading file index from disk…'}
        log.info('scan db: loading %d indexed files on startup…', count)
        results = _compute_scan_results()
        with _scan_lock:
            _scan_results  = results
            _scan_running  = False
            _scan_progress = {'done': count, 'total': count, 'status': 'Done'}
        log.info('scan db: startup load complete — %d results', len(results))
    except Exception as e:
        log.warning('scan db startup load failed: %s', e)
        with _scan_lock:
            _scan_running = False

# Initialise DB schema at import time
_scan_db_init()

def _atomic_write(path: Path, data: str):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(data)
    tmp.replace(path)

def _safe_json_load(path: Path, default):
    try:
        return json.loads(path.read_text())
    except Exception:
        return default

def load_config():
    return _safe_json_load(CONFIG_FILE, {'active_studios': []})

def save_config(cfg):
    _atomic_write(CONFIG_FILE, json.dumps(cfg, indent=2))

# Apply saved config on startup
_reload_config()

def get_active_studios():
    return set(load_config().get('active_studios', []))

def load_state():
    s = _safe_json_load(STATE_FILE, {})
    s.setdefault('queued', {})
    s.setdefault('imported', [])
    s.setdefault('not_found', [])
    s.setdefault('not_found_at', {})
    s.setdefault('failed', [])
    s.setdefault('blacklisted_releases', {})  # {code: [release_title, ...]}
    return s

def save_state(state):
    _atomic_write(STATE_FILE, json.dumps(state, indent=2))

def db_connect():
    # Open read-only via URI so SQLite never tries to write-lock the WAL/SHM files
    uri = f'file:{WHISPARR_DB}?mode=ro'
    for attempt in range(5):
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=10)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.OperationalError:
            if attempt == 4:
                raise
            time.sleep(0.5 * (attempt + 1))


def get_all_studios():
    conn = db_connect()
    rows = conn.execute('''
        SELECT DISTINCT mm.StudioTitle
        FROM Movies m
        JOIN MovieMetadata mm ON mm.Id = m.MovieMetadataId
        WHERE m.Monitored = 1 AND mm.StudioTitle IS NOT NULL
        ORDER BY mm.StudioTitle
    ''').fetchall()
    conn.close()
    return [r['StudioTitle'] for r in rows if r['StudioTitle']]

def get_performers(active_studios):
    if not active_studios:
        return []
    state        = load_state()
    queued_codes = set(state['queued'].keys())
    conn         = db_connect()
    placeholders = ','.join('?' * len(active_studios))
    studios      = list(active_studios)
    if PERFORMER_TAG:
        try:
            tag_row = conn.execute("SELECT Id FROM Tags WHERE Label=?", [PERFORMER_TAG]).fetchone()
            tag_id  = tag_row['Id'] if tag_row else None
        except Exception:
            tag_id = None
        if tag_id:
            all_p = conn.execute(
                "SELECT Id, ForeignId, Name, Images FROM Performers "
                "WHERE Monitored=1 AND Tags LIKE ? ORDER BY Name",
                [f'%{tag_id}%']
            ).fetchall()
        else:
            all_p = conn.execute(
                'SELECT Id, ForeignId, Name, Images FROM Performers WHERE Monitored=1 ORDER BY Name'
            ).fetchall()
    else:
        all_p = conn.execute(
            'SELECT Id, ForeignId, Name, Images FROM Performers WHERE Monitored=1 ORDER BY Name'
        ).fetchall()
    performers = []
    for p in all_p:
        rows = conn.execute(f'''
            SELECT m.MovieFileId, mm.Code
            FROM Credits c
            JOIN MovieMetadata mm ON mm.Id = c.MovieMetadataId
            JOIN Movies m ON m.MovieMetadataId = mm.Id
            WHERE c.PerformerForeignId = ?
              AND mm.StudioTitle IN ({placeholders})
              AND mm.Code IS NOT NULL
              AND m.Monitored = 1
        ''', [p['ForeignId']] + studios).fetchall()
        valid   = [r for r in rows if CODE_RE.match(str(r['Code'] or '').strip())]
        all_rows = conn.execute('''
            SELECT m.MovieFileId
            FROM Credits c
            JOIN MovieMetadata mm ON mm.Id = c.MovieMetadataId
            JOIN Movies m ON m.MovieMetadataId = mm.Id
            WHERE c.PerformerForeignId = ?
              AND m.Monitored = 1
        ''', [p['ForeignId']]).fetchall()
        have    = sum(1 for r in all_rows if r['MovieFileId'])
        missing = sum(1 for r in all_rows if not r['MovieFileId'])
        queued  = sum(1 for r in valid if not r['MovieFileId'] and str(r['Code']).upper() in queued_codes)
        headshot = None
        try:
            for img in json.loads(p['Images'] or '[]'):
                if img.get('coverType') == 'headshot':
                    headshot = f'/headshot/{p["Id"]}'
                    break
        except Exception:
            pass
        if not valid:
            continue
        performers.append({'name': p['Name'], 'pid': p['Id'], 'total': len(valid),
                           'have': have, 'missing': missing, 'queued': queued,
                           'headshot': headshot})
    conn.close()
    performers.sort(key=lambda p: p['name'].lower())
    return performers

def get_scenes(performer_name, active_studios):
    state           = load_state()
    queued_codes    = set(state['queued'].keys())
    imported_codes  = set(state['imported'])
    not_found_codes = set(state['not_found'])
    if not active_studios:
        return []
    conn         = db_connect()
    p = conn.execute('SELECT ForeignId FROM Performers WHERE Name=?', [performer_name]).fetchone()
    if not p:
        conn.close()
        return []
    placeholders = ','.join('?' * len(active_studios))
    rows = conn.execute(f'''
        SELECT m.Id as wid, m.MovieFileId, mm.Title, mm.Code,
               mm.StudioTitle, mm.Year, mm.Images
        FROM Credits c
        JOIN MovieMetadata mm ON mm.Id = c.MovieMetadataId
        JOIN Movies m ON m.MovieMetadataId = mm.Id
        WHERE c.PerformerForeignId = ?
          AND mm.StudioTitle IN ({placeholders})
          AND mm.Code IS NOT NULL
          AND m.Monitored = 1
        ORDER BY mm.Year DESC, mm.Code
    ''', [p['ForeignId']] + list(active_studios)).fetchall()
    conn.close()
    scenes = []
    for r in rows:
        code = str(r['Code'] or '').strip().upper()
        if not CODE_RE.match(code):
            continue
        if r['MovieFileId']:         status = 'have'
        elif code in queued_codes:   status = 'queued'
        elif code in imported_codes: status = 'imported'
        elif code in not_found_codes:status = 'not_found'
        else:                        status = 'missing'
        thumb = None
        try:
            for img in json.loads(r['Images'] or '[]'):
                url = img.get('RemoteUrl') or img.get('Url') or ''
                if url:
                    thumb = url
                    break
        except Exception:
            pass
        scenes.append({'wid': r['wid'], 'code': code, 'title': r['Title'] or code,
                       'studio': r['StudioTitle'] or '', 'year': r['Year'],
                       'status': status, 'thumb': thumb})
    return scenes

def search_prowlarr(code):
    """Return (nzb_list, torrent_list) sorted by size desc — all candidates from all indexers."""
    nzbs = []
    torrents = []
    for idx_id in get_prowlarr_indexers():
        try:
            r = requests.get(f'{PROWLARR_URL}/{idx_id}/api', params={
                't': 'search', 'q': code, 'cat': NEWZNAB_CATS, 'apikey': PROWLARR_KEY,
            }, timeout=30)
            if not r.ok:
                continue
            root = ET.fromstring(r.text)
            ns   = {'nn': 'http://www.newznab.com/DTD/2010/feeds/attributes/'}
            for item in root.findall('.//item'):
                title = (item.findtext('title') or '').strip()
                if not re.search(r'\b' + re.escape(code) + r'\b', title, re.IGNORECASE):
                    continue
                link  = item.findtext('link') or ''
                encl  = item.find('enclosure')
                encl_type = ''
                if encl is not None:
                    link = encl.get('url', link)
                    encl_type = encl.get('type', '')
                size_el = item.find('nn:attr[@name="size"]', ns)
                size = int(size_el.get('value', 0)) if size_el is not None else 0
                if not link:
                    continue
                is_torrent = (
                    encl_type == 'application/x-bittorrent'
                    or link.startswith('magnet:')
                    or '.torrent' in link.lower()
                )
                result = {'title': title, 'url': link, 'size': size, 'indexer': idx_id}
                if is_torrent:
                    torrents.append(result)
                else:
                    nzbs.append(result)
        except Exception:
            pass
    def _res_rank(title):
        t = title.lower()
        if '1080' in t: return 0
        if '2160' in t or '4k' in t: return 1
        if '720' in t: return 2
        return 3

    nzbs.sort(key=lambda x: (_res_rank(x['title']), -x['size']))
    torrents.sort(key=lambda x: (_res_rank(x['title']), -x['size']))
    return nzbs, torrents

def add_to_sabnzbd(nzb_url, nzb_name):
    global _nzb_last
    with _nzb_lock:
        wait = _NZB_INTERVAL - (time.time() - _nzb_last)
        if wait > 0:
            time.sleep(wait)
        _nzb_last = time.time()
    try:
        nzb_resp = requests.get(nzb_url, timeout=30)
        if nzb_resp.status_code == 429:
            log.warning('sabnzbd: 429 rate-limited for %s — moving to next candidate', nzb_name)
            return None
        nzb_resp.raise_for_status()
    except Exception as e:
        log.warning('sabnzbd: failed to download NZB for %s: %s', nzb_name, e)
        return None
    content = nzb_resp.content
    ct = nzb_resp.headers.get('Content-Type', '')
    # NZBs are XML — if content isn't XML or Content-Type says torrent, it was misclassified
    if 'bittorrent' in ct or not content.lstrip()[:1] == b'<':
        log.warning('sabnzbd: %s is not NZB XML (ct=%s), skipping to torrent fallback', nzb_name, ct)
        return None
    try:
        r = requests.post(f'{SABNZBD_URL}/api', params={
            'mode': 'addfile', 'nzbname': nzb_name,
            'cat': 'whisparr', 'apikey': SABNZBD_KEY, 'output': 'json',
        }, files={'nzbfile': (f'{nzb_name}.nzb', content, 'application/x-nzb')},
        timeout=60)
        data = r.json()
        log.info('sabnzbd addfile response for %s: %s', nzb_name, data)
        if data.get('status'):
            ids = data.get('nzo_ids', [])
            return ids[0] if ids else None
    except Exception as e:
        log.warning('sabnzbd: addfile exception for %s: %s', nzb_name, e)
    return None

def _qbit_session():
    if not QBITTORRENT_URL:
        return None
    try:
        s = requests.Session()
        r = s.post(f'{QBITTORRENT_URL}/api/v2/auth/login',
                   data={'username': QBITTORRENT_USER, 'password': QBITTORRENT_PASS},
                   timeout=10)
        if r.text.strip() == 'Ok.':
            return s
    except Exception:
        pass
    return None

def add_to_qbittorrent(torrent_url, name):
    s = _qbit_session()
    if not s:
        return None
    try:
        if torrent_url.startswith('magnet:'):
            r = s.post(f'{QBITTORRENT_URL}/api/v2/torrents/add',
                       data={'urls': torrent_url, 'category': 'whisparr'},
                       timeout=15)
        else:
            tr = requests.get(torrent_url, timeout=30)
            tr.raise_for_status()
            r = s.post(f'{QBITTORRENT_URL}/api/v2/torrents/add',
                       data={'category': 'whisparr'},
                       files={'torrents': (f'{name}.torrent', tr.content, 'application/x-bittorrent')},
                       timeout=15)
        if r.text.strip() != 'Ok.':
            return None
        time.sleep(2)
        torrents = s.get(f'{QBITTORRENT_URL}/api/v2/torrents/info',
                         params={'filter': 'all'}, timeout=10).json()
        recent = sorted(torrents, key=lambda x: x.get('added_on', 0), reverse=True)
        code = name.upper()
        for t in recent[:10]:
            if code in t['name'].upper():
                return t['hash']
        return recent[0]['hash'] if recent else None
    except Exception:
        pass
    return None

def get_qbittorrent_completed():
    s = _qbit_session()
    if not s:
        return {}
    try:
        r = s.get(f'{QBITTORRENT_URL}/api/v2/torrents/info',
                  params={'filter': 'completed'}, timeout=10)
        return {t['hash']: t['content_path'] for t in r.json()}
    except Exception:
        return {}

def get_sabnzbd_completed():
    try:
        r = requests.get(f'{SABNZBD_URL}/api', params={
            'mode': 'history', 'apikey': SABNZBD_KEY, 'output': 'json', 'limit': 500,
        }, timeout=15)
        return {s['nzo_id']: s.get('storage', '')
                for s in r.json().get('history', {}).get('slots', [])
                if s.get('status') == 'Completed'}
    except Exception:
        return {}

def get_sabnzbd_history():
    """Return list of (nzo_id, name, storage) for all completed SABnzbd items."""
    try:
        r = requests.get(f'{SABNZBD_URL}/api', params={
            'mode': 'history', 'apikey': SABNZBD_KEY, 'output': 'json', 'limit': 500,
        }, timeout=15)
        return [(s['nzo_id'], s.get('name', ''), s.get('storage', ''))
                for s in r.json().get('history', {}).get('slots', [])
                if s.get('status') == 'Completed']
    except Exception:
        return []

def get_sabnzbd_failed():
    """Return dict of {nzo_id: {'name': ..., 'storage': ..., 'fail_message': ...}} for failed items."""
    try:
        r = requests.get(f'{SABNZBD_URL}/api', params={
            'mode': 'history', 'apikey': SABNZBD_KEY, 'output': 'json', 'limit': 500,
        }, timeout=15)
        return {s['nzo_id']: {
                    'name':         s.get('name', ''),
                    'storage':      s.get('storage', ''),
                    'fail_message': s.get('fail_message', ''),
                }
                for s in r.json().get('history', {}).get('slots', [])
                if s.get('status') == 'Failed'}
    except Exception:
        return {}

def _delete_sabnzbd_history(nzo_id):
    """Remove a job from SABnzbd history."""
    try:
        requests.get(f'{SABNZBD_URL}/api', params={
            'mode': 'history', 'name': 'delete', 'value': nzo_id,
            'apikey': SABNZBD_KEY, 'output': 'json',
        }, timeout=10)
    except Exception:
        pass

def whisparr_movie_id_for_code(code):
    """Return (movie_id, path) for an unimported Whisparr movie matching code, or None."""
    try:
        conn = db_connect()
        row = conn.execute('''
            SELECT m.Id, m.Path FROM Movies m
            JOIN MovieMetadata mm ON m.MovieMetadataId = mm.Id
            WHERE mm.Code = ? AND m.MovieFileId = 0
            LIMIT 1
        ''', (code,)).fetchone()
        conn.close()
        return (row['Id'], row['Path']) if row else None
    except Exception:
        return None

def whisparr_has_file(movie_id):
    """Return True if Whisparr already has a file for this movie."""
    try:
        conn = db_connect()
        row = conn.execute('SELECT MovieFileId FROM Movies WHERE Id = ?', (movie_id,)).fetchone()
        conn.close()
        return bool(row and row['MovieFileId'])
    except Exception:
        return False

def whisparr_existing_file_size(movie_id):
    """Return size in bytes of Whisparr's existing file for this movie, or 0."""
    try:
        conn = db_connect()
        row = conn.execute('''
            SELECT mf.Size FROM Movies m
            JOIN MovieFiles mf ON mf.Id = m.MovieFileId
            WHERE m.Id = ?
        ''', (movie_id,)).fetchone()
        conn.close()
        return int(row['Size']) if row and row['Size'] else 0
    except Exception:
        return 0

def whisparr_code_has_file(code):
    """Return True if any Whisparr movie with this code already has a file."""
    try:
        conn = db_connect()
        row = conn.execute('''
            SELECT m.MovieFileId FROM Movies m
            JOIN MovieMetadata mm ON m.MovieMetadataId = mm.Id
            WHERE mm.Code = ? AND m.MovieFileId != 0
            LIMIT 1
        ''', (code,)).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False

def get_sabnzbd_downloads_dir():
    """Return SABnzbd's completed downloads directory path."""
    try:
        r = requests.get(f'{SABNZBD_URL}/api', params={
            'mode': 'get_config', 'section': 'misc', 'apikey': SABNZBD_KEY, 'output': 'json',
        }, timeout=10)
        return r.json().get('config', {}).get('misc', {}).get('complete_dir', '')
    except Exception:
        return ''

def _cleanup_downloads_dir():
    """Delete download folders that are empty or belong to already-imported movies."""
    dl_dir = get_sabnzbd_downloads_dir()
    if not dl_dir:
        return
    root = Path(dl_dir)
    if not root.exists():
        return
    import shutil
    for d in root.iterdir():
        if not d.is_dir():
            continue
        # Strip trailing .N duplicate suffix to get the base code
        code = re.sub(r'\.\d+$', '', d.name).upper()
        if not CODE_RE.match(code):
            continue
        # Empty folder — delete it
        if not any(d.rglob('*')):
            try:
                d.rmdir()
                log.info('cleanup: removed empty dir %s', d.name)
            except Exception:
                pass
            continue
        # Movie already imported in Whisparr — dupe download, delete it
        if whisparr_code_has_file(code):
            try:
                shutil.rmtree(d)
                log.info('cleanup: removed dupe download %s (already imported)', d.name)
            except Exception as e:
                log.warning('cleanup: failed to remove %s: %s', d.name, e)

_TUBE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
}

# Domains confirmed working with yt-dlp — used to validate manual URLs too
YTDLP_SUPPORTED_DOMAINS = {
    'tnaflix.com', 'porntrex.com', 'xvideos.com', 'xnxx.com',
    'xhamster.com', 'eporner.com', 'pornhub.com', 'redtube.com',
    'tube8.com', 'youporn.com', 'spankbang.com',
}

def _tube_domain(url):
    m = re.search(r'https?://(?:www\.)?([^/]+)', url)
    return m.group(1).lower() if m else ''

def search_tube(code):
    """Search tnaflix and porntrex for code; return yt-dlp-compatible video page URLs."""
    urls = []
    seen = set()

    # tnaflix
    try:
        r = requests.get('https://www.tnaflix.com/search.php',
                         params={'what': code},
                         headers={**_TUBE_HEADERS, 'Referer': 'https://www.tnaflix.com/'},
                         timeout=10)
        raw = re.findall(r'href="(https://www\.tnaflix\.com/[^"]+/video\d+[^"]*)"', r.text)
        for u in raw:
            u = u.split('"')[0]
            if u not in seen and code.upper() in u.upper():
                seen.add(u)
                urls.append(u)
    except Exception as e:
        log.debug('search_tube tnaflix error: %s', e)

    # porntrex
    try:
        r = requests.get('https://www.porntrex.com/search/',
                         params={'search_query': code},
                         headers={**_TUBE_HEADERS, 'Referer': 'https://www.porntrex.com/'},
                         timeout=10)
        raw = re.findall(r'href="(https://www\.porntrex\.com/video/[^"]+)"', r.text)
        for u in raw:
            u = u.split('"')[0]
            if u not in seen and code.upper() in u.upper():
                seen.add(u)
                urls.append(u)
    except Exception as e:
        log.debug('search_tube porntrex error: %s', e)

    return urls[:8]

def _extract_lp_scenes(html):
    """Extract {CODE: title_string} from an LP-family page HTML blob (LP and Analvids)."""
    scenes = {}

    # ── LegalPorno: code at START of URL slug (/scene/CODE-rest-of-title)
    for m in re.finditer(
        r'href="[^"]*?/scene/([A-Za-z]{1,4}\d{2,6})-([^"?]{5,200})"', html, re.IGNORECASE
    ):
        code  = m.group(1).upper()
        title = f'{code} {m.group(2).replace("-", " ").strip()}'
        if CODE_RE.match(code) and code not in _CODE_EXCLUDE:
            scenes.setdefault(code, title)

    # ── LegalPorno: code at START of title/h2/strong text
    for pat in (
        r'<h2[^>]*>\s*([A-Za-z]{1,4}\d{2,6}\s[^<]{4,200}?)\s*</h2>',
        r'<strong[^>]*>([A-Za-z]{1,4}\d{2,6}\s[^<]{4,200}?)</strong>',
        r'\btitle="([A-Za-z]{1,4}\d{2,6}\s[^"]{4,200}?)"',
    ):
        for m in re.finditer(pat, html, re.IGNORECASE):
            text = m.group(1).strip()
            cm   = re.match(r'^([A-Za-z]{1,4}\d{2,6})\b', text)
            if cm:
                code = cm.group(1).upper()
                if CODE_RE.match(code) and code not in _CODE_EXCLUDE:
                    scenes.setdefault(code, text)

    # ── Analvids: code at END of title attr ("Full Scene Title CODE")
    for m in re.finditer(r'title="([^"]{5,400}\s([A-Za-z]{1,4}\d{2,6}))"', html):
        title = m.group(1).strip()
        code  = m.group(2).upper()
        if CODE_RE.match(code) and code not in _CODE_EXCLUDE:
            scenes.setdefault(code, title)

    # ── Analvids: code at END of /watch/ID/slug_CODE URL
    for m in re.finditer(r'/watch/\d+/[^\s"]*[_-]([A-Za-z]{1,4}\d{2,6})"', html):
        code = m.group(1).upper()
        if CODE_RE.match(code) and code not in _CODE_EXCLUDE:
            scenes.setdefault(code, code)  # title already captured above if present

    return scenes


def _scrape_lp_performer(base_url):
    """Scrape an LP-family performer page (LP or Analvids) and return {CODE: title} dict."""
    scenes  = {}
    headers = {
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer':         base_url,
    }
    # Analvids uses ?page=N; LP also uses ?page=N
    for page in range(1, 51):
        purl = f'{base_url}?page={page}' if page > 1 else base_url
        try:
            r = requests.get(purl, headers=headers, timeout=20)
            if not r.ok:
                log.warning('LP scraper page %d: HTTP %s', page, r.status_code)
                break
        except Exception as e:
            log.warning('LP scraper page %d: %s', page, e)
            break
        new    = _extract_lp_scenes(r.text)
        before = len(scenes)
        for k, v in new.items():
            scenes.setdefault(k, v)
        if not new or len(scenes) == before:
            break
        if page > 1:
            time.sleep(1.5)
    log.info('LP scraper: %d scenes from %s', len(scenes), base_url)
    return scenes


def _ytdlp_worker(code, urls, dl_dir):
    """Try each URL with yt-dlp; download >=480p to dl_dir. Runs in background thread."""
    import yt_dlp
    dl_dir = Path(dl_dir)
    try:
        dl_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.warning('ytdlp %s: cannot create %s: %s', code, dl_dir, e)
        return False
    ydl_opts = {
        'format': f'bestvideo[height>={YTDLP_MIN_RES}]+bestaudio/best[height>={YTDLP_MIN_RES}]/best',
        'outtmpl': str(dl_dir / '%(title)s.%(ext)s'),
        'quiet': True,
        'no_warnings': True,
    }
    for url in urls:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            if find_video_file(str(dl_dir)):
                log.info('ytdlp %s: downloaded from %s', code, url)
                return True
        except Exception as e:
            log.warning('ytdlp %s: failed %s — %s', code, url, e)
    log.warning('ytdlp %s: all URLs failed', code)
    return False

def get_sabnzbd_queue():
    try:
        r = requests.get(f'{SABNZBD_URL}/api', params={
            'mode': 'queue', 'apikey': SABNZBD_KEY, 'output': 'json',
        }, timeout=10)
        return r.json().get('queue', {}).get('slots', [])
    except Exception:
        return []

def find_video_file(storage):
    p = Path(storage)
    if not p.exists():
        return None
    if p.is_file() and p.suffix.lower() in VIDEO_EXTS:
        return str(p)
    videos = sorted([f for f in p.rglob('*') if f.suffix.lower() in VIDEO_EXTS],
                    key=lambda f: f.stat().st_size, reverse=True)
    return str(videos[0]) if videos else None

def whisparr_manual_import(file_path, movie_id, _log_entry=None, force=False):
    # Guard: if Whisparr already has a file, upgrade if download is bigger, else clean up source
    if not force and whisparr_has_file(movie_id):
        dl_size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
        wh_size = whisparr_existing_file_size(movie_id)
        if dl_size > wh_size * 1.1:
            log.info('upgrading movie %s: download %.1fGB > existing %.1fGB — forcing import',
                     movie_id, dl_size/1e9, wh_size/1e9)
            force = True
        else:
            log.info('import skipped for movie %s — already has file: %s', movie_id, file_path)
            src_dir = Path(file_path).parent
            try:
                if src_dir.exists():
                    import shutil; shutil.rmtree(src_dir)
                    log.info('cleaned up source dir (already had file): %s', src_dir)
            except Exception as e:
                log.warning('cleanup of %s failed: %s', src_dir, e)
            return True

    fname  = Path(file_path).name
    folder = str(Path(file_path).parent)
    item   = None
    try:
        r = requests.get(f'{WHISPARR_URL}/api/v3/manualimport',
            params={'folder': folder, 'filterExistingFiles': 'false'},
            headers={'X-Api-Key': WHISPARR_KEY}, timeout=30)
        for c in r.json():
            if c.get('path') == file_path:
                item = c
                break
        log.info('manualimport probe: %s → %s', fname, 'matched' if item else 'no match, using fallback')
    except Exception as e:
        log.warning('manualimport probe failed for %s: %s', fname, e)
    if item:
        item['movieId'] = movie_id
        item.pop('movie', None)
        item.pop('rejections', None)
    else:
        item = {'path': file_path, 'movieId': movie_id,
                'quality': {'quality': {'id': 1, 'name': 'Unknown'}, 'revision': {'version': 1, 'real': 0}},
                'languages': [{'id': 1, 'name': 'English'}],
                'indexerFlags': 0}
    try:
        r = requests.post(f'{WHISPARR_URL}/api/v3/command',
            headers={'X-Api-Key': WHISPARR_KEY, 'Content-Type': 'application/json'},
            json={'name': 'ManualImport', 'files': [item], 'importMode': 'move'},
            timeout=30)
        log.info('manualimport command: %s → HTTP %s', fname, r.status_code)
        if r.status_code == 201:
            cmd_id = r.json().get('id', '?')
            log.info('manualimport queued: %s (cmd %s, movie %s)', fname, cmd_id, movie_id)
            src_dir = Path(file_path).parent
            file_size_gb = Path(file_path).stat().st_size / 1e9 if Path(file_path).exists() else 0
            # Poll until confirmed: wait ~1 min per GB, minimum 3 min, maximum 30 min
            poll_total = max(180, min(int(file_size_gb * 60), 1800))
            def _rescan(log_entry):
                deadline = time.time() + poll_total
                confirmed = False
                fired_rescan = False
                while time.time() < deadline:
                    time.sleep(30)
                    if whisparr_has_file(movie_id):
                        confirmed = True
                        break
                    # Fire rescan once after first 3 minutes
                    if not fired_rescan and (deadline - time.time()) < (poll_total - 180):
                        try:
                            requests.post(f'{WHISPARR_URL}/api/v3/command',
                                headers={'X-Api-Key': WHISPARR_KEY, 'Content-Type': 'application/json'},
                                json={'name': 'RescanMovie', 'movieId': movie_id}, timeout=10)
                            log.info('rescan fired for movie %s', movie_id)
                            fired_rescan = True
                        except Exception as e:
                            log.warning('rescan failed for movie %s: %s', movie_id, e)
                try:
                    if src_dir.exists() and not any(
                        f.suffix.lower() in VIDEO_EXTS for f in src_dir.rglob('*') if f.is_file()
                    ):
                        import shutil
                        shutil.rmtree(src_dir)
                        log.info('cleaned up source dir %s', src_dir)
                except Exception as e:
                    log.warning('cleanup of %s failed: %s', src_dir, e)
                if log_entry is not None:
                    log_entry['status'] = 'imported' if confirmed else 'failed'
                    log.info('import confirmed for movie %s: %s', movie_id, 'yes' if confirmed else 'NO')
            threading.Thread(target=_rescan, daemon=True,
                             args=(_log_entry,)).start()
            return True
        log.warning('manualimport rejected by Whisparr: %s HTTP %s — %s', fname, r.status_code, r.text[:200])
        return False
    except Exception:
        return False

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    active = get_active_studios()
    try:
        performers = get_performers(active) if active else []
    except Exception as e:
        log.warning('index: DB error — %s', e)
        performers = []
    return render_template('index.html', performers=performers, active_studios=active)

@app.route('/performer/<name>')
def performer(name):
    active = get_active_studios()
    try:
        scenes = get_scenes(name, active)
    except Exception as e:
        log.warning('performer page %s: DB error — %s', name, e)
        scenes = []
    have    = sum(1 for s in scenes if s['status'] == 'have')
    missing = sum(1 for s in scenes if s['status'] == 'missing')
    queued  = sum(1 for s in scenes if s['status'] == 'queued')
    return render_template('performer.html', name=name, scenes=scenes,
                           have=have, missing=missing, queued=queued)

@app.route('/studios')
def studios():
    all_studios = get_all_studios()
    active      = get_active_studios()
    return render_template('studios.html', all_studios=all_studios, active=active)

@app.route('/api/studios', methods=['POST'])
def api_studios():
    selected = request.get_json().get('studios', [])
    cfg = load_config()
    cfg['active_studios'] = selected
    save_config(cfg)
    return jsonify({'ok': True, 'count': len(selected)})

@app.route('/hunt', methods=['POST'])
def hunt():
    items = request.get_json().get('items', [])
    def generate():
        state = load_state()
        for item in items:
            code  = item['code'].upper()
            wid   = item['wid']
            title = item['title']
            yield f"data: {json.dumps({'code': code, 'status': 'searching'})}\n\n"
            nzb_results, torrent_results = search_prowlarr(code)
            if not nzb_results and not torrent_results:
                if code not in state['not_found']:
                    state['not_found'].append(code)
                save_state(state)
                yield f"data: {json.dumps({'code': code, 'status': 'not_found'})}\n\n"
                continue
            best = (nzb_results or torrent_results)[0]
            mb = best['size'] // 1024 // 1024 if best['size'] else 0
            yield f"data: {json.dumps({'code': code, 'status': 'found', 'release': best['title'][:70], 'mb': mb})}\n\n"
            # Try every NZB in size order, then every torrent
            nzo_id = client = result = None
            for candidate in nzb_results:
                nzo_id = add_to_sabnzbd(candidate['url'], code)
                if nzo_id:
                    client = 'sabnzbd'
                    result = candidate
                    break
            if not nzo_id:
                for candidate in torrent_results:
                    nzo_id = add_to_qbittorrent(candidate['url'], code)
                    if nzo_id:
                        client = 'qbittorrent'
                        result = candidate
                        break
            if nzo_id:
                state['queued'][code] = {'nzo_id': nzo_id, 'wid': wid, 'title': title,
                                         'release': result['title'], 'client': client}
                save_state(state)
                yield f"data: {json.dumps({'code': code, 'status': 'queued', 'nzo_id': nzo_id, 'client': client})}\n\n"
            else:
                if code not in state['failed']:
                    state['failed'].append(code)
                save_state(state)
                yield f"data: {json.dumps({'code': code, 'status': 'failed'})}\n\n"
        yield f"data: {json.dumps({'done': True})}\n\n"
    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'})

@app.route('/api/sync', methods=['POST'])
def api_sync():
    state  = load_state()
    conn   = db_connect()
    synced = []

    all_codes = list(state['queued'].keys()) + list(state['imported'])
    for code in all_codes:
        row = conn.execute('''
            SELECT m.MovieFileId FROM Movies m
            JOIN MovieMetadata mm ON mm.Id = m.MovieMetadataId
            WHERE mm.Code = ?
        ''', [code]).fetchone()
        if row and row['MovieFileId']:
            if code in state['queued']:
                del state['queued'][code]
                if code not in state['imported']:
                    state['imported'].append(code)
                synced.append(code)
            # already in imported — leave it

    # Clear not_found/failed entries where Whisparr now has the file
    def _has_file(code):
        row = conn.execute('''SELECT m.MovieFileId FROM Movies m
            JOIN MovieMetadata mm ON mm.Id=m.MovieMetadataId WHERE mm.Code=?''', [code]).fetchone()
        return bool(row and row['MovieFileId'])
    state['not_found'] = [c for c in state['not_found'] if not _has_file(c)]
    state['failed']    = [c for c in state.get('failed', []) if not _has_file(c)]

    conn.close()
    if synced:
        save_state(state)
    return jsonify({'ok': True, 'synced': synced, 'count': len(synced)})

@app.route('/queue')
def queue_page():
    state     = load_state()
    completed = get_sabnzbd_completed()
    active_q  = get_sabnzbd_queue()
    rows = []
    for code, info in state['queued'].items():
        nzo  = info['nzo_id']
        done = nzo in completed
        slot = next((s for s in active_q if s.get('nzo_id') == nzo), None)
        rows.append({'code': code, 'title': info['title'], 'release': info.get('release', ''),
                     'nzo_id': nzo, 'done': done, 'client': info.get('client', 'sabnzbd'),
                     'percentage': slot.get('percentage', '0') if slot else ('100' if done else '0')})
    blacklisted = state.get('blacklisted_releases', {})
    return render_template('queue.html', rows=rows,
                           imported=state['imported'], not_found=state['not_found'],
                           blacklisted=blacklisted,
                           local_imports=list(_local_import_log))

@app.route('/import', methods=['POST'])
def do_import():
    state     = load_state()
    completed = get_sabnzbd_completed()
    results   = []
    for code, info in list(state['queued'].items()):
        nzo_id = info['nzo_id']
        if nzo_id not in completed:
            continue
        storage = completed[nzo_id]
        video   = find_video_file(storage)
        if not video:
            results.append({'code': code, 'ok': False, 'msg': f'No video in {storage}'})
            continue
        ok = whisparr_manual_import(video, info['wid'])
        if ok:
            state['imported'].append(code)
            del state['queued'][code]
            save_state(state)
            results.append({'code': code, 'ok': True, 'msg': Path(video).name})
        else:
            results.append({'code': code, 'ok': False, 'msg': 'Whisparr import failed'})
    return jsonify(results)

@app.route('/thumb/<int:wid>')
def thumb(wid):
    for cover in ('screenshot', 'poster'):
        try:
            r = requests.get(f'{WHISPARR_URL}/api/v3/mediacover/{wid}/{cover}.jpg',
                             headers={'X-Api-Key': WHISPARR_KEY}, timeout=8)
            if r.ok:
                return Response(r.content, content_type=r.headers.get('Content-Type', 'image/jpeg'))
        except Exception:
            pass
    return '', 404

@app.route('/headshot/<int:performer_id>')
def headshot(performer_id):
    try:
        r = requests.get(f'{WHISPARR_URL}/MediaCover/performer/{performer_id}/headshot.jpg',
                         headers={'X-Api-Key': WHISPARR_KEY}, timeout=8)
        if r.ok:
            return Response(r.content, content_type=r.headers.get('Content-Type', 'image/jpeg'))
    except Exception:
        pass
    return '', 404

@app.route('/api/test', methods=['POST'])
def api_test():
    data    = request.get_json()
    service = data.get('service')
    url     = (data.get('url') or '').rstrip('/')
    key     = data.get('key') or ''
    try:
        if service == 'whisparr':
            r = requests.get(f'{url}/api/v3/system/status',
                             headers={'X-Api-Key': key}, timeout=8)
            if r.ok:
                ver = r.json().get('version', '?')
                return jsonify({'ok': True, 'msg': f'v{ver}'})
            return jsonify({'ok': False, 'msg': f'HTTP {r.status_code}'})
        elif service == 'prowlarr':
            r = requests.get(f'{url}/api/v1/system/status',
                             headers={'X-Api-Key': key}, timeout=8)
            if r.ok:
                ver = r.json().get('version', '?')
                return jsonify({'ok': True, 'msg': f'v{ver}'})
            return jsonify({'ok': False, 'msg': f'HTTP {r.status_code}'})
        elif service == 'sabnzbd':
            r = requests.get(f'{url}/api', params={
                'mode': 'version', 'apikey': key, 'output': 'json'}, timeout=8)
            if r.ok:
                ver = r.json().get('version', '?')
                return jsonify({'ok': True, 'msg': f'v{ver}'})
            return jsonify({'ok': False, 'msg': f'HTTP {r.status_code}'})
        elif service == 'qbittorrent':
            user = data.get('user', '')
            s = requests.Session()
            r = s.post(f'{url}/api/v2/auth/login',
                       data={'username': user, 'password': key}, timeout=8)
            if r.text.strip() == 'Ok.':
                ver = s.get(f'{url}/api/v2/app/version', timeout=8).text.strip()
                return jsonify({'ok': True, 'msg': f'v{ver}'})
            return jsonify({'ok': False, 'msg': 'Auth failed'})
        return jsonify({'ok': False, 'msg': 'Unknown service'})
    except requests.exceptions.ConnectionError:
        return jsonify({'ok': False, 'msg': 'Connection refused'})
    except Exception as e:
        return jsonify({'ok': False, 'msg': str(e)[:60]})

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    cfg = load_config()
    if request.method == 'POST':
        data = request.get_json()
        for key in ('whisparr_url', 'whisparr_key', 'prowlarr_url', 'prowlarr_key',
                    'sabnzbd_url', 'sabnzbd_key',
                    'qbittorrent_url', 'qbittorrent_user', 'qbittorrent_pass',
                    'performer_tag', 'auto_snatch', 'snatch_interval_h', 'retry_not_found_d',
                    'ytdlp_enabled', 'ytdlp_min_res', 'ytdlp_dl_dir', 'backup_roots'):
            if key in data:
                cfg[key] = data[key]
        save_config(cfg)
        _reload_config(cfg)
        return jsonify({'ok': True})
    return render_template('settings.html',
        build_sha=BUILD_SHA,
        whisparr_url=cfg.get('whisparr_url', WHISPARR_URL),
        whisparr_key=cfg.get('whisparr_key', WHISPARR_KEY),
        prowlarr_url=cfg.get('prowlarr_url', PROWLARR_URL),
        prowlarr_key=cfg.get('prowlarr_key', PROWLARR_KEY),
        sabnzbd_url=cfg.get('sabnzbd_url', SABNZBD_URL),
        sabnzbd_key=cfg.get('sabnzbd_key', SABNZBD_KEY),
        qbittorrent_url=cfg.get('qbittorrent_url', QBITTORRENT_URL),
        qbittorrent_user=cfg.get('qbittorrent_user', QBITTORRENT_USER),
        qbittorrent_pass=cfg.get('qbittorrent_pass', QBITTORRENT_PASS),
        performer_tag=cfg.get('performer_tag', ''),
        auto_snatch=cfg.get('auto_snatch', _DEFAULTS['auto_snatch']),
        snatch_interval_h=cfg.get('snatch_interval_h', _DEFAULTS['snatch_interval_h']),
        retry_not_found_d=cfg.get('retry_not_found_d', _DEFAULTS['retry_not_found_d']),
        ytdlp_enabled=cfg.get('ytdlp_enabled', _DEFAULTS['ytdlp_enabled']),
        ytdlp_min_res=cfg.get('ytdlp_min_res', _DEFAULTS['ytdlp_min_res']),
        ytdlp_dl_dir=cfg.get('ytdlp_dl_dir', _DEFAULTS['ytdlp_dl_dir']),
        backup_roots=cfg.get('backup_roots', _DEFAULTS['backup_roots']),
    )

@app.route('/logs')
def logs_page():
    return render_template('logs.html', lines=list(_ring.records))

@app.route('/api/snatch-now', methods=['POST'])
def api_snatch_now():
    threading.Thread(target=_run_auto_snatch, daemon=True, name='snatch-now').start()
    return jsonify({'ok': True, 'msg': 'Snatch run started in background'})

@app.route('/api/wipe-queue', methods=['POST'])
def api_wipe_queue():
    state = load_state()
    wiped = len(state.get('queued', {}))
    state['queued'] = {}
    save_state(state)
    return jsonify({'ok': True, 'wiped': wiped})

@app.route('/api/clear-blacklist', methods=['POST'])
def api_clear_blacklist():
    """Clear all or a specific code's blacklisted releases."""
    code  = (request.json or {}).get('code', '').upper().strip()
    state = load_state()
    if code:
        removed = len(state['blacklisted_releases'].pop(code, []))
    else:
        removed = sum(len(v) for v in state['blacklisted_releases'].values())
        state['blacklisted_releases'] = {}
    save_state(state)
    return jsonify({'ok': True, 'removed': removed})

@app.route('/manual/<code>')
def manual_page(code):
    code = code.upper()
    try:
        conn = db_connect()
        row = conn.execute('''
            SELECT m.Id as wid, mm.Title, mm.StudioTitle
            FROM Movies m
            JOIN MovieMetadata mm ON m.MovieMetadataId = mm.Id
            WHERE mm.Code = ? AND m.Monitored = 1
            LIMIT 1
        ''', (code,)).fetchone()
        if not row:
            conn.close()
            return f'Scene {code} not found in Whisparr', 404
        wid = row['wid']
        performers = conn.execute('''
            SELECT p.Name FROM Credits c
            JOIN Performers p ON p.ForeignId = c.PerformerForeignId
            JOIN Movies m ON m.MovieMetadataId = c.MovieMetadataId
            WHERE m.Id = ? AND p.Monitored = 1
        ''', (wid,)).fetchall()
        conn.close()
        performer_names = [p['Name'] for p in performers]
    except Exception as e:
        log.warning('manual_page %s: %s', code, e)
        return f'DB error: {e}', 500
    return render_template('manual.html', code=code, wid=wid,
                           title=row['Title'] or code,
                           studio=row['StudioTitle'] or '',
                           performer_names=performer_names)

@app.route('/api/manual-import', methods=['POST'])
def api_manual_import():
    data = request.get_json()
    code      = (data.get('code') or '').upper()
    wid       = data.get('wid')
    url       = (data.get('url') or '').strip()
    file_path = (data.get('file_path') or '').strip()
    title     = (data.get('title') or code)

    if not code or not wid:
        return jsonify({'ok': False, 'msg': 'code and wid required'})

    state = load_state()

    if url:
        domain = _tube_domain(url)
        if not any(domain == d or domain.endswith('.' + d) for d in YTDLP_SUPPORTED_DOMAINS):
            return jsonify({'ok': False,
                            'msg': f'{domain} is not supported by yt-dlp — try a different site'})
        dl_base = YTDLP_DL_DIR or get_sabnzbd_downloads_dir() or str(_DATA_DIR / 'ytdlp')
        dl_dir  = Path(dl_base) / code
        state['queued'][code] = {
            'nzo_id':        f'ytdlp_{code}',
            'wid':           wid,
            'title':         title,
            'release':       url[:80],
            'client':        'ytdlp',
            'dl_path':       str(dl_dir),
            'ytdlp_url':     url,
            'ytdlp_retries': 0,
            'started_at':    time.time(),
        }
        if code in state['not_found']:
            state['not_found'].remove(code)
        save_state(state)
        threading.Thread(target=_ytdlp_worker, args=(code, [url], dl_dir),
                         daemon=True, name=f'ytdlp-{code}').start()
        log.info('manual-import %s: yt-dlp queued — %s', code, url[:60])
        return jsonify({'ok': True, 'msg': f'yt-dlp download started for {code}'})

    if file_path:
        p = Path(file_path)
        if not p.exists():
            return jsonify({'ok': False, 'msg': f'File not found: {file_path}'})
        if p.suffix.lower() not in VIDEO_EXTS:
            return jsonify({'ok': False, 'msg': f'Not a recognised video file: {p.suffix}'})
        ok = whisparr_manual_import(file_path, wid)
        if ok:
            if code in state['not_found']:
                state['not_found'].remove(code)
            if code not in state['imported']:
                state['imported'].append(code)
            save_state(state)
            log.info('manual-import %s: imported from %s', code, file_path)
            return jsonify({'ok': True, 'msg': f'Imported {p.name}'})
        return jsonify({'ok': False, 'msg': 'Whisparr import failed — check logs'})

    return jsonify({'ok': False, 'msg': 'Provide a URL or file path'})

def _auto_snatch_loop():
    try:
        _run_auto_snatch()
    except Exception:
        log.exception('auto-snatch startup run error')
    while True:
        time.sleep(SNATCH_INTERVAL_H * 3600)
        if not AUTO_SNATCH:
            continue
        try:
            _run_auto_snatch()
        except Exception:
            log.exception('auto-snatch loop error')

def _run_auto_snatch():
    if not _snatch_running.acquire(blocking=False):
        log.info('auto-snatch: already running, skipping')
        return
    try:
        _run_auto_snatch_inner()
    finally:
        _snatch_running.release()

def _run_auto_snatch_inner():
    active   = get_active_studios()
    if not active:
        return
    state    = load_state()
    queued   = set(state['queued'].keys())
    imported = set(state['imported'])
    nf_codes = set(state['not_found'])
    nf_at    = state['not_found_at']
    retry_cutoff = time.time() - RETRY_NOT_FOUND_D * 86400

    conn = db_connect()
    placeholders = ','.join('?' * len(active))
    studios = list(active)

    # Get all monitored performers
    if PERFORMER_TAG:
        try:
            tag_row = conn.execute("SELECT Id FROM Tags WHERE Label=?", [PERFORMER_TAG]).fetchone()
            tag_id  = tag_row['Id'] if tag_row else None
        except Exception:
            tag_id = None
        perf_q = ("SELECT Id, ForeignId, Name FROM Performers WHERE Monitored=1 AND Tags LIKE ? ORDER BY Name",
                  [f'%{tag_id}%'] if tag_id else [])
        all_p = conn.execute(*perf_q).fetchall() if tag_id else \
                conn.execute('SELECT Id, ForeignId, Name FROM Performers WHERE Monitored=1').fetchall()
    else:
        all_p = conn.execute('SELECT Id, ForeignId, Name FROM Performers WHERE Monitored=1').fetchall()

    changed = False
    for p in all_p:
        rows = conn.execute(f'''
            SELECT m.Id as wid, m.MovieFileId, mm.Code, mm.Title
            FROM Credits c
            JOIN MovieMetadata mm ON mm.Id = c.MovieMetadataId
            JOIN Movies m ON m.MovieMetadataId = mm.Id
            WHERE c.PerformerForeignId = ?
              AND mm.StudioTitle IN ({placeholders})
              AND mm.Code IS NOT NULL AND m.Monitored = 1
        ''', [p['ForeignId']] + studios).fetchall()

        for r in rows:
            code = str(r['Code'] or '').strip().upper()
            if not CODE_RE.match(code):
                continue
            if r['MovieFileId'] or code in queued or code in imported:
                continue
            # Eligible for snatch — unless recently not_found
            if code in nf_codes:
                last = nf_at.get(code, 0)
                if isinstance(last, str):
                    from datetime import datetime, timezone
                    last = datetime.fromisoformat(last).timestamp()
                if last > retry_cutoff:
                    continue   # too recent, skip
                # Old enough — remove from not_found and retry
                state['not_found'].remove(code)
                nf_codes.discard(code)

            log.info('auto-snatch: searching %s (%s)', code, p['Name'])
            nzb_results, torrent_results = search_prowlarr(code)
            if not nzb_results and not torrent_results:
                urls = search_tube(code) if YTDLP_ENABLED else []
                if urls:
                    dl_base = YTDLP_DL_DIR or get_sabnzbd_downloads_dir() or str(_DATA_DIR / 'ytdlp')
                    dl_dir  = Path(dl_base) / code
                    state['queued'][code] = {
                        'nzo_id':       f'ytdlp_{code}',
                        'wid':          r['wid'],
                        'title':        r['Title'] or code,
                        'release':      urls[0][:80],
                        'client':       'ytdlp',
                        'dl_path':      str(dl_dir),
                        'ytdlp_url':    urls[0],
                        'ytdlp_retries': 0,
                        'started_at':   time.time(),
                    }
                    queued.add(code)
                    changed = True
                    threading.Thread(target=_ytdlp_worker, args=(code, urls, dl_dir),
                                     daemon=True, name=f'ytdlp-{code}').start()
                    log.info('auto-snatch: %s → ytdlp (%d tube URLs)', code, len(urls))
                else:
                    if code not in state['not_found']:
                        state['not_found'].append(code)
                    state['not_found_at'][code] = time.strftime('%Y-%m-%dT%H:%M:%S')
                    nf_codes.add(code)
                    changed = True
                    log.info('auto-snatch: %s not found anywhere', code)
                continue

            blacklisted = set(state.get('blacklisted_releases', {}).get(code, []))
            nzo_id = dl_client = result = None
            for candidate in nzb_results:
                if candidate['title'] in blacklisted:
                    log.info('auto-snatch %s: skipping blacklisted release — %s', code, candidate['title'][:60])
                    continue
                nzo_id = add_to_sabnzbd(candidate['url'], code)
                if nzo_id:
                    dl_client = 'sabnzbd'
                    result = candidate
                    break
            if not nzo_id:
                for candidate in torrent_results:
                    if candidate['title'] in blacklisted:
                        log.info('auto-snatch %s: skipping blacklisted torrent — %s', code, candidate['title'][:60])
                        continue
                    nzo_id = add_to_qbittorrent(candidate['url'], code)
                    if nzo_id:
                        dl_client = 'qbittorrent'
                        result = candidate
                        break
            if nzo_id:
                state['queued'][code] = {'nzo_id': nzo_id, 'wid': r['wid'],
                                         'title': r['Title'] or code,
                                         'release': result['title'], 'client': dl_client}
                queued.add(code)
                changed = True
                log.info('auto-snatch: queued %s via %s — %s', code, dl_client, result['title'][:60])
            else:
                urls = search_tube(code) if YTDLP_ENABLED else []
                if urls:
                    dl_base = YTDLP_DL_DIR or get_sabnzbd_downloads_dir() or str(_DATA_DIR / 'ytdlp')
                    dl_dir  = Path(dl_base) / code
                    state['queued'][code] = {
                        'nzo_id':        f'ytdlp_{code}',
                        'wid':           r['wid'],
                        'title':         r['Title'] or code,
                        'release':       urls[0][:80],
                        'client':        'ytdlp',
                        'dl_path':       str(dl_dir),
                        'ytdlp_url':     urls[0],
                        'ytdlp_retries': 0,
                        'started_at':    time.time(),
                    }
                    queued.add(code)
                    changed = True
                    threading.Thread(target=_ytdlp_worker, args=(code, urls, dl_dir),
                                     daemon=True, name=f'ytdlp-{code}').start()
                    log.info('auto-snatch: %s → ytdlp fallback (%d tube URLs)', code, len(urls))
                else:
                    log.warning('auto-snatch: all clients rejected %s, no tube results', code)
            time.sleep(2)

    conn.close()
    if changed:
        save_state(state)
    log.info('auto-snatch run complete')

# ── Local backup scanner ──────────────────────────────────────────────────────

def _load_whisparr_index():
    """One DB query → {CODE: (wid, title, has_file, existing_mb, monitored)} for all movies."""
    conn = db_connect()
    rows = conn.execute('''
        SELECT m.Id as wid, m.MovieFileId, m.Monitored, mm.Code, mm.Title,
               mf.Size as file_size
        FROM Movies m
        JOIN MovieMetadata mm ON mm.Id = m.MovieMetadataId
        LEFT JOIN MovieFiles mf ON mf.Id = m.MovieFileId
        WHERE mm.Code IS NOT NULL
    ''').fetchall()
    conn.close()
    return {r['Code'].upper(): (r['wid'], r['Title'], bool(r['MovieFileId']),
                                 round((r['file_size'] or 0) / 1_000_000),
                                 bool(r['Monitored']))
            for r in rows if r['Code']}

def _extract_code(path: Path):
    """Search all path components (innermost first) for a recognisable scene code."""
    for part in reversed(path.parts):
        for m in CODE_SEARCH_RE.finditer(part):
            candidate = m.group(1).upper()
            if CODE_RE.match(candidate) and candidate not in _CODE_EXCLUDE:
                return candidate
    return None

def _run_scan():
    global _scan_results, _scan_running, _scan_progress
    with _scan_lock:
        if _scan_running:
            return
        _scan_running  = True
        _scan_progress = {'done': 0, 'total': 0, 'status': 'Collecting files…'}
        _scan_results  = None

    try:
        # Walk backup roots and collect all video files
        all_videos = []
        for root in BACKUP_ROOTS:
            rpath = Path(root)
            for f in rpath.rglob('*'):
                if f.is_file() and f.suffix.lower() in VIDEO_EXTS:
                    all_videos.append((rpath, f))
        _scan_progress['total'] = len(all_videos)
        _scan_progress['status'] = f'Indexing {len(all_videos)} files…'

        # Build records for DB upsert
        records = []
        seen    = set()
        for i, (root, video) in enumerate(all_videos):
            _scan_progress['done'] = i + 1
            key = str(video)
            if key in seen:
                continue
            seen.add(key)
            try:
                rel  = video.relative_to(root)
                perf = rel.parts[0] if rel.parts else '?'
                code = _extract_code(video)
                size = round(video.stat().st_size / 1_000_000)
                records.append((key, perf, str(root), code, size))
            except Exception:
                pass

        # Persist to DB and remove stale entries
        _scan_progress['status'] = 'Saving file index…'
        _scan_db_upsert(records)
        purged = _scan_db_purge_missing()
        if purged:
            log.info('scan: purged %d files no longer on disk', purged)

        # Compute status from current Whisparr state
        _scan_progress['status'] = 'Computing status from Whisparr…'
        results = _compute_scan_results()

        _scan_progress['status'] = 'Done'
        with _scan_lock:
            _scan_results = results
        log.info('scan complete: %d files, %d results', len(records), len(results))
    except Exception as e:
        log.exception('scan error')
        _scan_progress['status'] = f'Error: {e}'
    finally:
        with _scan_lock:
            _scan_running = False

@app.route('/scan')
def scan_page():
    return render_template('scan.html', backup_roots=BACKUP_ROOTS)

@app.route('/lp')
def lp_page():
    return render_template('lp.html')

@app.route('/api/scan-start', methods=['POST'])
def api_scan_start():
    global _scan_results
    with _scan_lock:
        if _scan_running:
            return jsonify({'ok': False, 'error': 'scan already running'})
        _scan_results = None
    threading.Thread(target=_run_scan, daemon=True, name='local-scan').start()
    return jsonify({'ok': True})

@app.route('/api/scan-progress')
def api_scan_progress():
    with _scan_lock:
        done    = _scan_results is not None
        running = _scan_running
    return jsonify({
        **_scan_progress,
        'running': running,
        'ready':   done,
    })

@app.route('/api/scan-results')
def api_scan_results():
    with _scan_lock:
        results = _scan_results
    if results is None:
        return jsonify({'error': 'no scan results yet'}), 404
    status_filter = request.args.get('status')
    if status_filter:
        results = [r for r in results if r['status'] == status_filter]
    return jsonify(results)

@app.route('/api/lp-performers')
def api_lp_performers():
    """List all performers with persisted LP scrape data."""
    conn = _scan_db_connect()
    rows = conn.execute(
        'SELECT url, label, scraped_at, scene_count FROM performer_scrapes ORDER BY label'
    ).fetchall()
    conn.close()
    now    = time.time()
    result = []
    for r in rows:
        age_s = int(now - r['scraped_at']) if r['scraped_at'] else None
        if age_s is None:       age_str = 'unknown'
        elif age_s < 3600:      age_str = f'{age_s // 60}m ago'
        elif age_s < 86400:     age_str = f'{age_s // 3600}h ago'
        else:                   age_str = f'{age_s // 86400}d ago'
        result.append({'url': r['url'], 'label': r['label'],
                       'scene_count': r['scene_count'], 'scraped_ago': age_str})
    return jsonify(result)


@app.route('/api/performer-scenes')
def api_performer_scenes():
    """Return full scene list for a given performer URL from local DB."""
    url = request.args.get('url', '').strip()
    if not url:
        return jsonify({'error': 'url param required'}), 400
    conn = _scan_db_connect()
    rows = conn.execute('''
        SELECT ps.code,
               COALESCE(s.title, s.lp_title, ps.code) as title,
               s.wid, s.has_file, s.monitored,
               bf.path as local_path, bf.size_mb,
               s.lp_title
        FROM performer_scenes ps
        LEFT JOIN scenes s        ON s.code  = ps.code
        LEFT JOIN backup_files bf ON bf.code = ps.code
        WHERE ps.url = ?
        ORDER BY ps.code
    ''', (url,)).fetchall()
    meta = conn.execute(
        'SELECT label, scraped_at FROM performer_scrapes WHERE url = ?', (url,)
    ).fetchone()
    conn.close()
    scenes = []
    for r in rows:
        has_file  = bool(r['has_file'])
        local     = r['local_path']
        wid       = r['wid']
        monitored = bool(r['monitored'])
        if has_file:                         status = 'imported'
        elif local and wid and monitored:    status = 'importable'
        elif local and wid and not monitored:status = 'unmonitored'
        elif local and not wid:              status = 'add_import'
        else:                                status = 'missing'
        scenes.append({
            'code': r['code'], 'title': r['title'], 'lp_title': r['lp_title'],
            'wid': wid, 'has_file': has_file, 'local_path': local,
            'size_mb': r['size_mb'], 'status': status,
        })
    return jsonify({
        'label':  meta['label'] if meta else url,
        'scenes': scenes,
    })


@app.route('/api/scan-db-info')
def api_scan_db_info():
    try:
        conn  = _scan_db_connect()
        count = conn.execute('SELECT COUNT(*) FROM backup_files').fetchone()[0]
        meta  = conn.execute("SELECT value FROM scan_meta WHERE key='last_scan'").fetchone()
        conn.close()
        last_scan = float(meta['value']) if meta else None
        age_s     = int(time.time() - last_scan) if last_scan else None
        if age_s is None:
            age_str = 'never'
        elif age_s < 120:
            age_str = 'just now'
        elif age_s < 3600:
            age_str = f'{age_s // 60}m ago'
        elif age_s < 86400:
            age_str = f'{age_s // 3600}h ago'
        else:
            age_str = f'{age_s // 86400}d ago'
        return jsonify({'count': count, 'last_scan': age_str, 'last_scan_ts': last_scan})
    except Exception as e:
        return jsonify({'count': 0, 'last_scan': 'unknown', 'error': str(e)})

@app.route('/api/import-local', methods=['POST'])
def api_import_local():
    d     = request.json or {}
    video = d.get('video')
    wid   = d.get('wid')
    if not video or not wid:
        return jsonify({'ok': False, 'error': 'missing video or wid'}), 400
    if not Path(video).is_file():
        return jsonify({'ok': False, 'error': 'file not found'}), 404
    force = bool(d.get('force', False))
    entry = {
        'title':  d.get('title') or Path(video).stem,
        'video':  video,
        'wid':    int(wid),
        'status': 'pending',
        'time':   time.strftime('%H:%M'),
        'action': 'replace' if force else 'import',
    }
    _local_import_log.appendleft(entry)
    ok = whisparr_manual_import(video, int(wid), _log_entry=entry, force=force)
    if not ok:
        entry['status'] = 'failed'
    return jsonify({'ok': ok})

@app.route('/api/delete-local', methods=['POST'])
def api_delete_local():
    """Delete a local dupe/unwanted file and clean up its directory if empty."""
    d     = request.json or {}
    video = d.get('video')
    if not video:
        return jsonify({'ok': False, 'error': 'missing video'}), 400
    p = Path(video)
    if not p.is_file():
        return jsonify({'ok': False, 'error': 'file not found'}), 404
    try:
        import shutil
        # Delete companion sidecar files (nfo, jpg, xml, txt) in the same dir
        for sidecar in p.parent.iterdir():
            if sidecar.is_file() and sidecar.suffix.lower() in {'.nfo','.jpg','.jpeg','.png','.xml','.txt'}:
                sidecar.unlink(missing_ok=True)
        p.unlink()
        # Remove dir if now empty
        try:
            p.parent.rmdir()
        except OSError:
            pass
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/local-import-log')
def api_local_import_log():
    return jsonify(list(_local_import_log))

@app.route('/api/add-and-import', methods=['POST'])
def api_add_and_import():
    """Look up a scene code in TPDB via Whisparr, add to library, then import the file."""
    d        = request.json or {}
    video    = d.get('video')
    code     = d.get('code', '').upper()
    lp_title = (d.get('lp_title') or '').strip()
    if not video or not code:
        return jsonify({'ok': False, 'error': 'missing video or code'}), 400
    if not Path(video).is_file():
        return jsonify({'ok': False, 'error': 'file not found'}), 404
    try:
        # Lookup: try code first, then LP title as fallback if provided
        results = []
        search_terms = [code]
        if lp_title and lp_title.upper() != code:
            search_terms.append(lp_title)
        for term in search_terms:
            r = requests.get(f'{WHISPARR_URL}/api/v3/movie/lookup',
                             params={'term': term},
                             headers={'X-Api-Key': WHISPARR_KEY}, timeout=15)
            results = r.json() if r.ok else []
            if results:
                break
        if not results:
            return jsonify({'ok': False, 'error': f'No TPDB results for {code}'}), 404
        movie = results[0]
        # Get root folder + quality profile from first existing movie
        conn = db_connect()
        existing = conn.execute('SELECT Path FROM Movies LIMIT 1').fetchone()
        conn.close()
        root_folder = str(Path(existing['Path']).parent) if existing else '/mnt/user/data/media/xxx'
        qp = requests.get(f'{WHISPARR_URL}/api/v3/qualityprofile',
                          headers={'X-Api-Key': WHISPARR_KEY}, timeout=10).json()
        quality_id = qp[0]['id'] if qp else 1
        # Add to library
        movie['rootFolderPath']   = root_folder
        movie['qualityProfileId'] = quality_id
        movie['monitored']        = True
        movie['addOptions']       = {'searchForMovie': False}
        r2 = requests.post(f'{WHISPARR_URL}/api/v3/movie',
                           headers={'X-Api-Key': WHISPARR_KEY, 'Content-Type': 'application/json'},
                           json=movie, timeout=15)
        if r2.status_code not in (200, 201):
            return jsonify({'ok': False, 'error': f'Add failed HTTP {r2.status_code}: {r2.text[:200]}'}), 500
        wid   = r2.json()['id']
        title = movie.get('title') or code
        log.info('add-and-import: added %s (wid %s) to Whisparr', code, wid)
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500
    entry = {
        'title':  title,
        'video':  video,
        'wid':    wid,
        'status': 'pending',
        'time':   time.strftime('%H:%M'),
        'action': 'add+import',
    }
    _local_import_log.appendleft(entry)
    ok = whisparr_manual_import(video, wid, _log_entry=entry)
    if not ok:
        entry['status'] = 'failed'
    return jsonify({'ok': ok, 'wid': wid, 'title': title})

@app.route('/api/monitor-and-import', methods=['POST'])
def api_monitor_and_import():
    """Enable monitoring on a Whisparr movie then import the backup file."""
    d     = request.json or {}
    video = d.get('video')
    wid   = d.get('wid')
    if not video or not wid:
        return jsonify({'ok': False, 'error': 'missing video or wid'}), 400
    if not Path(video).is_file():
        return jsonify({'ok': False, 'error': 'file not found'}), 404
    wid = int(wid)
    # Fetch current movie record from Whisparr
    try:
        r = requests.get(f'{WHISPARR_URL}/api/v3/movie/{wid}',
                         headers={'X-Api-Key': WHISPARR_KEY}, timeout=10)
        if not r.ok:
            return jsonify({'ok': False, 'error': f'Whisparr GET failed: {r.status_code}'}), 500
        movie = r.json()
        movie['monitored'] = True
        r2 = requests.put(f'{WHISPARR_URL}/api/v3/movie/{wid}',
                          headers={'X-Api-Key': WHISPARR_KEY, 'Content-Type': 'application/json'},
                          json=movie, timeout=10)
        if not r2.ok:
            return jsonify({'ok': False, 'error': f'Whisparr PUT failed: {r2.status_code}'}), 500
        log.info('monitor-and-import: enabled monitoring for wid %s', wid)
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500
    entry = {
        'title':  d.get('title') or Path(video).stem,
        'video':  video,
        'wid':    wid,
        'status': 'pending',
        'time':   time.strftime('%H:%M'),
        'action': 'monitor+import',
    }
    _local_import_log.appendleft(entry)
    ok = whisparr_manual_import(video, wid, _log_entry=entry)
    if not ok:
        entry['status'] = 'failed'
    return jsonify({'ok': ok})

@app.route('/api/scan-ignore', methods=['POST'])
def api_scan_ignore():
    """Permanently ignore a file: remove from DB + in-memory results + config."""
    global _scan_results
    d = request.json or {}
    video = d.get('video')
    if not video:
        return jsonify({'ok': False, 'error': 'missing video'}), 400
    # Remove from persistent file index
    try:
        conn = _scan_db_connect()
        conn.execute('DELETE FROM backup_files WHERE path = ?', (video,))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning('scan-ignore: db delete failed for %s: %s', video, e)
    # Remove from in-memory results immediately
    with _scan_lock:
        if _scan_results is not None:
            _scan_results = [r for r in _scan_results if r.get('video') != video]
    # Persist to config ignored list as a belt-and-suspenders safeguard
    cfg = load_config()
    ignored = cfg.get('ignored_paths', [])
    if video not in ignored:
        ignored.append(video)
        cfg['ignored_paths'] = ignored
        save_config(cfg)
        _reload_config(cfg)
    return jsonify({'ok': True})

@app.route('/api/scan-lp-performer', methods=['POST'])
def api_scan_lp_performer():
    """Scrape an LP performer page and cross-reference with current scan's not-in-library results."""
    d   = request.json or {}
    url = (d.get('url') or '').strip().rstrip('/')
    if not url or not url.startswith('http'):
        return jsonify({'ok': False, 'error': 'Enter a full URL starting with http'}), 400
    try:
        lp_scenes = _scrape_lp_performer(url)
    except Exception as e:
        log.warning('LP scraper error: %s', e)
        return jsonify({'ok': False, 'error': str(e)}), 500
    if not lp_scenes:
        return jsonify({'ok': False, 'error': 'No scenes found — check URL or try again'}), 404
    # Derive a human-readable label from the URL
    label_m = re.search(r'/(?:actress|model)/[^/]+/([^/?#]+)', url)
    label   = label_m.group(1).replace('_', ' ').replace('-', ' ').title() if label_m else url.split('/')[-1]

    # Persist scenes + performer links into scan.db
    now   = time.time()
    sconn = _scan_db_connect()
    for code, lp_title in lp_scenes.items():
        sconn.execute('''
            INSERT INTO scenes (code, lp_title, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                lp_title   = excluded.lp_title,
                updated_at = excluded.updated_at
        ''', (code, lp_title, now))
        sconn.execute('INSERT OR IGNORE INTO performer_scenes (url, code) VALUES (?, ?)',
                      (url, code))
    sconn.execute('''
        INSERT INTO performer_scrapes (url, label, scraped_at, scene_count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            label       = excluded.label,
            scraped_at  = excluded.scraped_at,
            scene_count = excluded.scene_count
    ''', (url, label, now, len(lp_scenes)))
    sconn.commit()
    sconn.close()

    with _scan_lock:
        scan_ran = _scan_results is not None

    # Cross-reference with local backup files
    sconn2 = _scan_db_connect()
    rows   = sconn2.execute('''
        SELECT ps.code,
               COALESCE(s.title, s.lp_title, ps.code) as title,
               s.wid, s.has_file, s.monitored,
               bf.path as local_path, bf.size_mb,
               s.lp_title
        FROM performer_scenes ps
        LEFT JOIN scenes s       ON s.code  = ps.code
        LEFT JOIN backup_files bf ON bf.code = ps.code
        WHERE ps.url = ?
        ORDER BY ps.code
    ''', (url,)).fetchall()
    sconn2.close()

    scenes_out = []
    for r in rows:
        has_file  = bool(r['has_file'])
        local     = r['local_path']
        wid       = r['wid']
        monitored = bool(r['monitored'])
        if has_file:
            status = 'imported'
        elif local and wid and monitored:
            status = 'importable'
        elif local and wid and not monitored:
            status = 'unmonitored'
        elif local and not wid:
            status = 'add_import'
        else:
            status = 'missing'
        scenes_out.append({
            'code':       r['code'],
            'title':      r['title'],
            'lp_title':   r['lp_title'],
            'wid':        wid,
            'has_file':   has_file,
            'local_path': local,
            'size_mb':    r['size_mb'],
            'status':     status,
        })

    return jsonify({
        'ok':       True,
        'label':    label,
        'lp_count': len(lp_scenes),
        'scan_ran': scan_ran,
        'scenes':   scenes_out,
    })


_WHISPARR_CONTAINER = os.environ.get('WHISPARR_CONTAINER', 'whisparr-v3')
_STUCK_MINUTES      = int(os.environ.get('WHISPARR_STUCK_MINUTES', '60'))

def _whisparr_watchdog():
    """Restart Whisparr if any ManualImport command has been queued/started for too long."""
    try:
        r = requests.get(f'{WHISPARR_URL}/api/v3/command',
                         headers={'X-Api-Key': WHISPARR_KEY}, timeout=10)
        if not r.ok:
            return
        stuck_cutoff = time.time() - _STUCK_MINUTES * 60
        for cmd in r.json():
            if cmd.get('name') != 'ManualImport':
                continue
            if cmd.get('status') not in ('queued', 'started'):
                continue
            # stateChangeTime is ISO8601
            ts_str = cmd.get('stateChangeTime') or cmd.get('queued') or ''
            if not ts_str:
                continue
            try:
                from datetime import timezone
                import datetime as dt
                ts = dt.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                ts_epoch = ts.timestamp()
            except Exception:
                continue
            if ts_epoch < stuck_cutoff:
                log.warning('Whisparr watchdog: ManualImport stuck for >%dm (cmd id=%s) — restarting %s',
                            _STUCK_MINUTES, cmd.get('id'), _WHISPARR_CONTAINER)
                try:
                    import subprocess
                    subprocess.run(['docker', 'restart', _WHISPARR_CONTAINER],
                                   timeout=30, capture_output=True)
                    log.info('Whisparr watchdog: %s restarted', _WHISPARR_CONTAINER)
                except Exception as e:
                    log.warning('Whisparr watchdog: restart failed — %s', e)
                return  # one restart per check is enough
    except Exception as e:
        log.warning('Whisparr watchdog error: %s', e)


def _auto_import_loop():
    _cleanup_tick = 0
    while True:
        time.sleep(60)
        _cleanup_tick += 1
        try:
            state     = load_state()
            sab_done  = get_sabnzbd_completed()
            qbit_done = get_qbittorrent_completed()
            changed   = False

            # Normal: import tracked queued items
            for code, info in list(state['queued'].items()):
                nzo_id = info['nzo_id']
                client = info.get('client', 'sabnzbd')

                if client == 'ytdlp':
                    dl_path = info.get('dl_path', '')
                    video   = find_video_file(dl_path) if dl_path else None
                    if video:
                        ok = whisparr_manual_import(video, info['wid'])
                        if ok:
                            log.info('auto-import %s: imported via ytdlp — %s', code, Path(video).name)
                            state['imported'].append(code)
                            del state['queued'][code]
                            changed = True
                        else:
                            log.warning('auto-import %s: ytdlp Whisparr import failed', code)
                        continue
                    # No video yet — check if the thread is still alive
                    active_names  = {t.name for t in threading.enumerate()}
                    thread_alive  = f'ytdlp-{code}' in active_names
                    if thread_alive:
                        continue  # still downloading, leave it
                    # Thread is dead and no video — retry indefinitely (30 min cooldown)
                    ytdlp_url  = info.get('ytdlp_url', '')
                    started_at = info.get('started_at', 0)
                    if not ytdlp_url:
                        # No URL stored (legacy entry) — can't retry, drop it
                        log.warning('auto-import %s: ytdlp no URL stored, removing', code)
                        del state['queued'][code]
                        changed = True
                        continue
                    if time.time() - started_at < 1800:
                        continue  # cooldown — wait 30 min between attempts
                    retries = info.get('ytdlp_retries', 0) + 1
                    info['ytdlp_retries'] = retries
                    info['started_at']    = time.time()
                    dl_dir = Path(dl_path or str(_DATA_DIR / 'ytdlp' / code))
                    threading.Thread(target=_ytdlp_worker, args=(code, [ytdlp_url], dl_dir),
                                     daemon=True, name=f'ytdlp-{code}').start()
                    log.info('auto-import %s: ytdlp retry #%d (will keep trying)', code, retries)
                    changed = True
                    continue

                completed = sab_done if client == 'sabnzbd' else qbit_done
                if nzo_id not in completed:
                    continue
                storage = completed[nzo_id]
                video   = find_video_file(storage)
                if not video:
                    misses = info.get('no_video_misses', 0) + 1
                    info['no_video_misses'] = misses
                    changed = True
                    if misses >= 10:
                        log.warning('auto-import %s: no video after %d attempts, giving up — %s', code, misses, storage)
                        state['failed'].append(code)
                        del state['queued'][code]
                    else:
                        log.warning('auto-import %s: no video in %s (attempt %d/10)', code, storage, misses)
                    continue
                ok = whisparr_manual_import(video, info['wid'])
                if ok:
                    log.info('auto-import %s: imported via %s — %s', code, client, Path(video).name)
                    state['imported'].append(code)
                    del state['queued'][code]
                    changed = True
                else:
                    log.warning('auto-import %s: Whisparr import failed', code)

            # Detect SABnzbd failures and blacklist the release so the snatch loop retries
            sab_failed = get_sabnzbd_failed()
            for code, info in list(state['queued'].items()):
                if info.get('client', 'sabnzbd') != 'sabnzbd':
                    continue
                nzo_id = info['nzo_id']
                if nzo_id not in sab_failed:
                    continue
                fail_info = sab_failed[nzo_id]
                release   = info.get('release', nzo_id)
                log.warning('auto-import %s: SABnzbd failed — %s | %s',
                            code, release[:80], fail_info['fail_message'][:120])
                # Blacklist this specific release so we don't queue it again
                bl = state['blacklisted_releases'].setdefault(code, [])
                if release not in bl:
                    bl.append(release)
                # Clean up the leftover download folder
                storage = fail_info.get('storage', '')
                if storage:
                    src = Path(storage) if Path(storage).is_dir() else Path(storage).parent
                    try:
                        import shutil
                        if src.exists():
                            shutil.rmtree(src)
                            log.info('auto-import %s: deleted failed download dir %s', code, src)
                    except Exception as e:
                        log.warning('auto-import %s: cleanup of %s failed: %s', code, src, e)
                # Remove from SABnzbd history
                _delete_sabnzbd_history(nzo_id)
                # Remove from queued — snatch loop will retry with a different release
                del state['queued'][code]
                changed = True
                log.info('auto-import %s: removed from queue; snatch loop will retry skipping blacklisted release', code)

            # Rescue: catch completed SABnzbd downloads that lost their state entry
            tracked_nzo_ids = {info['nzo_id'] for info in state['queued'].values()}
            imported_set    = set(state.get('imported', []))
            for nzo_id, name, storage in get_sabnzbd_history():
                if nzo_id in tracked_nzo_ids or not name or not storage:
                    continue
                code = name.strip()
                if code in imported_set:
                    continue
                # If already imported in Whisparr, delete the orphaned download
                if whisparr_code_has_file(code):
                    src_dir = Path(storage) if Path(storage).is_dir() else Path(storage).parent
                    try:
                        import shutil
                        shutil.rmtree(src_dir)
                        log.info('rescue: deleted dupe download %s (already in Whisparr)', code)
                    except Exception as e:
                        log.warning('rescue: cleanup of dupe %s failed: %s', src_dir, e)
                    state['imported'].append(code)
                    changed = True
                    continue
                movie = whisparr_movie_id_for_code(code)
                if not movie:
                    continue
                wid, _ = movie
                video = find_video_file(storage)
                if not video:
                    continue
                ok = whisparr_manual_import(video, wid)
                if ok:
                    log.info('auto-import rescue %s: imported — %s', code, Path(video).name)
                    state['imported'].append(code)
                    changed = True
                else:
                    log.warning('auto-import rescue %s: Whisparr import failed', code)

            if changed:
                save_state(state)

            # Every 5 minutes: sweep downloads dir for dupes and empty folders
            if _cleanup_tick % 5 == 0:
                _cleanup_downloads_dir()
                _whisparr_watchdog()

        except Exception:
            log.exception('auto-import loop error')

def _startup_dl_cleanup():
    """On startup, delete any empty coded folders left by interrupted import threads."""
    time.sleep(10)
    import shutil, re as _re
    CODE_RE_S = _re.compile(r'\b([A-Z]{2,4}\d{3,5})\b')
    VIDEO_EXT_S = {'.mp4', '.mkv', '.avi', '.mov'}
    try:
        cfg = json.load(open(CONFIG_FILE))
        dl_root_str = cfg.get('sabnzbd_url', '')
    except Exception:
        dl_root_str = ''
    # Find downloads path from SABnzbd or fall back to /mnt/nas/downloads
    dl_root = Path('/mnt/nas/downloads')
    if not dl_root.exists():
        return
    cleaned = 0
    for folder in dl_root.iterdir():
        if not folder.is_dir(): continue
        base = _re.sub(r'(\.[0-9]+)+$', '', folder.name)
        if not CODE_RE_S.search(base): continue
        has_video = any(f.suffix.lower() in VIDEO_EXT_S for f in folder.rglob('*') if f.is_file())
        if not has_video:
            try:
                shutil.rmtree(folder)
                cleaned += 1
                log.info('startup cleanup: removed empty dl folder %s', folder.name)
            except Exception as e:
                log.warning('startup cleanup: could not remove %s: %s', folder.name, e)
    if cleaned:
        log.info('startup dl-cleanup: removed %d empty coded folders', cleaned)


_auto_import_started = False

def _start_auto_import():
    global _auto_import_started
    if _auto_import_started:
        return
    _auto_import_started = True
    threading.Thread(target=_auto_import_loop,   daemon=True, name='auto-import').start()
    threading.Thread(target=_auto_snatch_loop,   daemon=True, name='auto-snatch').start()
    threading.Thread(target=_startup_load_scan,  daemon=True, name='scan-startup').start()
    threading.Thread(target=_scene_sync_loop,    daemon=True, name='scene-sync').start()
    threading.Thread(target=_startup_dl_cleanup, daemon=True, name='dl-cleanup').start()
    log.info('background threads started: auto-import, auto-snatch, scan-startup, scene-sync, dl-cleanup')

# ── Ingest folder ─────────────────────────────────────────────────────────────

def _title_from_path(video: Path) -> str:
    """Extract a human-readable search title from a video filename."""
    stem = video.stem
    # Replace separators with spaces
    s = re.sub(r'[._\-]', ' ', stem)
    # Remove resolution, codec, format noise
    s = re.sub(r'\b(1080p|720p|2160p|4k|uhd|hdl|hdr|web[- ]?dl|x264|x265|hevc|avc|mp4|mkv|avi'
               r'|siterip|pack|xxx|italian|german|french|english|part\d+)\b', ' ', s, flags=re.I)
    # Remove dates like 2022 03 26 or 22 06 04
    s = re.sub(r'\b(20\d{2}|19\d{2})\b', ' ', s)
    s = re.sub(r'\b\d{2}\s+\d{2}\s+\d{2}\b', ' ', s)
    # Remove scene codes themselves
    s = re.sub(r'\b[A-Z]{1,4}\d{2,6}\b', ' ', s)
    # Remove short noise tokens
    s = re.sub(r'\b\w{1,2}\b', ' ', s)
    # Collapse whitespace
    s = ' '.join(s.split())
    return s[:120]


def _ingest_add_import(video: Path, code: str, search_term: str, root_folder, quality_id) -> dict:
    """
    Lookup search_term in TPDB, add to Whisparr if needed, then queue import.
    Returns dict with keys: wid, title, status ('queued'|'failed'), note, action.
    """
    r = requests.get(f'{WHISPARR_URL}/api/v3/movie/lookup',
                     params={'term': search_term},
                     headers={'X-Api-Key': WHISPARR_KEY}, timeout=15)
    hits = r.json() if r.ok else []
    if not hits:
        return {'status': 'failed', 'note': f'not in TPDB ({search_term})', 'action': 'add+import'}

    movie = hits[0]
    title = movie.get('title') or search_term

    # If already in library (Whisparr sets id > 0 on lookup results)
    wid = int(movie.get('id') or 0)
    if wid:
        action = 'import'
    else:
        movie['rootFolderPath']   = root_folder
        movie['qualityProfileId'] = quality_id
        movie['monitored']        = True
        movie['addOptions']       = {'searchForMovie': False}
        r2 = requests.post(f'{WHISPARR_URL}/api/v3/movie',
                           headers={'X-Api-Key': WHISPARR_KEY, 'Content-Type': 'application/json'},
                           json=movie, timeout=15)
        if r2.status_code in (200, 201):
            wid    = r2.json()['id']
            action = 'add+import'
        elif r2.status_code in (400, 409):
            # Already exists — find the wid by re-searching
            try:
                err = r2.json()
                # Whisparr returns list of errors
                if isinstance(err, list):
                    err_msg = ' '.join(e.get('errorMessage','') for e in err)
                else:
                    err_msg = str(err)
            except Exception:
                err_msg = r2.text[:120]
            # Try to get existing wid from the library
            existing = requests.get(f'{WHISPARR_URL}/api/v3/movie/lookup',
                                    params={'term': search_term},
                                    headers={'X-Api-Key': WHISPARR_KEY}, timeout=10)
            for m in (existing.json() if existing.ok else []):
                if int(m.get('id') or 0) > 0:
                    wid = int(m['id']); action = 'import'; break
            if not wid:
                return {'status': 'failed', 'note': f'already exists but wid not found: {err_msg}',
                        'action': 'add+import'}
        else:
            return {'status': 'failed', 'note': f'add HTTP {r2.status_code}: {r2.text[:80]}',
                    'action': 'add+import'}

    entry = {'title': title, 'video': str(video), 'wid': wid,
             'status': 'pending', 'time': time.strftime('%H:%M'), 'action': action}
    _local_import_log.appendleft(entry)
    ok = whisparr_manual_import(str(video), wid, _log_entry=entry)
    return {'wid': wid, 'title': title, 'status': 'queued' if ok else 'failed',
            'note': title, 'action': action}


def _ingest_get_library_config():
    """Return (root_folder, quality_id) from Whisparr, with fallbacks."""
    try:
        wconn = db_connect()
        ex    = wconn.execute('SELECT Path FROM Movies LIMIT 1').fetchone()
        wconn.close()
        root_folder = str(Path(ex['Path']).parent) if ex else '/mnt/user/data/media/xxx'
        qp = requests.get(f'{WHISPARR_URL}/api/v3/qualityprofile',
                          headers={'X-Api-Key': WHISPARR_KEY}, timeout=10).json()
        quality_id = qp[0]['id'] if qp else 1
        return root_folder, quality_id
    except Exception:
        return '/mnt/user/data/media/xxx', 1


def _do_ingest_folder(job_id, folder_path):
    job = _ingest_jobs[job_id]
    try:
        root = Path(folder_path)
        if not root.exists():
            job['error'] = 'Path not found'
            job['status'] = 'done'
            return

        videos = sorted([f for f in root.rglob('*')
                         if f.is_file() and f.suffix.lower() in VIDEO_EXTS])
        job['total'] = len(videos)
        if not videos:
            job['error'] = 'No video files found'
            job['status'] = 'done'
            return

        root_folder, quality_id = _ingest_get_library_config()

        for i, video in enumerate(videos):
            job['done'] = i + 1
            res = {'file': video.name, 'path': str(video),
                   'code': None, 'action': '', 'status': '', 'note': ''}

            code = _extract_code(video)
            res['code'] = code
            if not code:
                res['action'] = 'skip'; res['status'] = 'skipped'; res['note'] = 'no code in filename'
                job['results'].append(res); continue

            sconn = _scan_db_connect()
            row   = sconn.execute(
                'SELECT wid, has_file, monitored, title FROM scenes WHERE code=?', (code,)
            ).fetchone()
            sconn.close()

            if row and row['has_file']:
                # Live check: if download is bigger, upgrade; otherwise clean up source
                wid_check = row['wid']
                dl_size = video.stat().st_size
                wh_size = whisparr_existing_file_size(wid_check) if wid_check else 0
                if wid_check and dl_size > wh_size * 1.1:
                    log.info('ingest: upgrading %s — download %.1fGB > existing %.1fGB',
                             code, dl_size/1e9, wh_size/1e9)
                    # Fall through to import block with force
                    entry = {'title': row['title'] or code, 'video': str(video),
                             'wid': wid_check, 'status': 'pending',
                             'time': time.strftime('%H:%M'), 'action': 'upgrade'}
                    _local_import_log.appendleft(entry)
                    ok = whisparr_manual_import(str(video), wid_check, _log_entry=entry, force=True)
                    res['action'] = 'upgrade'
                    res['status'] = 'queued' if ok else 'failed'
                    res['note']   = row['title'] or code
                    job['results'].append(res); continue
                else:
                    res['action'] = 'skip'; res['status'] = 'skipped'
                    res['note'] = row['title'] or code
                    # Clean up source folder since Whisparr already has this file
                    try:
                        src_dir = video.parent
                        if src_dir.exists():
                            import shutil; shutil.rmtree(src_dir)
                            log.info('ingest: cleaned up source dir (already had file): %s', src_dir)
                    except Exception as e:
                        log.warning('ingest: cleanup of %s failed: %s', video.parent, e)
                    job['results'].append(res); continue

            if row:
                wid   = row['wid']
                title = row['title'] or code
                if not row['monitored']:
                    res['action'] = 'monitor+import'
                    try:
                        r = requests.get(f'{WHISPARR_URL}/api/v3/movie/{wid}',
                                         headers={'X-Api-Key': WHISPARR_KEY}, timeout=10)
                        mv = r.json(); mv['monitored'] = True
                        requests.put(f'{WHISPARR_URL}/api/v3/movie/{wid}',
                                     headers={'X-Api-Key': WHISPARR_KEY, 'Content-Type': 'application/json'},
                                     json=mv, timeout=10)
                    except Exception as e:
                        res['status'] = 'failed'; res['note'] = f'monitor: {e}'
                        job['results'].append(res); continue
                else:
                    res['action'] = 'import'
                entry = {'title': title, 'video': str(video), 'wid': wid,
                         'status': 'pending', 'time': time.strftime('%H:%M'), 'action': res['action']}
                _local_import_log.appendleft(entry)
                ok = whisparr_manual_import(str(video), wid, _log_entry=entry)
                res['status'] = 'queued' if ok else 'failed'
                res['note']   = title

            else:
                # Not in local scenes table — TPDB lookup → add → import
                # Try code first, then title extracted from filename as fallback
                result = _ingest_add_import(video, code, code, root_folder, quality_id)
                if result['status'] == 'failed' and 'not in TPDB' in result['note']:
                    title_term = _title_from_path(video)
                    if title_term and title_term.lower() != code.lower():
                        result = _ingest_add_import(video, code, title_term, root_folder, quality_id)
                        if result['status'] == 'queued':
                            result['note'] = f'{result["note"]} (via title)'
                res.update(result)

            job['results'].append(res)

        job['status'] = 'done'
    except Exception as e:
        job['error'] = str(e)
        job['status'] = 'done'


@app.route('/ingest')
def ingest_page():
    return render_template('ingest.html')


@app.route('/api/ingest-single', methods=['POST'])
def api_ingest_single():
    """Retry a single file with a user-supplied search term."""
    d           = request.json or {}
    video       = d.get('video', '').strip()
    search_term = d.get('search_term', '').strip()
    if not video or not search_term:
        return jsonify({'ok': False, 'error': 'missing video or search_term'}), 400
    if not Path(video).is_file():
        return jsonify({'ok': False, 'error': 'file not found'}), 404
    code = _extract_code(Path(video)) or search_term.upper()
    root_folder, quality_id = _ingest_get_library_config()
    result = _ingest_add_import(Path(video), code, search_term, root_folder, quality_id)
    return jsonify({'ok': result['status'] == 'queued', **result})


@app.route('/api/ingest-folder', methods=['POST'])
def api_ingest_folder():
    d    = request.json or {}
    path = (d.get('path') or '').strip()
    if not path:
        return jsonify({'ok': False, 'error': 'no path'}), 400
    if not Path(path).exists():
        return jsonify({'ok': False, 'error': 'path not found'}), 404

    import uuid
    job_id = str(uuid.uuid4())[:8]
    _ingest_jobs[job_id] = {
        'status': 'running', 'done': 0, 'total': 0,
        'results': [], 'error': None, 'path': path,
    }
    threading.Thread(target=_do_ingest_folder, args=(job_id, path),
                     daemon=True, name=f'ingest-{job_id}').start()
    return jsonify({'ok': True, 'job_id': job_id})


@app.route('/api/ingest-job/<job_id>')
def api_ingest_job(job_id):
    job = _ingest_jobs.get(job_id)
    if not job:
        return jsonify({'ok': False, 'error': 'unknown job'}), 404
    return jsonify({'ok': True, **job})


@app.before_request
def _ensure_auto_import():
    _start_auto_import()

if __name__ == '__main__':
    _start_auto_import()
    port = int(os.environ.get('SNATCHARR_PORT', 6060))
    app.run(host='0.0.0.0', port=port, debug=False)
