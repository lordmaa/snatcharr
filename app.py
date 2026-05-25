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

AUTO_SNATCH = True
SNATCH_INTERVAL_H = 4
RETRY_NOT_FOUND_D = 7

NEWZNAB_CATS = '6000,6010,6020,6030,6040,6050'

def get_prowlarr_indexers():
    try:
        r = requests.get(f'{PROWLARR_URL}/api/v1/indexer',
                         headers={'X-Api-Key': PROWLARR_KEY}, timeout=10)
        return [i['id'] for i in r.json() if i.get('enable', True)]
    except Exception:
        return []
CODE_RE           = re.compile(r'^[A-Za-z]{1,4}\d{2,6}$')
VIDEO_EXTS        = {'.mkv', '.mp4', '.avi', '.m4v', '.mov'}

def load_config():
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text())
    return {'active_studios': []}

def save_config(cfg):
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))

# Apply saved config on startup
_reload_config()

def get_active_studios():
    return set(load_config().get('active_studios', []))

def load_state():
    if STATE_FILE.exists():
        s = json.loads(STATE_FILE.read_text())
    else:
        s = {}
    s.setdefault('queued', {})
    s.setdefault('imported', [])
    s.setdefault('not_found', [])
    s.setdefault('not_found_at', {})   # code -> ISO timestamp of last search
    s.setdefault('failed', [])
    return s

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

def db_connect():
    conn = sqlite3.connect(WHISPARR_DB)
    conn.row_factory = sqlite3.Row
    return conn

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
        if not valid:
            continue
        have    = sum(1 for r in valid if r['MovieFileId'])
        missing = sum(1 for r in valid if not r['MovieFileId'])
        queued  = sum(1 for r in valid if not r['MovieFileId'] and str(r['Code']).upper() in queued_codes)
        headshot = None
        try:
            for img in json.loads(p['Images'] or '[]'):
                if img.get('coverType') == 'headshot':
                    headshot = f'/headshot/{p["Id"]}'
                    break
        except Exception:
            pass
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
        timeout=15)
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

def whisparr_manual_import(file_path, movie_id):
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
    except Exception:
        pass
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
        if r.status_code == 201:
            # Rescan after a delay so the async file move has time to complete
            def _rescan():
                time.sleep(180)
                try:
                    requests.post(f'{WHISPARR_URL}/api/v3/command',
                        headers={'X-Api-Key': WHISPARR_KEY, 'Content-Type': 'application/json'},
                        json={'name': 'RescanMovie', 'movieId': movie_id}, timeout=10)
                    log.info('rescan fired for movie %s', movie_id)
                except Exception:
                    pass
            threading.Thread(target=_rescan, daemon=True).start()
            return True
        return False
    except Exception:
        return False

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    active     = get_active_studios()
    performers = get_performers(active) if active else []
    return render_template('index.html', performers=performers, active_studios=active)

@app.route('/performer/<name>')
def performer(name):
    active  = get_active_studios()
    scenes  = get_scenes(name, active)
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
    return render_template('queue.html', rows=rows,
                           imported=state['imported'], not_found=state['not_found'])

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
                    'performer_tag', 'auto_snatch', 'snatch_interval_h', 'retry_not_found_d'):
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
    )

@app.route('/logs')
def logs_page():
    return render_template('logs.html', lines=list(_ring.records))

@app.route('/api/snatch-now', methods=['POST'])
def api_snatch_now():
    threading.Thread(target=_run_auto_snatch, daemon=True, name='snatch-now').start()
    return jsonify({'ok': True, 'msg': 'Snatch run started in background'})

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
                if code not in state['not_found']:
                    state['not_found'].append(code)
                state['not_found_at'][code] = time.strftime('%Y-%m-%dT%H:%M:%S')
                nf_codes.add(code)
                changed = True
                log.info('auto-snatch: %s not found', code)
                continue

            nzo_id = dl_client = result = None
            for candidate in nzb_results:
                nzo_id = add_to_sabnzbd(candidate['url'], code)
                if nzo_id:
                    dl_client = 'sabnzbd'
                    result = candidate
                    break
            if not nzo_id:
                for candidate in torrent_results:
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
                log.warning('auto-snatch: all clients rejected %s', code)
            time.sleep(2)

    conn.close()
    if changed:
        save_state(state)
    log.info('auto-snatch run complete')

def _auto_import_loop():
    while True:
        time.sleep(60)
        try:
            state = load_state()
            if not state['queued']:
                continue
            sab_done  = get_sabnzbd_completed()
            qbit_done = get_qbittorrent_completed()
            changed = False
            for code, info in list(state['queued'].items()):
                nzo_id = info['nzo_id']
                client = info.get('client', 'sabnzbd')
                completed = sab_done if client == 'sabnzbd' else qbit_done
                if nzo_id not in completed:
                    continue
                storage = completed[nzo_id]
                video   = find_video_file(storage)
                if not video:
                    log.warning('auto-import %s: no video in %s', code, storage)
                    continue
                ok = whisparr_manual_import(video, info['wid'])
                if ok:
                    log.info('auto-import %s: imported via %s — %s', code, client, Path(video).name)
                    state['imported'].append(code)
                    del state['queued'][code]
                    changed = True
                else:
                    log.warning('auto-import %s: Whisparr import failed', code)
            if changed:
                save_state(state)
        except Exception:
            log.exception('auto-import loop error')

_auto_import_started = False

def _start_auto_import():
    global _auto_import_started
    if _auto_import_started:
        return
    _auto_import_started = True
    threading.Thread(target=_auto_import_loop, daemon=True, name='auto-import').start()
    threading.Thread(target=_auto_snatch_loop, daemon=True, name='auto-snatch').start()
    log.info('auto-import and auto-snatch background threads started')

@app.before_request
def _ensure_auto_import():
    _start_auto_import()

if __name__ == '__main__':
    _start_auto_import()
    app.run(host='0.0.0.0', port=6060, debug=False)
