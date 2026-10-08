"""
epic.py — Epic Games Store integration for PlayDate.
Handles OAuth2 authentication, library sync, metadata, and cover art.
"""

import json
import logging
import os
import re
import threading
import time

import requests
from config import BASE_DIR, load_config, _save_config_data
from database import next_negative_appid
from images import save_as_jpg
from utils import review_score_label

log = logging.getLogger(__name__)

# ── Epic OAuth2 constants ─────────────────────────────────────────────────────
# Public credentials documented in Legendary and EpicResearch.
EPIC_CLIENT_ID     = '34a02cf8f4414e29b15921876da36f9a'
EPIC_CLIENT_SECRET = 'daafbccc737745039dffe53d94fc76cf'
EPIC_TOKEN_URL     = 'https://account-public-service-prod.ol.epicgames.com/account/api/oauth/token'
EPIC_AUTH_REDIRECT = (
    'https://www.epicgames.com/id/login'
    '?redirectUrl=https%3A%2F%2Fwww.epicgames.com%2Fid%2Fapi%2Fredirect'
    '%3FclientId%3D34a02cf8f4414e29b15921876da36f9a%26responseType%3Dcode'
)

_ASSETS_URL         = 'https://launcher-public-service-prod06.ol.epicgames.com/launcher/api/public/assets/Windows?label=Live'
_CATALOG_URL_TMPL   = 'https://catalog-public-service-prod06.ol.epicgames.com/catalog/api/shared/namespace/{ns}/bulk/items'
_STORE_GRAPHQL_URL  = 'https://store.epicgames.com/graphql'
_STORE_GQL_HEADERS  = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Content-Type': 'application/json',
    'Referer': 'https://store.epicgames.com/',
    'Origin': 'https://store.epicgames.com',
}

_RATINGS_QUERY = """
query getProductResult($sandboxId: String!, $locale: String!) {
  RatingsPolls {
    getProductResult(sandboxId: $sandboxId, locale: $locale) {
      averageRating
      pollResult { id total }
    }
  }
}
"""

_SEARCH_STORE_QUERY = """
query searchStoreQuery($namespace: String, $country: String!, $locale: String) {
  Catalog {
    searchStore(namespace: $namespace, country: $country, locale: $locale) {
      elements {
        productSlug urlSlug description tags { id } pcReleaseDate
        offerMappings { pageSlug pageType }
        catalogNs { mappings(pageType: "productHome") { pageSlug pageType } }
      }
    }
  }
}
"""

_CATALOG_TAGS_QUERY = """
query catalogTags($namespace: String!) {
  Catalog {
    tags(namespace: $namespace, start: 0, count: 999) {
      elements { id name }
    }
  }
}
"""

# In-process cache of Epic tag id -> name, populated on first use
_tag_name_cache: dict = {}
_tag_cache_lock = threading.Lock()

VERTICAL_DIR   = os.path.join(BASE_DIR, 'static', 'img', 'library', 'vertical')
HORIZONTAL_DIR = os.path.join(BASE_DIR, 'static', 'img', 'library', 'horizontal')


def _clean_epic_slug(slug):
    """
    Epic's catalog/store APIs (urlSlug/productSlug) inconsistently append a
    sub-page segment like "/home" to some products' slugs and not others --
    seemingly depending on how the listing was configured on Epic's end, not
    anything PlayDate can predict. The store_url template only wants the bare
    product slug (store.epicgames.com/p/{slug}); a "/home" (or any other
    sub-page) suffix produces a broken link for the affected games.
    """
    slug = (slug or '').strip().strip('/')
    return slug.split('/', 1)[0] if slug else ''


_HEX_ID_RE = re.compile(r'^[0-9a-f]{32}$')


def _store_page_slug(element):
    """
    The slug of a game's real store page, from a searchStore element, or ''.

    Newer store pages carry a short hash suffix ("luftrausers-51e5e9") that
    neither productSlug (null) nor the catalog's urlSlug ("luftrausers", or a
    bare 32-character id for some products) includes, so /p/{urlSlug} 404s.
    The real slug is in the productHome page mappings. urlSlug is kept only as
    a last resort, and never when it is an internal id.
    """
    mappings = list((element.get('catalogNs') or {}).get('mappings') or [])
    mappings += list(element.get('offerMappings') or [])
    for m in mappings:
        if (m or {}).get('pageType') == 'productHome':
            slug = _clean_epic_slug(m.get('pageSlug'))
            if slug:
                return slug
    slug = _clean_epic_slug(element.get('productSlug') or element.get('urlSlug'))
    return '' if _HEX_ID_RE.match(slug) else slug


# ── Token storage ─────────────────────────────────────────────────────────────

def load_epic_tokens():
    """Return stored Epic tokens dict, or None if not connected."""
    epic = (load_config() or {}).get('epic_games', {})
    if epic.get('access_token') and epic.get('refresh_token'):
        return epic
    return None


def is_connected():
    return load_epic_tokens() is not None


def get_display_name():
    tokens = load_epic_tokens()
    return tokens.get('display_name') if tokens else None


def clear_epic_tokens():
    """Remove all Epic data from config.json."""
    data = load_config() or {}
    data.pop('epic_games', None)
    _save_config_data(data)


# ── Auth flow ─────────────────────────────────────────────────────────────────

def get_auth_url():
    return EPIC_AUTH_REDIRECT


def _extract_auth_code(raw: str) -> str:
    """
    Accept any of the three formats a user might paste from the Epic auth page:
      - the raw hex code ("2cf717db…")
      - the JSON body  ({"authorizationCode":"2cf717db…",…})
      - the redirectUrl ("https://localhost/launcher/authorized?code=2cf717db…")
    Returns the bare authorization code, or '' if nothing was found.
    """
    raw = raw.strip()
    if raw.startswith('{'):
        try:
            obj = json.loads(raw)
            return (obj.get('authorizationCode') or obj.get('code') or '').strip()
        except Exception:
            pass
    if 'code=' in raw:
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(raw).query)
        return ((qs.get('code') or [''])[0]).strip()
    return raw


def exchange_code(code):
    """
    Exchange an authorization code for Epic tokens.
    Returns (True, display_name) on success, (False, error_message) on failure.
    """
    code = _extract_auth_code(code)
    if not code:
        return False, 'Could not extract an authorization code from the input'
    from runners.oauth2 import exchange_authorization_code
    try:
        data = exchange_authorization_code(
            EPIC_TOKEN_URL, EPIC_CLIENT_ID, EPIC_CLIENT_SECRET,
            code, use_basic_auth=True,
        )
    except ValueError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)

    try:
        access_token  = data['access_token']
        refresh_token = data['refresh_token']
        expires_at    = int(time.time()) + int(data.get('expires_in', 7200))
        account_id    = data.get('account_id', '')
        display_name  = data.get('displayName', '')

        cfg = load_config() or {}
        cfg['epic_games'] = {
            'access_token':  access_token,
            'refresh_token': refresh_token,
            'expires_at':    expires_at,
            'account_id':    account_id,
            'display_name':  display_name,
        }
        _save_config_data(cfg)
        log.info(f'Epic auth: connected as {display_name!r} (account_id={account_id!r})')
        return True, display_name
    except KeyError as e:
        return False, f'Unexpected token response (missing {e})'


def get_valid_session():
    """Return a requests.Session with a valid Epic Bearer token, or None."""
    from runners.oauth2 import get_valid_session as _get_session
    return _get_session(
        'epic_games', EPIC_TOKEN_URL, EPIC_CLIENT_ID, EPIC_CLIENT_SECRET,
        use_basic_auth=True,
    )


# ── Library sync ──────────────────────────────────────────────────────────────

_sync_state = {
    'running': False, 'phase': None, 'done': 0, 'total': 0, 'current_game': '',
    'new_games': 0, 'total_games': 0, 'duplicates_detected': 0, 'error': None,
}
_sync_lock   = threading.Lock()
_sync_cancel = threading.Event()


def get_sync_state():
    with _sync_lock:
        return dict(_sync_state)


def start_library_sync():
    """Start a background library sync. Returns {status: 'started'|'already_running'}."""
    with _sync_lock:
        if _sync_state['running']:
            return {'status': 'already_running'}
        _sync_cancel.clear()
        _sync_state.update({
            'running': True, 'phase': 'fetching_assets', 'done': 0, 'total': 0,
            'current_game': '', 'new_games': 0, 'total_games': 0,
            'duplicates_detected': 0, 'error': None,
        })
    threading.Thread(target=_run_sync_library, daemon=True).start()
    return {'status': 'started'}


def cancel_library_sync():
    """Signal the running library sync to stop."""
    _sync_cancel.set()


# Bonus content (soundtracks, artbooks) that publishers sometimes ship as their
# own top-level catalog item with neither `mainGameItem` nor an
# addons/digitalextras category set -- confirmed live against Q.U.B.E. 2's
# bundled soundtrack, which the catalog API tags identically to a real game.
_BONUS_CONTENT_RE = re.compile(r'\b(soundtrack|art\s*book|ost)\b', re.I)

# Dev-tool / editor entries. Epic files these under the 'engines' category, but
# so is at least one actual game (Bus Simulator 18), so 'engines' alone isn't
# enough -- pair it with a name match. 'ARK ModKit (UE4)', 'Conan Exiles Dev Kit'.
_DEVKIT_RE = re.compile(r'\b(dev\s?kit|mod\s?kit|sdk|editor)\b', re.I)

# Catalog category paths that are never a standalone game. 'software' covers the
# non-game apps Epic gives away (Brave, itch.io, iHeart, and game-making tools
# like RPG in a Box); 'addons'/'digitalextras' are DLC / soundtracks / artbooks.
_NON_GAME_CATEGORIES = {'addons', 'digitalextras', 'software'}


def _is_dlc_or_extra(entry, name):
    """
    True if a catalog entry is DLC, a digital extra, a non-game application, or a
    dev kit rather than a standalone game -- skip it during library sync.

    Epic marks formal DLC via `mainGameItem` (requires requesting the catalog
    batch with includeMainGameDetails=true) and/or an 'addons'/'digitalextras'
    category; standalone non-games (browsers, media apps, tools) sit under
    'software'; dev kits under 'engines' + a kit/editor name. A conservative
    name check (see _BONUS_CONTENT_RE) covers bonus content that slips through.
    """
    if entry.get('mainGameItem'):
        return True
    cats = {c.get('path') for c in entry.get('categories', [])}
    if cats & _NON_GAME_CATEGORIES:
        return True
    if 'engines' in cats and _DEVKIT_RE.search(name or ''):
        return True
    if _BONUS_CONTENT_RE.search(name or ''):
        return True
    return False


def _run_sync_library():
    try:
        _do_sync_library()
    except Exception as e:
        log.error(f'Epic library sync thread: {e}', exc_info=True)
        with _sync_lock:
            _sync_state.update({'running': False, 'phase': 'error', 'error': str(e)})


def _do_sync_library():
    from database import get_db, update_game_data as _update_game_data
    from datetime import datetime, timezone, date as _date

    session = get_valid_session()
    if not session:
        with _sync_lock:
            _sync_state.update({'running': False, 'phase': 'error',
                                'error': 'Not connected to Epic — please reconnect'})
        return

    try:
        resp = session.get(_ASSETS_URL, timeout=20)
        resp.raise_for_status()
        assets = resp.json()
    except Exception as e:
        with _sync_lock:
            _sync_state.update({'running': False, 'phase': 'error',
                                'error': f'Failed to fetch Epic library: {e}'})
        return

    assets = [a for a in assets if a.get('namespace') not in ('ue',)]
    if not assets:
        with _sync_lock:
            _sync_state.update({'running': False, 'phase': 'done', 'new_games': 0, 'total_games': 0})
        return

    log.info(f'Epic sync: {len(assets)} assets to process')

    # Load existing/blacklisted cids up front so we can skip catalog API calls
    # for games already in the DB -- avoids fetching 50+ batches on every re-sync.
    db = get_db()
    try:
        existing = {row['platform_id'] for row in db.execute(
            "SELECT platform_id FROM games WHERE platform = 'epic_games'"
        ).fetchall()}
        blacklisted = {
            row[0]
            for row in db.execute(
                "SELECT platform_id FROM blacklist WHERE platform_id IS NOT NULL"
            ).fetchall()
        }
    finally:
        db.close()

    # Collect all appNames per catalogItemId -- Epic often has multiple asset records
    # for the same game (entitlement + installable). We need to pick the right one.
    # Installable assets have a non-empty buildVersion; entitlement records do not.
    cid_appnames = {}  # catalogItemId -> [(appName, buildVersion), ...]
    by_ns = {}
    for a in assets:
        ns    = a.get('namespace', 'fn')
        cid   = a.get('catalogItemId', '')
        aname = a.get('appName', '')
        if cid and aname and cid not in existing and cid not in blacklisted:
            cid_appnames.setdefault(cid, []).append((aname, a.get('buildVersion', '')))
            by_ns.setdefault(ns, []).append((cid, aname))

    # Resolve best appName per cid before the main loop.
    cid_best_appname = {}
    for cid, entries in cid_appnames.items():
        with_build = [aname for aname, bv in entries if bv]
        best = with_build[0] if with_build else entries[0][0]
        cid_best_appname[cid] = best
        if len(entries) > 1:
            all_names = [a for a, _ in entries]
            log.debug(f'Epic sync: {cid} has {len(entries)} appNames {all_names!r}, chose {best!r}')

    total_new = len(cid_appnames)
    log.info(f'Epic sync: {total_new} new games to process (skipped {len(assets) - total_new} existing)')

    with _sync_lock:
        _sync_state.update({'phase': 'processing', 'done': 0, 'total': total_new, 'current_game': ''})

    today_ts       = int(datetime.now(timezone.utc).timestamp())
    today          = _date.today().isoformat()
    new_games_count = 0
    seen_cids      = set()

    # Per-namespace cache for store/ratings API calls (same ns = same data)
    store_cache   = {}
    ratings_cache = {}

    db = get_db()
    try:
        for ns, items in by_ns.items():
            cids = [cid for cid, _ in items]
            url  = _CATALOG_URL_TMPL.format(ns=ns)

            for i in range(0, len(cids), 50):
                if _sync_cancel.is_set():
                    with _sync_lock:
                        _sync_state.update({'running': False, 'phase': 'stopped',
                                            'new_games': new_games_count,
                                            'total_games': len(assets)})
                    return

                batch = cids[i:i + 50]
                params = [('id', cid) for cid in batch]
                params += [('country', 'US'), ('locale', 'en'),
                           ('includeDLCDetails', 'true'), ('includeMainGameDetails', 'true')]
                catalog_batch = {}
                try:
                    r = session.get(url, params=params, timeout=20)
                    if r.status_code == 200:
                        catalog_batch = r.json()
                    else:
                        log.warning(f'Epic catalog batch [{ns}]: HTTP {r.status_code}')
                except Exception as e:
                    log.warning(f'Epic catalog batch fetch failed [{ns}]: {e}')

                for cid in batch:
                    if cid in seen_cids or cid in existing or cid in blacklisted:
                        continue
                    seen_cids.add(cid)

                    entry    = catalog_batch.get(cid, {})
                    app_name = cid_best_appname.get(cid, '')
                    name     = entry.get('title') or app_name

                    if _is_dlc_or_extra(entry, name):
                        log.info(f'Epic sync: skipping {name!r} (DLC / digital extra / non-game app, not a standalone game)')
                        continue

                    url_slug = _clean_epic_slug(entry.get('urlSlug', ''))
                    if _HEX_ID_RE.match(url_slug):
                        url_slug = ''

                    with _sync_lock:
                        _sync_state['current_game'] = name

                    next_appid = next_negative_appid(db)
                    db.execute(
                        """INSERT OR IGNORE INTO games
                           (appid, name, platform, platform_id, platform_slug, platform_appname,
                            platform_ns, date_added,
                            completion_status, installed,
                            art_fetched, meta_fetched, cheevos_fetched,
                            protondb_fetched, hltb_fetched)
                           VALUES (?, ?, 'epic_games', ?, ?, ?, ?, ?,
                                   'Never Played', 0,
                                   '0', '0', '0', '0', '0')""",
                        (next_appid, name, cid, url_slug, app_name, ns, today_ts),
                    )
                    db.commit()
                    log.info(f'Epic sync: added {name!r} as appid {next_appid}')

                    # Metadata from catalog entry + store/ratings API
                    meta = _extract_metadata(entry)
                    if ns not in store_cache:
                        store_cache[ns] = _fetch_epic_store_data(ns)
                    if ns not in ratings_cache:
                        ratings_cache[ns] = _fetch_epic_ratings(ns)
                    _apply_store_data(meta, store_cache[ns])
                    meta.update(ratings_cache[ns])
                    meta['meta_fetched'] = today
                    try:
                        _update_game_data(next_appid, **meta)
                    except Exception as e:
                        log.warning(f'Epic metadata: DB update failed for {name!r}: {e}')

                    # Art from Epic CDN
                    key_images = entry.get('keyImages', [])
                    try:
                        _download_epic_art(next_appid, key_images)
                        _update_game_data(next_appid, art_fetched=today)
                        log.info(f'Epic art: downloaded for {name!r} (appid {next_appid})')
                    except Exception as e:
                        log.warning(f'Epic art: failed for {name!r}: {e}')

                    new_games_count += 1
                    with _sync_lock:
                        _sync_state['done'] += 1

                    if _sync_cancel.is_set():
                        with _sync_lock:
                            _sync_state.update({'running': False, 'phase': 'stopped',
                                                'new_games': new_games_count,
                                                'total_games': len(assets)})
                        return

                time.sleep(0.3)
    finally:
        db.close()

    # Re-run install detection now that platform_appname is correct for all records
    try:
        from .watcher import sync_epic_install_status
        sync_epic_install_status()
    except Exception as e:
        log.warning(f'Epic sync: install status refresh failed: {e}')

    from database import auto_detect_duplicates
    from plugins import get_platform_priority
    dupes = auto_detect_duplicates(platform_priority=get_platform_priority())

    if new_games_count > 0:
        try:
            import_purchase_dates()
        except Exception as e:
            log.warning(f'Epic sync: post-sync date import failed: {e}')

    with _sync_lock:
        _sync_state.update({
            'running': False, 'phase': 'done',
            'new_games': new_games_count, 'total_games': len(assets),
            'duplicates_detected': dupes,
        })


def _fetch_art_for_games(games):
    """Download Epic CDN art for newly-added games (runs in background)."""
    for g in games:
        appid      = g['appid']
        name       = g['name']
        key_images = g.get('key_images', [])
        try:
            _download_epic_art(appid, key_images)
            log.info(f'Epic art: downloaded for {name!r} (appid {appid})')
        except Exception as e:
            log.warning(f'Epic art: failed for {name!r}: {e}')
        time.sleep(0.3)


def _fetch_meta_for_new_games(games):
    """Fetch metadata and ratings for newly-added games (runs in background).
    Reuses catalog entries already in memory from the library sync."""
    from database import update_game_data
    from datetime import date
    today = date.today().isoformat()
    for g in games:
        appid = g['appid']
        name  = g['name']
        entry = g.get('entry') or {}
        ns    = g.get('ns') or ''
        try:
            meta = _extract_metadata(entry)
            meta['meta_fetched'] = today
            if ns:
                _apply_store_data(meta, _fetch_epic_store_data(ns))
                meta.update(_fetch_epic_ratings(ns))
            update_game_data(appid, **meta)
            log.info(f'Epic meta: fetched for {name!r} (appid {appid})')
        except Exception as e:
            log.warning(f'Epic meta: failed for {name!r}: {e}')
        time.sleep(0.5)


def _get_tag_name_cache():
    """Return the in-process tag id->name map, fetching from Epic if empty."""
    with _tag_cache_lock:
        if _tag_name_cache:
            return _tag_name_cache
    try:
        resp = requests.post(
            _STORE_GRAPHQL_URL,
            headers=_STORE_GQL_HEADERS,
            json={'query': _CATALOG_TAGS_QUERY, 'variables': {'namespace': 'epic'}},
            timeout=15,
        )
        if resp.status_code == 200:
            elements = ((resp.json().get('data') or {})
                        .get('Catalog', {}).get('tags', {}).get('elements', []))
            with _tag_cache_lock:
                for el in elements:
                    if el.get('id') and el.get('name'):
                        _tag_name_cache[el['id']] = el['name']
            log.info(f'Epic tag cache: loaded {len(_tag_name_cache)} tags')
        else:
            log.warning(f'Epic tag cache: HTTP {resp.status_code}')
    except Exception as e:
        log.warning(f'Epic tag cache fetch failed: {e}')
    return _tag_name_cache


def _parse_pc_release(value):
    """Epic's pcReleaseDate (ISO string) as a unix timestamp, or None. This is
    the date shown as "Release Date" on the store page, set by the publisher.
    (The sibling releaseDate/creationDate fields are when the item was listed
    in Epic's catalog.) Unreleased games carry a 2099 placeholder, so anything
    more than a year ahead is ignored."""
    from datetime import datetime
    if not value:
        return None
    try:
        ts = int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp())
    except Exception:
        return None
    return ts if ts <= time.time() + 365 * 86400 else None


def _apply_store_data(meta, store, current_release=None):
    """Merge a _fetch_epic_store_data() result into a meta dict. Drops the
    private keys, and sets release_date to Epic's date when the game has none
    yet or Epic's is earlier (the earliest platform is the original release;
    a later Epic date is just when it was listed there)."""
    store = dict(store)
    store.pop('_description', None)
    epic_ts = store.pop('_release_date', None)
    meta.update(store)
    if epic_ts and (not current_release or epic_ts < current_release):
        meta['release_date'] = epic_ts


def _fetch_epic_store_data(namespace):
    """Fetch store tags and product slug for a game by namespace via GraphQL.
    Returns {'tags': 'comma,separated', 'platform_slug': 'slug'} with only populated keys."""
    try:
        resp = requests.post(
            _STORE_GRAPHQL_URL,
            headers=_STORE_GQL_HEADERS,
            json={'query': _SEARCH_STORE_QUERY,
                  'variables': {'namespace': namespace, 'country': 'US', 'locale': 'en-US'}},
            timeout=10,
        )
        if resp.status_code != 200:
            log.warning(f'Epic store data HTTP {resp.status_code} for {namespace!r}')
            return {}
        elements = ((resp.json().get('data') or {})
                    .get('Catalog', {}).get('searchStore', {}).get('elements', []))
        if not elements:
            return {}
        el = elements[0]
        result = {}
        tag_ids = [t['id'] for t in (el.get('tags') or []) if t.get('id')]
        if tag_ids:
            tag_map = _get_tag_name_cache()
            names = [tag_map[tid] for tid in tag_ids if tid in tag_map]
            if names:
                result['tags'] = ','.join(names)
                log.info(f'Epic store tags for {namespace!r}: {names}')
        slug = _store_page_slug(el)
        if slug:
            result['platform_slug'] = slug
        desc = (el.get('description') or '').strip()
        if desc:
            result['_description'] = desc
        ts = _parse_pc_release(el.get('pcReleaseDate'))
        if ts:
            result['_release_date'] = ts
        return result
    except Exception as e:
        log.warning(f'Epic store data fetch failed for {namespace!r}: {e}')
        return {}


_REPAIR_KEY = 'epic_listing_repair'   # config.json key: store-listing repair finished (1)


def _claim_progress(total):
    """Claim core's shared bulk-op slot (the top-bar progress button, labelled
    "Backfill missing metadata") for this repair. Returns (state, lock, cancel)
    or None when the slot is busy or this core has no such slot, in which case
    the repair just runs without a progress bar. The same slot is used by
    core's own startup metadata backfill."""
    try:
        from library import _bulk_op_state, _bulk_op_lock, _bulk_op_cancel
    except Exception:
        return None
    with _bulk_op_lock:
        if _bulk_op_state['running']:
            return None
        _bulk_op_cancel.clear()
        _bulk_op_state.update(running=True, op='metadata', total=total, done=0,
                              failed=0, rate_limit_hit=False, aborted=False, result=None)
    return _bulk_op_state, _bulk_op_lock, _bulk_op_cancel


def _release_progress(slot, aborted=False, result=None):
    if not slot:
        return
    state, lock, _cancel = slot
    with lock:
        state['aborted'] = aborted
        state['result'] = result
        state['running'] = False


def _apply_epic_release_dates():
    """Final repair pass, after the PCGamingWiki/Steam backfill: set each game's
    release date to the earlier of that date and Epic's own pcReleaseDate (the
    date on the store page). Epic's date is earlier for Epic exclusives and the
    backfill's is earlier for re-listed older games, so the earlier one is the
    original release; a game the backfill found nothing for gets Epic's.
    Marks the repair finished (flag 2) only if most lookups worked."""
    from database import get_db, update_game_data
    db = get_db()
    rows = db.execute(
        "SELECT appid, name, platform_ns, release_date FROM games "
        "WHERE platform = 'epic_games' AND platform_ns IS NOT NULL AND platform_ns != ''"
    ).fetchall()
    db.close()
    by_ns = {}
    for r in rows:
        by_ns.setdefault(r['platform_ns'], []).append(r)
    slot = _claim_progress(len(by_ns))
    try:
        ok = changed = 0
        for ns, games in by_ns.items():
            if slot and slot[2].is_set():
                log.info('Epic listing repair: cancelled during the release date pass, will retry next start')
                _release_progress(slot, aborted=True)
                return
            epic_ts = _fetch_epic_store_data(ns).get('_release_date')
            if epic_ts:
                ok += 1
                for g in games:
                    cur = g['release_date']
                    if not cur or epic_ts < cur:
                        update_game_data(g['appid'], release_date=epic_ts)
                        changed += 1
                        log.info(f"Epic listing repair: {g['name']!r} release date set from Epic "
                                 f"(was {cur or 'empty'}, now {epic_ts})")
            if slot:
                with slot[1]:
                    slot[0]['done'] += 1
                    if not epic_ts:
                        slot[0]['failed'] += 1
            time.sleep(0.3)
        if by_ns and ok < len(by_ns) / 2:
            log.warning(f'Epic listing repair: only {ok}/{len(by_ns)} release date lookups worked, will retry next start')
            _release_progress(slot, aborted=True)
            return
        cfg = load_config() or {}
        cfg[_REPAIR_KEY] = 2
        _save_config_data(cfg)
        log.info(f'Epic listing repair: release date pass done, {changed} dates set from Epic')
        _release_progress(slot)
    except Exception:
        _release_progress(slot, aborted=True)
        raise


def repair_listing_data():
    """
    One-time repair of two things older syncs stored wrongly (runs on a
    background thread at startup, see EpicGamesPlugin.on_startup):

      * platform_slug: the store link. Re-read from Epic's public store API
        (no login needed), which knows the real page slug.
      * release_date: Epic only knows when an item was listed in its catalog,
        so every stored date is a listing date. They are cleared and the games
        queued for core's metadata backfill (PCGamingWiki, then Steam), which
        finds the real release date.

    The slug/clear stage runs once (config.json flag, set only when most
    lookups worked, so an offline start retries). The backfill stage is driven
    purely by DB state (Epic game, no release date, backfill not yet attempted)
    so an interrupted or cancelled run resumes on the next start. Progress
    shows in the top-bar button (see _claim_progress). Never raises.
    """
    slot = None
    try:
        from database import get_db, update_game_data
        cfg = load_config() or {}
        if (cfg.get(_REPAIR_KEY) or 0) >= 2:
            return
        if not cfg.get(_REPAIR_KEY):
            db = get_db()
            rows = db.execute(
                "SELECT appid, platform_ns, platform_slug, release_date FROM games "
                "WHERE platform = 'epic_games' AND platform_ns IS NOT NULL AND platform_ns != ''"
            ).fetchall()
            db.close()

            by_ns = {}
            for r in rows:
                by_ns.setdefault(r['platform_ns'], []).append(r)
            slot = _claim_progress(len(by_ns))
            ok = 0
            slugs = {}
            for ns in by_ns:
                if slot and slot[2].is_set():
                    log.info('Epic listing repair: cancelled during store lookups, will retry next start')
                    _release_progress(slot, aborted=True)
                    return
                slug = _fetch_epic_store_data(ns).get('platform_slug')
                if slug:
                    ok += 1
                    slugs[ns] = slug
                if slot:
                    with slot[1]:
                        slot[0]['done'] += 1
                        if not slug:
                            slot[0]['failed'] += 1
                time.sleep(0.3)
            if by_ns and ok < len(by_ns) / 2:
                log.warning(f'Epic listing repair: only {ok}/{len(by_ns)} store lookups worked, will retry next start')
                _release_progress(slot, aborted=True)
                return

            fixed = cleared = 0
            for ns, games in by_ns.items():
                for g in games:
                    fields = {}
                    if slugs.get(ns) and slugs[ns] != g['platform_slug']:
                        fields['platform_slug'] = slugs[ns]
                        fixed += 1
                    if g['release_date']:
                        fields['release_date'] = None
                        fields['meta_backfill_fetched'] = '0'
                        cleared += 1
                    if fields:
                        update_game_data(g['appid'], **fields)
            log.info(f'Epic listing repair: {fixed} store links fixed, {cleared} listing dates cleared')
            cfg = load_config() or {}
            cfg[_REPAIR_KEY] = 1
            _save_config_data(cfg)
            _release_progress(slot)
            slot = None

        db = get_db()
        pending = [r['appid'] for r in db.execute(
            "SELECT appid FROM games WHERE platform = 'epic_games' AND release_date IS NULL "
            "AND (meta_backfill_fetched IS NULL OR meta_backfill_fetched IN ('0', 'retry')) "
            "ORDER BY appid"
        ).fetchall()]
        db.close()
        if not pending:
            _apply_epic_release_dates()
            return
        slot = _claim_progress(len(pending))
        if slot is None:
            try:
                from library import _bulk_op_state  # noqa: F401
                log.info('Epic listing repair: another bulk operation is running, backfill will resume next start')
                return
            except Exception:
                pass   # core without the shared slot: run with no progress bar

        def _cb(event, _data, _total):
            if not slot:
                return
            with slot[1]:
                if event == 'done':
                    slot[0]['done'] += 1
                elif event == 'failed':
                    slot[0]['failed'] += 1
                elif event == 'rate_limit':
                    slot[0]['rate_limit_hit'] = True

        from scrapers import _backfill_batch
        log.info(f'Epic listing repair: backfilling release dates for {len(pending)} games')
        result = _backfill_batch(pending, slot[2] if slot else None, _cb,
                                 inter_delay=3.0, rerun=True)
        _release_progress(slot, aborted=result.get('aborted', False), result=result)
        slot = None
        log.info(f'Epic listing repair: backfill finished {result}')
        # Only once nothing is left to backfill: a Stop click ends the batch
        # early without marking it aborted, and the date pass must not run on
        # a half-done backfill (it would not break anything, but the flag
        # would then say the repair is finished when it is not).
        db = get_db()
        left = db.execute(
            "SELECT COUNT(*) FROM games WHERE platform = 'epic_games' AND release_date IS NULL "
            "AND (meta_backfill_fetched IS NULL OR meta_backfill_fetched IN ('0', 'retry'))"
        ).fetchone()[0]
        db.close()
        if left:
            log.info(f'Epic listing repair: {left} games still to backfill, will resume next start')
        else:
            _apply_epic_release_dates()
    except Exception as e:
        _release_progress(slot, aborted=True)
        log.warning(f'Epic listing repair failed: {e}')


def fetch_description(appid, platform_id):
    """Fetch a short description for an Epic game via the store GraphQL API."""
    from database import get_db
    db  = get_db()
    row = db.execute(
        "SELECT platform_ns FROM games WHERE appid = ? AND platform = 'epic_games'",
        (appid,)
    ).fetchone()
    db.close()
    if not row or not row['platform_ns']:
        return None
    data = _fetch_epic_store_data(row['platform_ns'])
    return data.get('_description') or None


def _fetch_epic_ratings(namespace):
    """Fetch user ratings via Epic store GraphQL (no auth required). Returns partial meta dict or {}."""
    import math
    try:
        resp = requests.post(
            _STORE_GRAPHQL_URL,
            headers=_STORE_GQL_HEADERS,
            json={'query': _RATINGS_QUERY, 'variables': {'sandboxId': namespace, 'locale': 'en-US'}},
            timeout=10,
        )
        log.info(f'Epic ratings HTTP {resp.status_code} for {namespace!r}: {resp.text[:300]}')
        if resp.status_code != 200:
            return {}
        result = ((resp.json().get('data') or {})
                  .get('RatingsPolls', {})
                  .get('getProductResult')) or {}
        avg = result.get('averageRating')
        if avg is None:
            log.info(f'Epic ratings: no averageRating for {namespace!r}')
            return {}
        pct   = round(avg / 5 * 100)
        total = sum(p.get('total', 0) for p in (result.get('pollResult') or []))
        p     = avg / 5
        weighted = round((p - (p - 0.5) * (2 ** (-math.log10(total + 1)))) * 100)
        return {
            'review_percentage':  pct,
            'review_score':       review_score_label(pct, total),
            'total_reviews':      total,
            'weighted_percentage': weighted,
        }
    except Exception as e:
        log.warning(f'Epic ratings fetch failed for {namespace!r}: {e}')
        return {}


def _extract_metadata(entry):
    """
    Pull the useful fields from a catalog entry dict.
    Returns a meta dict ready for update_game_data (without meta_fetched).
    """
    meta = {}

    # customAttributes holds developerName/publisherName/genres as typed values;
    # the top-level developer/publisher fields are often empty.
    custom = {k: (v.get('value') or '') for k, v in entry.get('customAttributes', {}).items()}

    devs = (custom.get('developerName') or custom.get('developerDisplayName')
            or entry.get('developer', '')).strip()
    if devs:
        meta['developers'] = devs

    publisher = (custom.get('publisherName') or custom.get('publisherDisplayName')
                 or entry.get('publisher', '') or devs).strip()
    if publisher:
        meta['publishers'] = publisher

    genres_str = custom.get('genres', '').strip()
    if genres_str:
        # Epic stores as "Action,Adventure" -- matches our comma-separated convention
        meta['genres'] = genres_str

    url_slug = _clean_epic_slug(entry.get('urlSlug', ''))
    if url_slug and not _HEX_ID_RE.match(url_slug):
        meta['platform_slug'] = url_slug

    # No release_date on purpose. Epic's releaseInfo.dateAdded / creationDate
    # (and the store API's releaseDate) are when the item was listed in Epic's
    # catalog, not when the game came out: a free giveaway of a 2014 game
    # reads as the day the giveaway went up. Leaving it empty lets core's
    # metadata backfill (PCGamingWiki, then the Steam page) fill the real date.

    return meta


_ART_KEY_TYPES = {'vertical': 'DieselGameBoxTall', 'horizontal': 'DieselGameBox'}


def art_urls(appid):
    """Where Epic's own artwork for this game lives, for core's Artwork Sources
    "Store" option: {'vertical': url, 'horizontal': url} from the catalog
    keyImages ({} if not connected, no catalog entry, or nothing usable).
    Core does the download and JPEG conversion; Epic has no icon art."""
    if not is_connected():
        return {}
    session = get_valid_session()
    if not session:
        return {}
    from database import get_db
    db  = get_db()
    row = db.execute(
        "SELECT platform_id, platform_ns FROM games WHERE appid = ? AND platform = 'epic_games'",
        (appid,)
    ).fetchone()
    db.close()
    if not row or not row['platform_id'] or not row['platform_ns']:
        return {}
    entry = _fetch_catalog_entry(session, row['platform_id'], row['platform_ns'])
    urls = {}
    for img in (entry or {}).get('keyImages', []):
        for kind, img_type in _ART_KEY_TYPES.items():
            if img.get('type') == img_type and img.get('url') and kind not in urls:
                urls[kind] = img['url']
    return urls


def _download_epic_art(appid, key_images):
    """Download vertical and horizontal art from Epic keyImages list."""
    type_map = {
        'DieselGameBoxTall': os.path.join(VERTICAL_DIR, f'{appid}.jpg'),
        'DieselGameBox':     os.path.join(HORIZONTAL_DIR, f'{appid}.jpg'),
    }
    for img_entry in key_images:
        img_type = img_entry.get('type', '')
        url      = img_entry.get('url', '')
        dest     = type_map.get(img_type)
        if dest and url and not os.path.exists(dest):
            try:
                resp = requests.get(url, timeout=20)
                resp.raise_for_status()
                save_as_jpg(resp.content, dest)
            except Exception as e:
                log.warning(f'Epic art [{img_type}] download failed: {e}')


# ── Metadata scrape ───────────────────────────────────────────────────────────

def _fetch_catalog_entry(session, platform_id, namespace):
    """Fetch a single catalog entry. Returns dict or None."""
    try:
        url    = _CATALOG_URL_TMPL.format(ns=namespace)
        params = [('id', platform_id), ('country', 'US'), ('locale', 'en'),
                  ('includeDLCDetails', 'false')]
        resp = session.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json().get(platform_id)
        log.warning(f'Epic catalog fetch failed for {platform_id}: HTTP {resp.status_code}')
    except Exception as e:
        log.warning(f'Epic catalog fetch failed for {platform_id}: {e}')
    return None


def scan_junk():
    """Re-check every imported Epic game against the catalog with the current
    _is_dlc_or_extra() filter. Returns [{appid, name, reason}] for the ones that
    now look like DLC / digital extras / non-game apps -- untouched rows only.
    Used by the Blacklist Manager's deep junk scan. [] if not connected."""
    session = get_valid_session()
    if not session:
        return []

    from database import get_db
    db = get_db()
    rows = db.execute(
        "SELECT appid, name, platform_id, platform_ns FROM games "
        "WHERE platform='epic_games' AND platform_id IS NOT NULL "
        "AND COALESCE(playtime_forever,0)=0 AND COALESCE(installed,0)=0 "
        "AND COALESCE(completion_status,'Never Played')='Never Played' "
        "AND (duplicate_of IS NULL OR duplicate_auto=1)"
    ).fetchall()
    db.close()

    by_ns = {}
    for r in rows:
        by_ns.setdefault(r['platform_ns'] or '', []).append(r)

    out = []
    for ns, group in by_ns.items():
        if not ns:
            continue
        cids = [r['platform_id'] for r in group]
        entries = {}
        for i in range(0, len(cids), 50):
            url    = _CATALOG_URL_TMPL.format(ns=ns)
            params = [('id', c) for c in cids[i:i + 50]]
            params += [('country', 'US'), ('locale', 'en'), ('includeMainGameDetails', 'true')]
            try:
                resp = session.get(url, params=params, timeout=25)
                if resp.status_code == 200:
                    entries.update(resp.json())
            except Exception as e:
                log.warning(f'Epic scan_junk catalog fetch [{ns}]: {e}')
            time.sleep(0.1)
        for r in group:
            e = entries.get(r['platform_id'])
            if not e:
                continue
            name = e.get('title') or r['name']
            if _is_dlc_or_extra(e, name):
                cats = {c.get('path') for c in e.get('categories', [])}
                if e.get('mainGameItem') or (cats & {'addons'}):
                    reason = 'DLC / add-on'
                elif 'digitalextras' in cats:
                    reason = 'digital extra (soundtrack / artbook)'
                elif 'software' in cats:
                    reason = 'application, not a game'
                elif 'engines' in cats:
                    reason = 'dev kit'
                else:
                    reason = 'not a standalone game'
                out.append({'appid': r['appid'], 'name': r['name'], 'reason': reason})
    return out


def scrape_single(appid):
    """
    Re-fetch metadata and art for a single Epic game.
    Returns a dict of updated fields, or None on failure.
    Raises RuntimeError when not connected or the session has expired, so
    the route can tell that apart from a genuine fetch failure instead of
    reporting both as the same generic error.
    """
    if not is_connected():
        raise RuntimeError('Epic Games account not connected')
    session = get_valid_session()
    if not session:
        raise RuntimeError('Epic Games session expired — please reconnect')

    from database import get_db
    db  = get_db()
    row = db.execute(
        "SELECT platform_id, platform_slug, platform_ns, release_date FROM games WHERE appid = ? AND platform = 'epic_games'",
        (appid,)
    ).fetchone()
    db.close()
    if not row or not row['platform_ns']:
        return None

    entry = _fetch_catalog_entry(session, row['platform_id'], row['platform_ns'])
    if not entry:
        return None

    from datetime import date
    ns    = row['platform_ns']
    today = date.today().isoformat()
    meta  = _extract_metadata(entry)
    meta['meta_fetched'] = today
    _apply_store_data(meta, _fetch_epic_store_data(ns), row['release_date'])
    meta.update(_fetch_epic_ratings(ns))

    # Re-fetch art if files are missing
    key_images  = entry.get('keyImages', [])
    vert_path   = os.path.join(VERTICAL_DIR, f'{appid}.jpg')
    horiz_path  = os.path.join(HORIZONTAL_DIR, f'{appid}.jpg')
    if not os.path.exists(vert_path) or not os.path.exists(horiz_path):
        try:
            _download_epic_art(appid, key_images)
        except Exception as e:
            log.warning(f'Epic scrape-single: art re-fetch failed for appid {appid}: {e}')

    return meta


# ── Metadata sync (background) ────────────────────────────────────────────────

_meta_state = {'running': False, 'total': 0, 'done': 0, 'updated': 0, 'errors': 0}
_meta_lock  = threading.Lock()


def get_meta_sync_state():
    with _meta_lock:
        return dict(_meta_state)


def start_meta_sync(force=False):
    """Start background metadata sync. Poll get_meta_sync_state() for progress."""
    with _meta_lock:
        if _meta_state['running']:
            return {'status': 'already_running'}
        _meta_state.update({'running': True, 'total': 0, 'done': 0, 'updated': 0, 'errors': 0})

    def _run():
        try:
            _sync_metadata(force=force)
        except Exception as e:
            log.error(f'Epic meta sync thread: {e}', exc_info=True)
        finally:
            with _meta_lock:
                _meta_state['running'] = False

    threading.Thread(target=_run, daemon=True).start()
    return {'status': 'started'}


def _fetch_entitlement_date_maps():
    """
    Fetch all Epic entitlements and return (ns_date_map, cid_date_map).
    Raises RuntimeError on auth failure or unexpected response.
    """
    from datetime import datetime

    tokens = load_epic_tokens()
    if not tokens:
        raise RuntimeError('Epic account not connected')
    account_id = tokens.get('account_id', '')
    if not account_id:
        raise RuntimeError('No Epic account ID stored')
    session = get_valid_session()
    if not session:
        raise RuntimeError('Could not refresh Epic token')

    r = session.get(
        f'https://entitlement-public-service-prod08.ol.epicgames.com'
        f'/entitlement/api/account/{account_id}/entitlements',
        params={'count': 5000},
        timeout=30,
    )
    r.raise_for_status()
    entitlements = r.json()
    if not isinstance(entitlements, list):
        raise RuntimeError('Unexpected entitlements response format')

    log.info(f'Epic import-dates: {len(entitlements)} entitlements received')

    # Assets API and entitlements API use different catalogItemIds for the same game,
    # but both share the same namespace. Match by namespace; catalogItemId as fallback.
    # Exclude the top-level 'epic' namespace (platform-wide items, not individual games).
    ns_date_map  = {}
    cid_date_map = {}
    for ent in entitlements:
        ns  = ent.get('namespace', '')
        cid = ent.get('catalogItemId', '')
        raw_date = ent.get('grantDate') or ent.get('createdDate') or ent.get('created')
        if not raw_date:
            continue
        try:
            ts = int(datetime.fromisoformat(raw_date.replace('Z', '+00:00')).timestamp())
        except Exception:
            continue
        if ns and ns != 'epic':
            if ns not in ns_date_map or ts < ns_date_map[ns]:
                ns_date_map[ns] = ts
        if cid:
            if cid not in cid_date_map or ts < cid_date_map[cid]:
                cid_date_map[cid] = ts

    return ns_date_map, cid_date_map


def import_purchase_dates():
    """
    Fetch Epic entitlements and update date_added for all matched library games.
    Returns {'updated': int, 'not_found': int} or {'error': str}.
    """
    from database import get_db, update_game_data

    try:
        ns_date_map, cid_date_map = _fetch_entitlement_date_maps()
    except Exception as e:
        log.warning(f'Epic import-dates: {e}')
        return {'error': str(e)}

    db   = get_db()
    rows = db.execute(
        "SELECT appid, platform_id, platform_ns FROM games WHERE platform = 'epic_games'"
    ).fetchall()
    db.close()

    updated   = 0
    not_found = 0
    for row in rows:
        appid       = row['appid']
        platform_id = row['platform_id'] or ''
        platform_ns = row['platform_ns'] or ''
        ts = cid_date_map.get(platform_id) or ns_date_map.get(platform_ns)
        if ts is None:
            not_found += 1
            continue
        try:
            update_game_data(appid, date_added=ts)
            updated += 1
        except Exception as e:
            log.warning(f'Epic import-dates: DB update failed for {platform_id}: {e}')

    log.info(f'Epic import-dates: {updated} updated, {not_found} not in library | '
             f'debug={{"entitlement_count": {len(ns_date_map)+len(cid_date_map)}, '
             f'"db_epic_count": {len(rows)}, '
             f'"entitlement_sample": {list(cid_date_map.keys())[:10]}}}')
    return {'updated': updated, 'not_found': not_found}


def fetch_purchase_dates_for_appids(appids):
    """
    Return {appid: unix_ts_or_None} for the given Epic appids without updating the DB.
    Raises RuntimeError on auth/API failure.
    """
    from database import get_db

    ns_date_map, cid_date_map = _fetch_entitlement_date_maps()

    db   = get_db()
    ph   = ','.join('?' * len(appids))
    rows = db.execute(
        f'SELECT appid, platform_id, platform_ns FROM games WHERE appid IN ({ph})', appids
    ).fetchall()
    db.close()

    result = {}
    for row in rows:
        ts = (cid_date_map.get(row['platform_id'] or '')
              or ns_date_map.get(row['platform_ns'] or ''))
        result[row['appid']] = ts
    for appid in appids:
        result.setdefault(appid, None)
    return result


def _sync_metadata(force=False):
    session = get_valid_session()
    if not session:
        return

    from database import get_db
    from datetime import date

    db = get_db()
    try:
        if force:
            rows = db.execute(
                "SELECT appid, platform_id, platform_ns, name, release_date FROM games WHERE platform = 'epic_games'"
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT appid, platform_id, platform_ns, name, release_date FROM games "
                "WHERE platform = 'epic_games' AND (meta_fetched IS NULL OR meta_fetched = '0')"
            ).fetchall()

        updated = 0
        errors  = 0
        today   = date.today().isoformat()

        with _meta_lock:
            _meta_state['total'] = len(rows)

        from database import update_game_data
        for row in rows:
            appid       = row['appid']
            platform_id = row['platform_id']
            platform_ns = row['platform_ns'] or ''
            name        = row['name']

            if not platform_ns:
                errors += 1
                with _meta_lock:
                    _meta_state.update({'done': _meta_state['done'] + 1,
                                        'updated': updated, 'errors': errors})
                continue

            entry = _fetch_catalog_entry(session, platform_id, platform_ns)
            if entry is None:
                try:
                    update_game_data(appid, meta_fetched=today)
                except Exception:
                    pass
                errors += 1
            else:
                meta = _extract_metadata(entry)
                meta['meta_fetched'] = today
                _apply_store_data(meta, _fetch_epic_store_data(platform_ns), row['release_date'])
                meta.update(_fetch_epic_ratings(platform_ns))

                try:
                    update_game_data(appid, **meta)
                    log.info(f'Epic metadata: updated {name!r} (appid {appid})')
                    updated += 1
                except Exception as e:
                    log.error(f'Epic metadata: DB update failed for {name!r}: {e}')
                    errors += 1

            with _meta_lock:
                _meta_state.update({'done': _meta_state['done'] + 1,
                                    'updated': updated, 'errors': errors})
            time.sleep(0.5)

    finally:
        db.close()

    log.info(f'Epic metadata sync complete: {updated} updated, {errors} errors')
