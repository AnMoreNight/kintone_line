"""
Kintone REST API: find a record by 名前 + フリガナ, then set the LINE user id on LINEユーザーID.

Field codes are fixed in this module: 名前, フリガナ, LINEユーザーID.

Records from GET /k/v1/records.json use SINGLE_LINE_TEXT for 名前 / フリガナ (see sample.json
from the same API). Values often use full-width space (U+3000) between parts; user input is
normalized the same way before querying.

If the LINEユーザーID field does not exist on the app yet, it is created (1行テキスト) via
preview form API + deploy. That requires the API token to have **アプリ管理** (manage app),
not only record permissions. See:
https://kintone.dev/en/docs/kintone/rest-api/apps/add-form-fields
https://kintone.dev/en/docs/kintone/rest-api/apps/settings/deploy-app-settings
"""
from __future__ import annotations

import logging
import os
import re
import sys
from dotenv import load_dotenv
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote, urlencode

import requests

load_dotenv()
logger = logging.getLogger(__name__)

# Ensure INFO logs go to stdout on Vercel when this module is imported before main configures logging.
if not logging.root.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "linebot-kintone/1.0"})

# Kintone フィールドコード（固定）
FIELD_NAME = "名前"
FIELD_FURIGANA = "フリガナ"
FIELD_LINE_USER_ID = "LINEユーザーID"

# After first successful check or create, skip GET on each request (per serverless instance).
_line_user_id_field_ready: Optional[bool] = None


def _base_url() -> Optional[str]:
    raw = os.getenv("KINTONE_BASE_URL", "").strip().rstrip("/")
    if not raw:
        return None
    return raw


def _app_id() -> Optional[str]:
    v = os.getenv("KINTONE_APP_ID", "").strip()
    return v or None


def _api_token() -> Optional[str]:
    v = os.getenv("KINTONE_API_TOKEN", "").strip()
    return v or None


def _kintone_configured() -> bool:
    return bool(_base_url() and _app_id() and _api_token())


def normalize_kintone_field_value(value: str) -> str:
    """
    Match how 名前 / フリガナ are stored in app 6 (sample records): strip, then collapse
    any run of whitespace (half/full) to a single ideographic space (　).
    """
    s = value.strip()
    if not s:
        return s
    return re.sub(r"\s+", "\u3000", s)


def _escape_query_string(value: str) -> str:
    """Escape double quotes and backslashes for Kintone query literals."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _build_query(name: str, furigana: str) -> str:
    nf = FIELD_NAME
    ff = FIELD_FURIGANA
    n = _escape_query_string(name.strip())
    f = _escape_query_string(furigana.strip())
    return f'{nf} = "{n}" and {ff} = "{f}"'


def _headers() -> Dict[str, str]:
    """POST/PUT with JSON body."""
    return {
        "X-Cybozu-API-Token": _api_token() or "",
        "Content-Type": "application/json",
    }


def _headers_get() -> Dict[str, str]:
    """GET requests: API token only (no Content-Type — matches Postman; some proxies mishandle GET + JSON Content-Type)."""
    return {
        "X-Cybozu-API-Token": _api_token() or "",
    }


def _build_records_get_url(base: str, app_id: str, query: str) -> str:
    """
    Build the same GET URL shape as Postman: app, totalCount, then query.
    Use quote_via=quote so spaces become %20 (not +), which matches browser/Postman exports for Kintone.
    """
    qs = urlencode(
        [
            ("app", str(app_id)),
            ("totalCount", "true"),
            ("query", query),
        ],
        quote_via=quote,
        safe="",
    )
    return f"{base}/k/v1/records.json?{qs}"


def _app_id_int() -> int:
    aid = _app_id() or "0"
    return int(aid) if str(aid).isdigit() else 0


def _get_form_field_codes() -> Optional[Dict[str, Any]]:
    """GET live form fields; returns properties dict or None on error."""
    if not _kintone_configured():
        return None
    base = _base_url()
    app_id = _app_id()
    url = f"{base}/k/v1/app/form/fields.json?{urlencode({'app': app_id})}"
    logger.info("Kintone API -> GET /k/v1/app/form/fields.json app=%s", app_id)
    try:
        r = _SESSION.get(url, headers=_headers_get(), timeout=30)
    except requests.RequestException as e:
        logger.exception("Kintone GET form fields failed: %s", e)
        return None
    logger.info("Kintone API <- GET /k/v1/app/form/fields.json HTTP %s", r.status_code)
    if r.status_code != 200:
        logger.error(
            "Kintone GET form fields: status=%s body=%s",
            r.status_code,
            r.text[:2000],
        )
        return None
    try:
        data = r.json()
    except ValueError:
        logger.error("Kintone GET form fields: invalid JSON")
        return None
    return data.get("properties") or {}


def ensure_line_user_id_field_exists() -> bool:
    """
    If LINEユーザーID is not on the app form, add it (SINGLE_LINE_TEXT) and deploy preview → live.
    Requires API token with app management permission for add+deploy.
    """
    global _line_user_id_field_ready
    if _line_user_id_field_ready:
        return True
    if not _kintone_configured():
        return False

    props = _get_form_field_codes()
    if props is None:
        return False
    if FIELD_LINE_USER_ID in props:
        logger.info("Kintone: field %s already exists on app", FIELD_LINE_USER_ID)
        _line_user_id_field_ready = True
        return True

    base = _base_url()
    app_int = _app_id_int()
    add_url = f"{base}/k/v1/preview/app/form/fields.json"
    logger.info(
        "Kintone API -> POST /k/v1/preview/app/form/fields.json app=%s field=%s",
        app_int,
        FIELD_LINE_USER_ID,
    )
    add_body: Dict[str, Any] = {
        "app": app_int,
        "properties": {
            FIELD_LINE_USER_ID: {
                "type": "SINGLE_LINE_TEXT",
                "code": FIELD_LINE_USER_ID,
                "label": "LINEユーザーID",
                "noLabel": False,
                "required": False,
                "unique": False,
            }
        },
    }
    try:
        r = _SESSION.post(add_url, headers=_headers(), json=add_body, timeout=60)
    except requests.RequestException as e:
        logger.exception("Kintone add form field failed: %s", e)
        return False
    logger.info("Kintone API <- POST /k/v1/preview/app/form/fields.json HTTP %s", r.status_code)
    if r.status_code != 200:
        logger.error(
            "Kintone POST preview add fields failed: status=%s body=%s",
            r.status_code,
            r.text[:2000],
        )
        return False

    deploy_url = f"{base}/k/v1/preview/app/deploy.json"
    logger.info("Kintone API -> POST /k/v1/preview/app/deploy.json app=%s", app_int)
    deploy_body = {
        "apps": [
            {
                "app": app_int,
                "revision": "-1",
            }
        ]
    }
    try:
        r2 = _SESSION.post(deploy_url, headers=_headers(), json=deploy_body, timeout=120)
    except requests.RequestException as e:
        logger.exception("Kintone deploy app settings failed: %s", e)
        return False
    logger.info("Kintone API <- POST /k/v1/preview/app/deploy.json HTTP %s", r2.status_code)
    if r2.status_code != 200:
        logger.error(
            "Kintone deploy failed: status=%s body=%s",
            r2.status_code,
            r2.text[:2000],
        )
        return False

    logger.info("Kintone: added field %s and deployed app settings", FIELD_LINE_USER_ID)
    _line_user_id_field_ready = True
    return True


def find_record_by_name_furigana(
    name: str,
    furigana: str,
) -> Tuple[Optional[str], Optional[int]]:
    """
    Returns (record_id_str, total_count) or (None, None) on error/no config.
    record_id_str is the numeric record id as string for API use.
    """
    if not _kintone_configured():
        logger.error("Kintone env not fully configured (KINTONE_BASE_URL, KINTONE_APP_ID, KINTONE_API_TOKEN).")
        return None, None

    base = _base_url()
    app_id = _app_id()
    query = _build_query(name, furigana)
    url = _build_records_get_url(base, app_id or "", query)
    logger.info(
        "Kintone API -> GET /k/v1/records.json app=%s fields=[%s,%s] query=%s",
        app_id,
        FIELD_NAME,
        FIELD_FURIGANA,
        query,
    )

    try:
        r = _SESSION.get(url, headers=_headers_get(), timeout=30)
    except requests.RequestException as e:
        logger.exception("Kintone GET records request failed: %s", e)
        return None, None

    logger.info(
        "Kintone API <- GET /k/v1/records.json HTTP %s url=%s",
        r.status_code,
        r.url,
    )
    if r.status_code != 200:
        logger.error(
            "Kintone GET records failed: status=%s body=%s",
            r.status_code,
            r.text[:2000],
        )
        return None, None

    try:
        data = r.json()
    except ValueError:
        logger.error("Kintone GET records: invalid JSON: %s", r.text[:500])
        return None, None

    records = data.get("records") or []
    total = data.get("totalCount")
    try:
        total_int = int(total) if total is not None else len(records)
    except (TypeError, ValueError):
        total_int = len(records)

    if not records:
        logger.info(
            "Kintone: no matching record (totalCount=%s, records_returned=0)",
            total,
        )
        return None, total_int

    if total_int > 1:
        logger.warning(
            "Kintone: multiple records match (%s); updating the first only ($id=%s)",
            total_int,
            records[0].get("$id"),
        )

    rid = records[0].get("$id", {}).get("value")
    if rid is None:
        logger.error("Kintone: first record missing $id: %s", records[0])
        return None, total_int

    logger.info("Kintone: found record $id=%s (totalCount=%s)", rid, total_int)
    return str(rid), total_int


def update_record_line_uid(record_id: str, line_user_id: str) -> bool:
    """PUT /k/v1/record.json — set single-line text field LINEユーザーID."""
    if not _kintone_configured():
        return False

    base = _base_url()
    app_id = _app_id()
    field = FIELD_LINE_USER_ID

    try:
        rid = int(record_id)
    except ValueError:
        logger.error("Invalid Kintone record id: %s", record_id)
        return False

    body: Dict[str, Any] = {
        "app": int(app_id) if str(app_id).isdigit() else app_id,
        "id": rid,
        "record": {
            field: {"value": line_user_id},
        },
    }

    url = f"{base}/k/v1/record.json"
    logger.info(
        "Kintone API -> PUT /k/v1/record.json app=%s id=%s field=%s",
        app_id,
        rid,
        field,
    )
    try:
        r = _SESSION.put(url, headers=_headers(), json=body, timeout=30)
    except requests.RequestException as e:
        logger.exception("Kintone PUT record failed: %s", e)
        return False

    logger.info("Kintone API <- PUT /k/v1/record.json HTTP %s", r.status_code)
    if r.status_code != 200:
        logger.error(
            "Kintone PUT record failed: status=%s body=%s",
            r.status_code,
            r.text[:2000],
        )
        return False

    logger.info(
        "Kintone: updated record $id=%s field[%s]=%r",
        rid,
        field,
        line_user_id,
    )
    return True


def link_line_user_to_kintone(
    line_user_id: str,
    name: Optional[str],
    furigana: Optional[str],
) -> Optional[str]:
    """
    Find record by name + furigana, write LINE user id to LINEユーザーID field.
    Returns a short Japanese message for the LINE reply, or None if name/furigana missing.
    """
    if not name or not furigana:
        return None

    name = normalize_kintone_field_value(name)
    furigana = normalize_kintone_field_value(furigana)
    if not name or not furigana:
        return None

    logger.info(
        "link_line_user: start line_user_id=%s 名前=%r フリガナ=%r",
        line_user_id,
        name,
        furigana,
    )

    if not _kintone_configured():
        logger.error("link_line_user: Kintone env not set; skipping link")
        return (
            "お名前とフリガナを受け取りました。"
            "（Kintone連携の設定が完了していないため、登録はスキップされました。）"
        )

    record_id, total = find_record_by_name_furigana(name, furigana)
    if record_id is None:
        if total is None:
            logger.info("link_line_user: find record failed (API error)")
            return "Kintoneの検索に失敗しました。しばらくしてからお試しください。"
        if total == 0:
            logger.info(
                "link_line_user: no Kintone row matched 名前=%r フリガナ=%r",
                name,
                furigana,
            )
            return (
                "Kintoneに一致する名前・フリガナが見つかりませんでした。"
                "入力内容をご確認ください。"
            )
        logger.info(
            "link_line_user: find record unexpected state total=%s 名前=%r フリガナ=%r",
            total,
            name,
            furigana,
        )
        return "Kintoneの検索に失敗しました。しばらくしてからお試しください。"

    logger.info(
        "link_line_user: ensure LINEユーザーID field exists record_id=%s",
        record_id,
    )
    if not ensure_line_user_id_field_exists():
        return (
            "Kintoneに「LINEユーザーID」フィールドを追加できませんでした。"
            "APIトークンにアプリ管理権限があるか、管理者にフォームに1行テキスト"
            f"（フィールドコード「{FIELD_LINE_USER_ID}」）を追加してもらってください。"
        )

    if update_record_line_uid(record_id, line_user_id):
        logger.info("link_line_user: success record_id=%s", record_id)
        return "KintoneにLINE IDを登録しました。ありがとうございます。"

    logger.info("link_line_user: PUT record failed record_id=%s", record_id)
    return "Kintoneの更新に失敗しました。しばらくしてからお試しください。"
