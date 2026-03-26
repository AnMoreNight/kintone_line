"""
LINE webhook (FastAPI). Entry point for Vercel: `app` at project root (see Vercel FastAPI docs).
"""
import os
import re
import json
import logging
from typing import Optional, Tuple

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_REPLY = (
    "名前とフリガナの両方を含むメッセージを送ってください。"
    "（例：名前 : 山田 / フリガナ : ヤマダ）"
)

_NAME_RE = re.compile(r"名前\s*[:：]\s*([^\n]+)")
_FURIGANA_RE = re.compile(r"フリガナ\s*[:：]\s*([^\n]+)")

_line_bot_api: Optional[LineBotApi] = None
_parser: Optional[WebhookParser] = None


def _get_line_clients() -> Tuple[Optional[LineBotApi], Optional[WebhookParser]]:
    """Lazy init so the app can boot on Vercel even if env vars are missing (misconfiguration)."""
    global _line_bot_api, _parser
    if _parser is not None and _line_bot_api is not None:
        return _line_bot_api, _parser

    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
    secret = os.getenv("LINE_CHANNEL_SECRET")
    if not token or not secret:
        logger.error(
            "LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET are not set. "
            "Set them in the Vercel project Environment Variables (not only .env locally)."
        )
        return None, None

    _line_bot_api = LineBotApi(token)
    _parser = WebhookParser(secret)
    return _line_bot_api, _parser


app = FastAPI()


@app.get("/")
async def health_check():
    return {"status": "ok"}


def extract_name_furigana(text: str) -> Tuple[Optional[str], Optional[str]]:
    m_name = _NAME_RE.search(text)
    m_furi = _FURIGANA_RE.search(text)
    name = m_name.group(1).strip() if m_name else None
    furigana = m_furi.group(1).strip() if m_furi else None
    return name, furigana


def handle_message(user_id: str, message_text: str) -> str:
    if "名前" not in message_text or "フリガナ" not in message_text:
        return DEFAULT_REPLY

    name, furigana = extract_name_furigana(message_text)
    logger.info(
        "registration_fields user_id=%s name=%s furigana=%s raw=%s",
        user_id,
        json.dumps(name, ensure_ascii=False),
        json.dumps(furigana, ensure_ascii=False),
        json.dumps(message_text, ensure_ascii=False),
    )

    return "お名前とフリガナを受け取りました。ありがとうございます。"


@app.post("/api/callback")
async def callback(
    request: Request,
    x_line_signature: Optional[str] = Header(None, alias="X-Line-Signature"),
):
    line_bot_api, parser = _get_line_clients()
    if not line_bot_api or not parser:
        # Acknowledge webhook so LINE does not endlessly retry; fix env vars to get replies.
        logger.error("Webhook received but LINE credentials are not configured.")
        return JSONResponse(content={"status": "ignored", "reason": "missing_line_env"})

    if not x_line_signature:
        logger.warning("Missing X-Line-Signature header")
        raise HTTPException(status_code=400, detail={"error": "Missing signature"})

    body = await request.body()
    body_str = body.decode("utf-8")

    try:
        events = parser.parse(body_str, x_line_signature)
    except InvalidSignatureError as e:
        logger.error("Invalid signature: %s", e)
        raise HTTPException(status_code=400, detail={"error": "Invalid signature"})
    except Exception as e:
        logger.exception("Parsing error: %s", e)
        raise HTTPException(status_code=400, detail={"error": "Invalid request body"})

    for event in events:
        if not isinstance(event, MessageEvent) or not isinstance(event.message, TextMessage):
            continue
        user_id = event.source.user_id
        message_text = event.message.text.strip()
        reply_text = handle_message(user_id, message_text)
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=reply_text),
            )
        except LineBotApiError as e:
            # Expired reply_token, invalid token, etc. — log and continue; still return 200.
            logger.exception("LINE reply_message failed: %s", e)

    return JSONResponse(content={"status": "ok"})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("[Unhandled Error] %s", exc)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"},
    )
