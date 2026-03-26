import os
import re
import json
import logging
from typing import Optional, Tuple
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
from mangum import Mangum

# Load .env
load_dotenv()

# LINE credentials
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    raise RuntimeError("Missing LINE credentials in environment variables.")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(LINE_CHANNEL_SECRET)

DEFAULT_REPLY = "名前とフリガナの両方を含むメッセージを送ってください。（例：名前 : 山田 / フリガナ : ヤマダ）"

# 名前 : xxx / フリガナ : xxx（行末まで）
_NAME_RE = re.compile(r"名前\s*[:：]\s*([^\n]+)")
_FURIGANA_RE = re.compile(r"フリガナ\s*[:：]\s*([^\n]+)")

# FastAPI app
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
    # LINE user id + parsed fields — hook save logic here later
    logging.info(
        "registration_fields user_id=%s name=%s furigana=%s raw=%s",
        user_id,
        json.dumps(name, ensure_ascii=False),
        json.dumps(furigana, ensure_ascii=False),
        json.dumps(message_text, ensure_ascii=False),
    )

    return "お名前とフリガナを受け取りました。ありがとうございます。"


@app.post("/api/callback")
async def callback(request: Request, x_line_signature: str = Header(None)):
    body = await request.body()
    body_str = body.decode("utf-8")

    try:
        events = parser.parse(body_str, x_line_signature)
    except InvalidSignatureError as e:
        logging.error(f"Invalid signature: {e}")
        raise HTTPException(status_code=400, detail={"error": "Invalid signature"})
    except Exception as e:
        logging.error(f"Parsing error: {e}")
        raise HTTPException(status_code=400, detail={"error": str(e)})

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            user_id = event.source.user_id
            message_text = event.message.text.strip()
            
            reply_text = handle_message(user_id, message_text)     
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=reply_text)
            )

    return "OK"


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"[Unhandled Error] {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": str(exc)},
    )

handler = Mangum(app)
