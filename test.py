import os, requests
from dotenv import load_dotenv
load_dotenv(".env")

token = os.getenv("TELEGRAM_BOT_TOKEN")
if not token:
    raise SystemExit("Нет TELEGRAM_BOT_TOKEN в .env")

data = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=20).json()
res = data.get("result", [])

print("updates:", len(res))
for u in res[-30:]:
    msg = u.get("message") or u.get("channel_post") or {}
    chat = msg.get("chat") or {}
    text = msg.get("text") or msg.get("caption") or ""
    if chat:
        print("chat_id:", chat.get("id"),
              "| type:", chat.get("type"),
              "| title:", chat.get("title"),
              "| text:", text[:40])