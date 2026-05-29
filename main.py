from fastapi import FastAPI, Request
import uvicorn

from core import handle_event


app = FastAPI()


@app.post("/webhook")
async def webhook(req: Request):

    data = await req.json()

    event = {
        "text": data.get("text"),
        "user_id": data.get("user_id"),
        "user_name": data.get("user_name"),
        "channel_id": data.get("channel_id"),
        "post_id": data.get("post_id")
    }

    await handle_event(event)

    return {"ok": True}


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
