import json
import os

from config import QUEUE_FILE


def ensure_file():
    if not os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f)


def load_all():
    ensure_file()

    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_all(data):
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_state(key):
    data = load_all()
    return data.get(
        key,
        {
            "queue": [],
            "paused": None,
            "fast_request": None
        }
    )


def save_state(key, state):
    data = load_all()
    data[key] = state
    save_all(data)


def load_queue(key):
    return load_state(key).get("queue", [])


def save_queue(key, queue):
    state = load_state(key)
    state["queue"] = queue
    save_state(key, state)


def load_paused(key):
    return load_state(key).get("paused")


def save_paused(key, value):
    state = load_state(key)
    state["paused"] = value
    save_state(key, state)


def load_fastrequest(key):
    return load_state(key).get("fast_request")


def save_fastrequest(key, value):
    state = load_state(key)
    state["fast_request"] = value
    save_state(key, state)
