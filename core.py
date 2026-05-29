import asyncio

from storage import (
    load_queue,
    save_queue,
    load_paused,
    save_paused,
    load_fastrequest,
    save_fastrequest
)

from config import (
    FIRST_REMINDER,
    SECOND_REMINDER,
    REPORT_TIMEOUT,
    REPORT_REPEAT_DELAY,
    REPORT_FINAL_WAIT,
    FASTREQUEST_TIMEOUT
)

from mm_client import MattermostClient


mm = MattermostClient()

_pending = {}


def key(channel_id):
    return str(channel_id)


def ensure_pending(k):
    if k not in _pending:
        _pending[k] = {
            "pre_take": {},
            "in_report": {},
            "fastreq": {}
        }


def find_index(queue, user_id):
    for i, u in enumerate(queue):
        if str(u["id"]) == str(user_id):
            return i
    return None


def mention(user):
    return f"@{user.get('username')}"


def cancel_task(k, bucket, user_id):
    ensure_pending(k)

    task = _pending[k][bucket].get(user_id)

    if task and not task.done():
        task.cancel()

    _pending[k][bucket].pop(user_id, None)


def cancel_all_user_tasks(k, user_id):
    cancel_task(k, "pre_take", user_id)
    cancel_task(k, "in_report", user_id)
    cancel_task(k, "fastreq", user_id)


async def handle_event(event):

    text = (event.get("text") or "").strip()

    if text.startswith("/standup"):
        await standup(event)

    elif text.startswith("/takereport"):
        await takereport(event)

    elif text.startswith("/finished"):
        await finished(event)

    elif text.startswith("/skip"):
        await skip(event)

    elif text.startswith("/delete"):
        await delete(event)

    elif text.startswith("/list"):
        await list_queue(event)

    elif text.startswith("/fastreport"):
        await fastreport(event)

    elif text.startswith("/yes"):
        await yes(event)

    elif text.startswith("/no"):
        await no(event)

    elif text.startswith("/da"):
        await da(event)


async def standup(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    uid = event["user_id"]

    if find_index(queue, uid) is not None:
        mm.send_message(event["channel_id"], "Ты уже в очереди 👍")
        return

    entry = {
        "id": uid,
        "username": event["user_name"],
        "status": "waiting",
        "warned_pre_take": False,
        "awaiting_response": False
    }

    queue.append(entry)

    save_queue(k, queue)

    position = len(queue)

    mm.send_message(
        event["channel_id"],
        f"Добавил тебя в очередь. Позиция #{position}"
    )

    if position == 1:
        await tag_next(event["channel_id"], queue[0])
        await schedule_pre_take(event["channel_id"], k, queue[0])


async def tag_next(channel_id, user):

    mm.send_message(
        channel_id,
        f"🔥 @{user['username']}, твоя очередь! "
        f"Нажми /takereport или /skip"
    )


async def schedule_pre_take(channel_id, k, user):

    ensure_pending(k)

    uid = user["id"]

    cancel_task(k, "pre_take", uid)

    async def seq():

        try:

            await asyncio.sleep(FIRST_REMINDER)

            queue = load_queue(k)

            if not queue:
                return

            if str(queue[0]["id"]) != str(uid):
                return

            mm.send_message(
                channel_id,
                f"@{user['username']} если не нажмешь "
                f"/takereport через 5 минут, удалю из очереди 😔"
            )

            await asyncio.sleep(SECOND_REMINDER)

            queue = load_queue(k)

            if not queue:
                return

            if str(queue[0]["id"]) != str(uid):
                return

            removed = queue.pop(0)

            save_queue(k, queue)

            mm.send_message(
                channel_id,
                f"@{removed['username']} удален из очереди 🫣"
            )

            if queue:
                await tag_next(channel_id, queue[0])
                await schedule_pre_take(channel_id, k, queue[0])

        except asyncio.CancelledError:
            return

    task = asyncio.create_task(seq())

    _pending[k]["pre_take"][uid] = task


async def takereport(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    uid = event["user_id"]

    if not queue:
        mm.send_message(event["channel_id"], "Очередь пустая")
        return

    idx = find_index(queue, uid)

    if idx is None:
        mm.send_message(event["channel_id"], "Ты не в очереди")
        return

    if idx != 0:
        mm.send_message(event["channel_id"], "Пока не твоя очередь 🙂")
        return

    queue[0]["status"] = "in_report"
    queue[0]["awaiting_response"] = False

    save_queue(k, queue)

    cancel_task(k, "pre_take", uid)

    mm.send_message(
        event["channel_id"],
        "Ты взял отчет. Когда закончишь нажми /finished"
    )

    await schedule_in_report(event["channel_id"], k, queue[0])


async def schedule_in_report(channel_id, k, user):

    ensure_pending(k)

    uid = user["id"]

    cancel_task(k, "in_report", uid)

    async def seq():

        try:

            await asyncio.sleep(REPORT_TIMEOUT)

            queue = load_queue(k)

            if not queue:
                return

            if str(queue[0]["id"]) != str(uid):
                return

            mm.send_message(
                channel_id,
                f"@{user['username']} ты еще в отчете? "
                f"/da или /no"
            )

            queue[0]["awaiting_response"] = True

            save_queue(k, queue)

            await asyncio.sleep(REPORT_REPEAT_DELAY)

            queue = load_queue(k)

            if not queue:
                return

            if not queue[0]["awaiting_response"]:
                return

            mm.send_message(
                channel_id,
                f"@{user['username']} ответь /da или /no"
            )

            await asyncio.sleep(REPORT_FINAL_WAIT)

            queue = load_queue(k)

            if not queue:
                return

            if queue[0]["awaiting_response"]:

                removed = queue.pop(0)

                save_queue(k, queue)

                mm.send_message(
                    channel_id,
                    f"@{removed['username']} удален из очереди 🫣"
                )

                if queue:
                    await tag_next(channel_id, queue[0])
                    await schedule_pre_take(channel_id, k, queue[0])

        except asyncio.CancelledError:
            return

    task = asyncio.create_task(seq())

    _pending[k]["in_report"][uid] = task


async def finished(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    uid = event["user_id"]

    if not queue:
        return

    if str(queue[0]["id"]) != str(uid):
        mm.send_message(event["channel_id"], "Сейчас не твоя очередь")
        return

    removed = queue.pop(0)

    save_queue(k, queue)

    cancel_all_user_tasks(k, uid)

    mm.send_message(
        event["channel_id"],
        f"@{removed['username']} завершил отчет ✅"
    )

    if queue:
        await tag_next(event["channel_id"], queue[0])
        await schedule_pre_take(event["channel_id"], k, queue[0])

    else:
        mm.send_message(
            event["channel_id"],
            "Очередь пустая 😢"
        )


async def skip(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    uid = event["user_id"]

    if len(queue) <= 1:
        mm.send_message(
            event["channel_id"],
            "Некого пропускать 🙂"
        )
        return

    if str(queue[0]["id"]) != str(uid):
        mm.send_message(
            event["channel_id"],
            "Skip доступен только первому"
        )
        return

    user = queue.pop(0)

    queue.append(user)

    save_queue(k, queue)

    cancel_all_user_tasks(k, uid)

    mm.send_message(
        event["channel_id"],
        f"@{user['username']} перенесен в конец очереди"
    )

    await tag_next(event["channel_id"], queue[0])

    await schedule_pre_take(
        event["channel_id"],
        k,
        queue[0]
    )


async def delete(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    uid = event["user_id"]

    idx = find_index(queue, uid)

    if idx is None:
        mm.send_message(event["channel_id"], "Тебя нет в очереди")
        return

    if idx == 0:
        mm.send_message(
            event["channel_id"],
            "Ты первый. Используй /skip"
        )
        return

    removed = queue.pop(idx)

    save_queue(k, queue)

    cancel_all_user_tasks(k, uid)

    mm.send_message(
        event["channel_id"],
        f"@{removed['username']} удален из очереди"
    )


async def list_queue(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    if not queue:
        mm.send_message(event["channel_id"], "Очередь пустая")
        return

    lines = []

    for i, user in enumerate(queue, start=1):

        if i == 1 and user["status"] == "in_report":
            lines.append(
                f"{i}) @{user['username']} (в отчете)"
            )
        else:
            lines.append(
                f"{i}) @{user['username']}"
            )

    mm.send_message(
        event["channel_id"],
        "\n".join(lines)
    )


async def fastreport(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    new_user = {
        "id": event["user_id"],
        "username": event["user_name"],
        "status": "in_report"
    }

    if not queue or queue[0]["status"] != "in_report":

        queue.insert(0, new_user)

        save_queue(k, queue)

        mm.send_message(
            event["channel_id"],
            f"@{new_user['username']} зашел вне очереди 🚀"
        )

        await schedule_in_report(
            event["channel_id"],
            k,
            new_user
        )

        return

    current = queue[0]

    save_fastrequest(
        k,
        {
            "current": current,
            "new": new_user
        }
    )

    mm.send_message(
        event["channel_id"],
        f"@{current['username']} "
        f"пропустить @{new_user['username']}? "
        f"/yes или /no"
    )


async def yes(event):

    k = key(event["channel_id"])

    fr = load_fastrequest(k)

    if not fr:
        return

    current = fr["current"]

    if str(current["id"]) != str(event["user_id"]):
        return

    queue = load_queue(k)

    paused = queue.pop(0)

    save_paused(k, paused)

    new_user = fr["new"]

    queue.insert(0, new_user)

    save_queue(k, queue)

    save_fastrequest(k, None)

    mm.send_message(
        event["channel_id"],
        f"@{new_user['username']} можешь заходить 🚀"
    )

    await schedule_in_report(
        event["channel_id"],
        k,
        new_user
    )


async def no(event):

    k = key(event["channel_id"])

    fr = load_fastrequest(k)

    if fr:

        current = fr["current"]

        if str(current["id"]) == str(event["user_id"]):

            mm.send_message(
                event["channel_id"],
                f"@{fr['new']['username']} "
                f"тебя пока не пропустили 😔"
            )

            save_fastrequest(k, None)

            return

    queue = load_queue(k)

    if not queue:
        return

    if str(queue[0]["id"]) != str(event["user_id"]):
        return

    if not queue[0]["awaiting_response"]:
        return

    removed = queue.pop(0)

    save_queue(k, queue)

    cancel_all_user_tasks(k, removed["id"])

    mm.send_message(
        event["channel_id"],
        f"@{removed['username']} покинул отчет"
    )

    if queue:
        await tag_next(event["channel_id"], queue[0])
        await schedule_pre_take(
            event["channel_id"],
            k,
            queue[0]
        )


async def da(event):

    k = key(event["channel_id"])

    queue = load_queue(k)

    if not queue:
        return

    if str(queue[0]["id"]) != str(event["user_id"]):
        return

    if not queue[0]["awaiting_response"]:
        return

    queue[0]["awaiting_response"] = False

    save_queue(k, queue)

    cancel_task(
        k,
        "in_report",
        event["user_id"]
    )

    mm.send_message(
        event["channel_id"],
        "Продолжаем ждать 👍"
    )

    await schedule_in_report(
        event["channel_id"],
        k,
        queue[0]
    )
