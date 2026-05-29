import requests

from config import MM_URL, MM_TOKEN


class MattermostClient:

    def __init__(self):
        self.base_url = MM_URL

        self.headers = {
            "Authorization": f"Bearer {MM_TOKEN}",
            "Content-Type": "application/json"
        }

    def send_message(self, channel_id, text):
        payload = {
            "channel_id": channel_id,
            "message": text
        }

        requests.post(
            f"{self.base_url}/api/v4/posts",
            headers=self.headers,
            json=payload,
            timeout=10
        )
