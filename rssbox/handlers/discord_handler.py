import logging
import os
import threading
from queue import Queue

import requests

DEFAULT_DISCORD_LOGGER_USERNAME = "ptx"
logger = logging.getLogger(__name__)


class DiscordHandler(logging.Handler):
    def __init__(self, webhook_url: str, minimum_level=logging.INFO):
        super().__init__()
        self.webhook_url = webhook_url
        self.username = os.environ.get(
            "DISCORD_LOGGER_USERNAME", DEFAULT_DISCORD_LOGGER_USERNAME
        )
        self.avatar_url = os.environ.get("DISCORD_LOGGER_AVATAR_URL")
        self.minimum_level = minimum_level

        self.session = requests.Session()
        self.queue = Queue()
        self.listener_thread = threading.Thread(target=self.listener)
        self.listener_thread.daemon = True
        self.listener_thread.start()

    def emit(self, record):
        self.queue.put_nowait(record)

    def listener(self):
        while True:
            record = self.queue.get()
            self.send(record)

    def send(self, record: logging.LogRecord) -> None:
        if record.levelno < self.minimum_level:
            self.queue.task_done()
            return

        formatted_message = self.format(record)
        message = (
            f">>> ```{formatted_message}```"
            if record.exc_info
            else f"> ```{record.levelname}: {formatted_message}```"
        )
        try:
            self.session.post(
                self.webhook_url,
                json={
                    "username": self.username,
                    "content": message,
                    "avatar_url": self.avatar_url,
                },
            )
        except Exception as e:
            logger.error(e)
        finally:
            self.queue.task_done()

    def close(self) -> None:
        self.queue.join()
        return super().close()
