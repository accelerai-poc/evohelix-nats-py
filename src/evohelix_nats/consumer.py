from nats.aio.client import Client as NATS
import logging

logger = logging.getLogger("uvicorn")


class NATSConsumer(object):
    def __init__(self, endpoint, username, password) -> None:
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.client = NATS()

    async def connect(self):
        async def error_cb(e):
            logger.warn("Connection Error...", e)

        async def closed_cb():
            logger.info("Connection closed...")

        async def disconnected_cb():
            logger.info("Got disconnected...")

        async def reconnected_cb():
            logger.info("Got reconnected...")

        await self.client.connect(
            self.endpoint,
            reconnected_cb=reconnected_cb,
            disconnected_cb=disconnected_cb,
            error_cb=error_cb,
            closed_cb=closed_cb,
            max_reconnect_attempts=5,
            user=self.username,
            password=self.password,
        )

    async def start_consuming(self, subject, handler):
        logger.info("Start consuming from '{}'...".format(subject))
        return await self.client.subscribe(subject, cb=handler)
