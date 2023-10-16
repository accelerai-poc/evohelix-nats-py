from nats.aio.client import Client as NATS
import logging
from python_settings import settings

logger = logging.getLogger("uvicorn")


class NATSClient(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(NATSClient, cls).__new__(cls)
            cls._instance.client = NATS()
        return cls._instance

    async def connect(self):
        async def error_cb(e):
            logger.warn("Connection Error...", e)

        async def closed_cb():
            logger.info("Connection closed...")

        async def disconnected_cb():
            logger.info("Got disconnected...")

        async def reconnected_cb():
            logger.info("Got reconnected...")

        return await self._instance.client.connect(
            settings.NATS_ENDPOINT,
            reconnected_cb=reconnected_cb,
            disconnected_cb=disconnected_cb,
            error_cb=error_cb,
            closed_cb=closed_cb,
            max_reconnect_attempts=5,
            user=settings.NATS_USERNAME,
            password=settings.NATS_PASSWORD,
        )

    async def start_consuming(self, subject: str, handler):
        logger.info("Start consuming from '{}'...".format(subject))
        return await self.client.subscribe(subject, cb=handler)

    async def init_js(self, subject: str):
        self.js = self.client.jetstream()
        return await self.js.add_stream(subjects=[subject])

    async def publish_js(self, subject: str, msg: str):
        return await self.js.publish(subject, msg.encode())

    async def subscribe_js(self, subject: str, handler):
        logger.info("Subscribed to '{}'...".format(subject))
        return await self.js.subscribe(subject, cb=handler)

    async def publish(self, subject: str, msg: str):
        return await self.client.publish(subject, msg.encode())
