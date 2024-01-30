from nats.aio.client import Client as NATS
from nats.js.errors import APIError
import logging
import auth
from python_settings import settings

logger = logging.getLogger("uvicorn")


class NATSClient(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(NATSClient, cls).__new__(cls)
            cls._instance.client = NATS()
        return cls._instance

    async def connect(self, enable_jetstream=True):
        async def error_cb(e):
            logger.warn("Connection Error...", e)

        async def closed_cb():
            logger.info("Connection closed...")

        async def disconnected_cb():
            logger.info("Got disconnected...")

        async def reconnected_cb():
            logger.info("Got reconnected...")

        await self.client.connect(
            settings.NATS_ENDPOINT,
            reconnected_cb=reconnected_cb,
            disconnected_cb=disconnected_cb,
            error_cb=error_cb,
            closed_cb=closed_cb,
            max_reconnect_attempts=5,
            user=settings.NATS_USERNAME,
            password=settings.NATS_PASSWORD,
        )
        if enable_jetstream:
            self.js = self.client.jetstream()
            try:
                await self.js.add_stream(
                    name=settings.SERVICE_NAME,
                    subjects=[settings.SERVICE_NAME + ".*"])
            except APIError as error:
                if error["err_code"] != 10058:
                    raise
                await self.js.update_stream(
                    name=settings.SERVICE_NAME,
                    subjects=[settings.SERVICE_NAME + ".*"])

    async def request(self, subject: str, msg: str,
                      token: str, timeout: float = 0.5):
        target = subject.split(".")[0]
        jwt = auth.exchange(token, target)
        headers = {"X-Evo-Authorization": jwt["access_token"]}
        return await self.client.request(subject, msg.encode(), timeout,
                                         headers=headers)

    async def publish(self, subject: str, msg: str, token: str):
        target = subject.split(".")[0]
        jwt = auth.exchange(token, target)
        headers = {"X-Evo-Authorization": jwt["access_token"]}
        return await self.js.publish(subject, msg.encode(), headers=headers)

    async def subscribe(self, subject: str, handler):
        async def auth_middleware(msg):
            token = msg.headers.get("X-Evo-Authorization", "invalid")
            if auth.validate(token, subject):
                return await handler()
            else:
                msg.nak()
        return await self.js.subscribe(subject, cb=auth_middleware)

    async def broadcast(self, subject: str, msg: str, token: str):
        target = subject.split(".")[0]
        jwt = auth.exchange(token, target)
        headers = {"X-Evo-Authorization": jwt["access_token"]}
        return await self.client.publish(subject, msg.encode(),
                                         headers=headers)
