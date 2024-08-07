from nats.aio.client import Client as NATS
from nats.js.api import RetentionPolicy
from nats.js.errors import APIError
import logging
from . import auth
from python_settings import settings

logger = logging.getLogger("uvicorn")


class NATSClient(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(NATSClient, cls).__new__(cls)
            cls._instance.client = NATS()
            cls._instance.access_token = None
            cls._instance.refresh_token = None
        return cls._instance

    def get_auth_token(self):
        if self.access_token:
            decoded = auth.decode(self.access_token)
            if "error" in decoded.keys():
                token_response = auth.get_service_token(self.refresh_token)
                if 'access_token' in token_response.keys():
                    self.access_token = token_response['access_token']
                if 'refresh_token' in token_response.keys():
                    self.refresh_token = token_response['refresh_token']
            return self.access_token
        token_response = auth.get_service_token(self.refresh_token)
        if 'access_token' in token_response.keys():
            self.access_token = token_response['access_token']
        if 'refresh_token' in token_response.keys():
            self.refresh_token = token_response['refresh_token']
        return self.access_token

    async def connect(self, js_subjects=[], js_retention=RetentionPolicy.WORK_QUEUE, use_token=False):
        async def error_cb(e):
            logger.warn("Connection Error...", e)

        async def closed_cb():
            logger.info("Connection closed...")

        async def disconnected_cb():
            logger.info("Got disconnected...")

        async def reconnected_cb():
            logger.info("Got reconnected...")

        if use_token:
            await self.client.connect(
                settings.NATS_ENDPOINT,
                reconnected_cb=reconnected_cb,
                disconnected_cb=disconnected_cb,
                error_cb=error_cb,
                closed_cb=closed_cb,
                max_reconnect_attempts=5,
                token=self.get_auth_token(),
                inbox_prefix=f"_INBOX_{settings.SERVICE_NAME}".encode()
            )
        else:
            await self.client.connect(
                settings.NATS_ENDPOINT,
                reconnected_cb=reconnected_cb,
                disconnected_cb=disconnected_cb,
                error_cb=error_cb,
                closed_cb=closed_cb,
                max_reconnect_attempts=5,
                user=settings.NATS_USERNAME,
                password=settings.NATS_PASSWORD,
                inbox_prefix=f"_INBOX_{settings.SERVICE_NAME}".encode()
            )
        self.js = self.client.jetstream()
        if js_subjects:
            try:
                await self.js.add_stream(
                    name=settings.SERVICE_NAME,
                    retention=js_retention,
                    subjects=js_subjects)
            except APIError as error:
                # Config should not change then!
                if error.err_code != 10058:
                    raise
                await self.js.update_stream(
                    name=settings.SERVICE_NAME,
                    retention=js_retention,
                    subjects=js_subjects)

    async def broadcast(self, subject: str, msg: str, token: str = None):
        target = subject.split(".")[0]
        if not token:
            token = self.get_auth_token()
        jwt = auth.exchange(token, target)
        headers = {"X-Evo-Authorization": jwt["access_token"]}
        return await self.client.publish(subject, msg.encode(),
                                         headers=headers)

    async def request(self, subject: str, msg: str,
                      token: str = None, timeout: float = 0.5):
        target = subject.split(".")[0]
        if not token:
            token = self.get_auth_token()
        jwt = auth.exchange(token, target)
        headers = {"X-Evo-Authorization": jwt["access_token"]}
        return await self.client.request(subject, msg.encode(), timeout,
                                         headers=headers)

    async def publish(self, subject: str, msg: str, token: str = None, js=False):
        target = subject.split(".")[0]
        if not token:
            token = self.get_auth_token()
        jwt = auth.exchange(token, target)
        if "access_token" not in jwt.keys():
            raise RuntimeError(f"Token exchange failed: {jwt}")
        headers = {"X-Evo-Authorization": jwt["access_token"]}
        if js:
            return await self.js.publish(subject, msg.encode(),
                                         headers=headers)
        return await self.client.publish(subject, msg.encode(),
                                         headers=headers)

    async def reply(self, subject: str, msg: str, status: int=200):
        headers = {"X-Evo-Status": str(status)}
        return await self.client.publish(subject, msg.encode(), headers=headers)

    async def subscribe(self, subject: str, handler, js=False):
        async def auth_middleware(msg):
            token = "invalid"
            if msg.headers and "X-Evo-Authorization" in msg.headers.keys():
                token = msg.headers["X-Evo-Authorization"]
            if auth.validate(token):
                await handler(msg, token)
            else:
                if js:
                    await msg.nak()
                else:
                    await self.reply(msg.reply, '{"error": "unauthorized"}', status=401)
        if js:
            return await self.js.subscribe(subject, cb=auth_middleware)
        return await self.client.subscribe(subject, cb=auth_middleware)
