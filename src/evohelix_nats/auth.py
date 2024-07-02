from python_settings import settings
import requests
from requests.auth import HTTPBasicAuth
import jwt

base_url = f"{settings.KEYCLOAK_URL}/realms/{settings.KEYCLOAK_REALM}"
jwks_client = jwt.PyJWKClient(base_url + "/protocol/openid-connect/certs")


def decode(token):
    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        return jwt.decode(
            token, signing_key.key, algorithms=["RS256"],
            audience=settings.KEYCLOAK_CLIENT_ID,
            issuer=base_url)
    except jwt.exceptions.InvalidTokenError as e:
        return {"error": str(e)}
    except jwt.exceptions.DecodeError as e:
        return {"error": str(e)}


def exchange(token, target_client):
    token_response = requests.post(
        base_url + "/protocol/openid-connect/token",
        auth=HTTPBasicAuth(
            username=settings.KEYCLOAK_CLIENT_ID,
            password=settings.KEYCLOAK_CLIENT_SECRET),
        headers={
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "requested_token_type": "urn:ietf:params:oauth:token-type:refresh_token",  # noqa
            "subject_token": token,
            "audience": target_client
        }
    )
    return token_response.json()


def validate(token, subject):
    decoded = decode(token)
    if "error" in decoded.keys():
        return False
    service_access = settings.SERVICE_NAME in decoded["aud"]
    return decoded if service_access and subject in decoded["roles"] else False
