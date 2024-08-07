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
    return requests.post(
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
    ).json()


def validate(token):
    decoded = decode(token)
    if "error" in decoded.keys():
        return False
    return decoded if settings.SERVICE_NAME in decoded["aud"] else False


def get_service_token(refresh_token=None):
    data = {"grant_type": "client_credentials"}
    if refresh_token:
        data["grant_type"] = "refresh_token"
        data["refresh_token"] = refresh_token
    return requests.post(
        settings.KEYCLOAK_URL + '/realms/' + settings.KEYCLOAK_REALM + '/protocol/openid-connect/token',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        auth=HTTPBasicAuth(
            username=settings.KEYCLOAK_CLIENT_ID,
            password=settings.KEYCLOAK_CLIENT_SECRET),
        data=data
    ).json()
