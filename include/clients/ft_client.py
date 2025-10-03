import os, time, requests
from typing import Any, Dict, Optional
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception
import logging

TOKEN_URL = os.getenv("FT_TOKEN_URL")
API_BASE = os.getenv("FT_API_BASE", "https://api.francetravail.io/")
LOGGER = logging.getLogger(__name__)

# --- Exceptions dédiées ---
class FTHTTPError(Exception):
    pass

class FTNotFound(FTHTTPError):
    """404: ressource/route inexistante pour le payload/env courant -> ne pas retry."""
    pass

class FTRetryable(FTHTTPError):
    """Erreurs transitoires: 429/408/5xx/erreurs réseau -> retry."""
    pass


def _is_retryable_status(status: int) -> bool:
    # 408 Request Timeout, 429 Too Many Requests, 5xx
    return status == 408 or status == 429 or 500 <= status <= 599


class FTClient:
    def __init__(self):
        self.client_id = os.getenv("FT_CLIENT_ID")
        self.client_secret = os.getenv("FT_CLIENT_SECRET")
        self.scope = os.getenv("FT_SCOPE")
        self._token = None
        self._token_exp = 0

    def _need_new_token(self) -> bool:
        return (not self._token) or (time.time() > self._token_exp - 60)

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
    def _fetch_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope
        }
        r = requests.post(
            TOKEN_URL,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30
        )

        print(r)
        r.raise_for_status()
        token = r.json()
        self._token = token["access_token"]
        self._token_exp = time.time() + int(token.get("expires_in", 3600))

    def _auth_header(self):
        if self._need_new_token():
            self._fetch_token()
        return {"Authorization": f"Bearer {self._token}"}

    def _request(
        self,
        method: str,
        path: str,
        accept: str = "application/json",
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = API_BASE.rstrip("/") + "/" + path.lstrip("/")

        base_headers = {
            "Content-Type": "application/json",
            "Accept": accept,
        }
        headers = {**base_headers, **self._auth_header()}

        try:
            r = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params if method.lower() == "get" else None,
                json=payload if method.lower() == "post" else None,
                timeout=60,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Erreur réseau/transitoire -> retry
            raise FTRetryable(str(e)) from e

        # Log court (éviter d'inonder les logs avec des payloads volumineux)
        LOGGER.info("FT %s %s -> %s", method.upper(), url, r.status_code)

        # Gestion fine des statuts
        if r.status_code == 404:
            # Pas de retry -> on laisse le caller décider (souvent: skip)
            detail = None
            try:
                detail = r.text
            except Exception:
                pass
            raise FTNotFound(f"404 Not Found on {url}: {detail}")

        if _is_retryable_status(r.status_code):
            # Respect 'Retry-After' si présent (et laisser tenacity faire l'exponentiel ensuite)
            try:
                retry_after = int(r.headers.get("Retry-After", "0"))
            except ValueError:
                retry_after = 0
            if retry_after > 0:
                LOGGER.warning("429/5xx reçu, sleep Retry-After=%ss avant retry…", retry_after)
                time.sleep(retry_after)
            raise FTRetryable(f"Retryable HTTP {r.status_code} on {url}: {r.text}")

        # Pour les autres 4xx -> lever une erreur non-retryable
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            # 4xx non retryable
            raise FTHTTPError(f"HTTP {r.status_code} on {url}: {r.text}") from e

        # Réponse OK
        if accept == "application/json":
            return r.json()
        return {"raw": r.text}

    # Retenter UNIQUEMENT sur des erreurs retryables
    _retry_policy = dict(
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(lambda e: isinstance(e, FTRetryable)),
        reraise=True,
    )

    @retry(**_retry_policy)
    def get(
        self,
        path: str,
        accept: str = "application/json",
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request("GET", path, accept=accept, params=params)

    @retry(**_retry_policy)
    def post(
        self,
        path: str,
        accept: str = "application/json",
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request("POST", path, accept=accept, payload=payload)
