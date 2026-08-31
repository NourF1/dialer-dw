"""Browser-less ReadyMode client.

Ported from the production ListKit client (readymode_http.py), with three
deliberate changes for warehouse use:
  * base_url is a constructor argument, not a module constant
  * network calls retry with exponential backoff        (chunk 3 — not yet added)
  * NO type coercion and NO row filtering — every row ReadyMode sends is
    returned, with every value as the string the source rendered. Casting
    and cleaning belong in dbt, where they are testable and reversible.

BOUNDARY: this module knows about HTTP and ReadyMode's HTML/CSV. It knows
nothing about BigQuery, Airflow, or dbt. If you ever need to import
google.cloud here, the boundary has been crossed.

Build status: chunk 1 of 3 — skeleton + authentication.
  TODO chunk 2: fetch_call_log() and fetch_dialer_report()
  TODO chunk 3: tenacity retries + schema-drift detection
"""
from __future__ import annotations

import csv  # noqa: F401  (used in chunk 2)
import io  # noqa: F401  (used in chunk 2)
import re  # noqa: F401  (used in chunk 2)
from datetime import date  # noqa: F401  (used in chunk 2)

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")


class LoginError(RuntimeError):
    """Authentication failed — bad creds, 2FA/CAPTCHA, or IP block.

    Distinct from ReadymodeFormatError on purpose: this one means a human must
    fix credentials, so Phase 4 should alert and NOT retry.
    """


class ReadymodeFormatError(RuntimeError):
    """A report endpoint returned something unexpected — ReadyMode likely
    changed an internal form field, export template, or table layout.

    Means the parser needs updating. Retrying will not help.
    """


class ReadymodeClient:
    """Authenticated HTTP client for a single ReadyMode tenant."""

    def __init__(self, base_url: str, user: str, password: str):
        # Normalise once, at the boundary — every method below can then assume
        # a clean base and never produce a double slash.
        self.base = base_url.rstrip("/")
        self._user = user
        self._password = password
        self._session: requests.Session | None = None

    def login(self) -> None:
        """Authenticate and store a session, bumping any existing one.

        Returns None by design: the session is an implementation detail.
        Callers must never hold or pass it, or two code paths could share one
        session and neither would know which broke it.
        """
        s = requests.Session()
        s.headers.update({
            "User-Agent": UA,
            # Origin + Referer are MANDATORY. Without them the server returns
            # 500 "cURL error 3: URL malformed" — which is ReadyMode's internal
            # proxy failing, not a malformed URL. It is a crude anti-CSRF check.
            "Origin": self.base,
            "Referer": f"{self.base}/login_new/",
        })

        # 1. GET first — seeds the PHPSESSID cookie. POSTing without it means
        #    the server has no session to attach the login to.
        s.get(f"{self.base}/login_new/")

        # 2. The login form. The non-obvious fields were captured from the live
        #    UI; ReadyMode rejects the POST without them.
        form = {
            "autoequals": "WebRTC",
            "user_tz": "America/New_York",
            "use_phone_module": "auto",
            "then": "",
            "login_account": self._user,
            "login_password": self._password,
        }
        r = s.post(f"{self.base}/login_new/", data=form, allow_redirects=True)

        # 3. ReadyMode allows ONE active session per user. A second login hits
        #    an interstitial; re-POST with logout_other_sessions to force it.
        #    NOTE: this EVICTS the other session — see the scheduling warning
        #    in the README before running this near liskit-daily-http's window.
        if "already logged in" in r.text.lower():
            forced = dict(form, login_as_admin="", logout_other_sessions="on")
            s.post(f"{self.base}/login_new/", data=forced, allow_redirects=True)

        # 4. Verify against page CONTENT, not the status code.
        #    A FAILED login returns 200 OK with the login page in the body, so
        #    raise_for_status() would sail straight past it and we would land an
        #    empty partition and a silently empty fact table. Check for a marker
        #    that only appears once actually authenticated.
        dash = s.get(f"{self.base}/")
        if "hotbar_logout" not in dash.text and "SIGN OUT" not in dash.text:
            raise LoginError(
                "authentication failed — still on login page after force-login"
            )

        self._session = s


if __name__ == "__main__":
    # Checkpoint 1 harness — remove once run_extract.py exists.
    import os

    client = ReadymodeClient(
        os.environ["READYMODE_URL"],
        os.environ["READYMODE_USER"],
        os.environ["READYMODE_PASSWORD"],
    )
    client.login()
    print("login OK — session established")
