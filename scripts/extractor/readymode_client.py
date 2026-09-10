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

Build status: chunk 2 of 3 — authentication + fetchers.
  TODO chunk 3: tenacity retries + schema-drift detection
"""
from __future__ import annotations

import csv
import io
import logging
import re
from datetime import date, timedelta

import requests

logger = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")

# Call-result type ids that are checked by default on the Call Log report; sending
# them all means the export covers every disposition.
CALL_RESULT_TYPES = ["6", "-2", "3", "141", "1", "2", "5", "7", "138", "139", "-1"]

# Exact export field keys for the "dispo" template (id 13), captured from the
# ExportMenu. Order defines CSV column order.
DISPO_FIELDS = [
    ("Original campaign", "Original campaign"),
    ("Current campaign", "Current campaign"),
    ("u.u_name", "Agent name"),
    ("Log Type", "Log Type"),
    ("Log Time (Date)", "Log Time (Date)"),
]


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


def _mmddyyyy(d: date) -> str:
    return d.strftime("%m/%d/%Y")


def _cells(row_html: str) -> list[str]:
    return [re.sub(r"<[^>]+>", "", c).strip()
            for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.S)]


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

    def _require_session(self) -> requests.Session:
        """Guard function: ensures login() was called prior to fetching reports."""
        if self._session is None:
            raise LoginError("Must call login() before attempting to fetch reports.")
        return self._session

    def _verify_session_alive(self, response_text: str) -> None:
        """Detect if session expired and ReadyMode redirected/rendered the login screen."""
        lowered = response_text.lower()
        if "login_account" in lowered or "login_password" in lowered:
            raise LoginError("Session expired or invalid — server returned login page instead of requested report.")

    def fetch_call_log(self, day: date) -> list[dict]:
        """Fetch call log dispositions for `day`, returning raw list[dict]."""
        s = self._require_session()
        d = _mmddyyyy(day)
        xhr = {"X-Requested-With": "XMLHttpRequest", "Referer": f"{self.base}/+CCS Reports/call_log"}

        # 1) Set the session's Call Log date range (the export inherits it).
        form = [
            ("update", "1"),
            ("report[time_from_d]", d), ("report[time_from_dateonly]", "1"),
            ("report[time_to_d]", d), ("report[time_to_dateonly]", "1"),
            ("report[page]", "0"),
            ("report[restrict_uid]", "0"), ("report[restrict_campaign]", "0"),
            ("report[restrict_batch]", "0"), ("report[sourceFilter]", "-1"),
            ("report[durationFilter]", "-1"), ("report[callTypeFilter]", "_"),
        ] + [("report[types][]", t) for t in CALL_RESULT_TYPES]

        update_res = s.post(f"{self.base}/+CCS Reports/call_log/update", data=form, headers=xhr)
        self._verify_session_alive(update_res.text)

        # 2) Stream the CSV export with the dispo fields.
        payload = [("fieldList[keys][]", k) for k, _ in DISPO_FIELDS] + \
                  [("fieldList[names][]", n) for _, n in DISPO_FIELDS]
        r = s.post(f"{self.base}/+CCS Reports/call_log/ExportMenu/CL.csv", data=payload,
                   headers={"Referer": f"{self.base}/+CCS Reports/call_log"})

        self._verify_session_alive(r.text)

        # Content-Type / HTML body verification: dead sessions return 200 OK with HTML markup
        ctype = r.headers.get("Content-Type", "")
        if "csv" not in ctype.lower() or r.text.lstrip().startswith("<!DOCTYPE") or r.text.lstrip().startswith("<html"):
            raise ReadymodeFormatError(
                f"Dispo CSV export did not return valid CSV (Content-Type={ctype!r}, len={len(r.text)}). "
                "The Call Log export endpoint or dispo field keys likely changed."
            )

        # csv.DictReader preserves native structure/None values without string coercion
        rows = list(csv.DictReader(io.StringIO(r.text)))
        expected = {"Original campaign", "Log Type", "Log Time (Date)"}
        if rows and not expected.issubset(set(rows[0].keys())):
            raise ReadymodeFormatError(
                f"Dispo CSV columns changed — got {list(rows[0].keys())}, expected to include {sorted(expected)}."
            )

        return rows

    def fetch_dialer_report(self, day: date) -> list[dict]:
        """Fetch per-campaign report for `day`, mapping all table columns by header name.

        Lands all columns without value-based filtering. Drops rows strictly when
        cell count does not match header count (structural mismatch) to prevent
        misaligned columns. If no rows are present, returns an empty list to allow
        zero-row partition loads.
        """
        s = self._require_session()
        d = _mmddyyyy(day)
        dialer = f"{self.base}/+CCS Reports/dialer"
        r = s.post(dialer, data={"date_from": d, "date_to": d},
                   headers={"X-Requested-With": "XMLHttpRequest", "Referer": dialer})

        self._verify_session_alive(r.text)

        rows = [_cells(rh) for rh in re.findall(r"<tr[^>]*>(.*?)</tr>", r.text, re.S)]
        rows = [c for c in rows if c]
        if not rows:
            logger.warning(
                f"Dialer report returned no table rows for date={day.isoformat()}. "
                "Treating as empty dataset (row_count=0)."
            )
            return []

        header = rows[0]
        header_len = len(header)
        header_lower = [h.lower() for h in header]

        # Drift Detection: Ensure core pipeline columns exist in header
        required_cols = {"playlist", "calls", "connects"}
        missing = required_cols - set(header_lower)
        if missing:
            raise ReadymodeFormatError(
                f"Dialer report header missing expected column(s) {missing}. "
                f"Header was {header}. Readymode renamed a critical column."
            )

        out = []
        structural_skips = 0

        for cells_row in rows[1:]:
            # Structural Guard: Mismatched column count cannot be aligned safely
            if len(cells_row) != header_len:
                structural_skips += 1
                continue

            # Map columns strictly by header name alignment
            row_dict = {col_name: cell_val for col_name, cell_val in zip(header, cells_row) if col_name}
            if row_dict:
                out.append(row_dict)

        if structural_skips > 0:
            logger.warning(
                "Skipped %d rows in dialer report due to structural cell count mismatch "
                "(expected %d columns).",
                structural_skips,
                header_len,
            )

        # Fail loudly if structural format corrupts all table rows
        if structural_skips > 0 and len(out) == 0:
            raise ReadymodeFormatError(
                f"Dialer report table structural mismatch: all {structural_skips} data rows "
                f"had cell counts differing from the {header_len}-column header."
            )

        return out


if __name__ == "__main__":
    import os

    client = ReadymodeClient(
        os.environ["READYMODE_URL"],
        os.environ["READYMODE_USER"],
        os.environ["READYMODE_PASSWORD"],
    )
    client.login()
    print("login OK — session established")

    target_day = date.today() - timedelta(days=1)
    target_date_str = _mmddyyyy(target_day)

    call_logs = client.fetch_call_log(target_day)
    dialer_report = client.fetch_dialer_report(target_day)

    print(f"Date {target_day}: {len(call_logs)} call log rows, {len(dialer_report)} dialer report rows.")

    if call_logs:
        log_dates = {r.get("Log Time (Date)", "").split()[0] for r in call_logs}
        assert log_dates == {target_date_str}, (
            f"Date range mutation failed! Expected records strictly for {target_date_str}, "
            f"but retrieved dates: {log_dates}"
        )
        print(f"Acceptance check passed — all call log records belong strictly to {target_date_str}.")