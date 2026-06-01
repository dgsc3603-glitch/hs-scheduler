import json
import urllib.error
import urllib.request


class CloudflareD1Client:
    def __init__(self, account_id="", database_id="", api_token="", timeout_seconds=8):
        self.account_id = str(account_id or "").strip()
        self.database_id = str(database_id or "").strip()
        self.api_token = str(api_token or "").strip()
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self):
        return bool(self.account_id and self.database_id and self.api_token)

    @property
    def base_url(self):
        return (
            f"https://api.cloudflare.com/client/v4/accounts/{self.account_id}/d1/database/{self.database_id}"
        )

    def query(self, sql, params=None):
        if not self.enabled:
            return []

        payload = {"sql": sql, "params": params or []}
        request = urllib.request.Request(
            f"{self.base_url}/query",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"D1 query failed: {exc.code} {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"D1 query failed: {exc}") from exc

        body = json.loads(raw) if raw else {}
        if not body.get("success", False):
            raise RuntimeError(f"D1 query failed: {body}")

        result = body.get("result") or []
        rows = []
        for item in result:
            if isinstance(item, dict) and isinstance(item.get("results"), list):
                rows.extend(item["results"])
        return rows

    def execute(self, sql, params=None):
        return self.query(sql, params=params)
