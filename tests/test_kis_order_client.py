import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(r"E:\1_Data")
TOOLS = ROOT / "tools"
for path in (ROOT, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from kis_order_client import KISConfig, KISOrderClient


class _FakeResponse:
    def __init__(self, status_code: int, body: dict):
        self.status_code = int(status_code)
        self._body = dict(body)
        self.headers = {}
        self.text = str(body)

    def json(self):
        return dict(self._body)


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def request(self, **kwargs):
        self.calls.append({"ts": time.monotonic(), **kwargs})
        return self._responses.pop(0)


class KISOrderClientRateLimiterTests(unittest.TestCase):
    def _client(self, *, min_interval=0.05, retries=1, backoff=0.01, shared_budget_file=""):
        cfg = KISConfig(
            app_key="app",
            app_secret="secret",
            cano="12345678",
            acnt_prdt_cd="01",
            mock=True,
            timeout_sec=1.0,
            min_request_interval_sec=min_interval,
            rate_limit_retries=retries,
            rate_limit_backoff_sec=backoff,
            shared_budget_file=shared_budget_file,
        )
        return KISOrderClient(cfg)

    def test_request_json_enforces_min_interval_between_requests(self):
        client = self._client(min_interval=0.05, retries=0, backoff=0.0)
        client._session = _FakeSession(
            [
                _FakeResponse(200, {"ok": True}),
                _FakeResponse(200, {"ok": True}),
            ]
        )

        client._request_json("GET", "/first", headers={})
        client._request_json("GET", "/second", headers={})

        calls = client._session.calls
        self.assertEqual(len(calls), 2)
        self.assertGreaterEqual(calls[1]["ts"] - calls[0]["ts"], 0.045)

    def test_request_json_retries_http_429_with_backoff(self):
        client = self._client(min_interval=0.0, retries=1, backoff=0.01)
        client._session = _FakeSession(
            [
                _FakeResponse(429, {"msg1": "rate limit exceeded"}),
                _FakeResponse(200, {"ok": True}),
            ]
        )

        body, _ = client._request_json("GET", "/retry", headers={})

        self.assertTrue(body["ok"])
        self.assertEqual(len(client._session.calls), 2)

    def test_request_json_enforces_shared_budget_across_clients(self):
        with tempfile.TemporaryDirectory() as td:
            shared_budget_file = str(Path(td) / "kis_rate_budget_mock.json")
            client_a = self._client(min_interval=0.05, retries=0, backoff=0.0, shared_budget_file=shared_budget_file)
            client_b = self._client(min_interval=0.05, retries=0, backoff=0.0, shared_budget_file=shared_budget_file)
            client_a._session = _FakeSession([_FakeResponse(200, {"ok": True})])
            client_b._session = _FakeSession([_FakeResponse(200, {"ok": True})])

            client_a._request_json("GET", "/shared-a", headers={})
            started = time.monotonic()
            client_b._request_json("GET", "/shared-b", headers={})
            elapsed = time.monotonic() - started

            self.assertGreaterEqual(elapsed, 0.045)

    def test_normalize_broker_response_maps_known_rate_limit_error(self):
        meta = KISOrderClient.normalize_broker_response(
            status_code=400,
            path="/oauth2/tokenP",
            body={"msg_cd": "EGW00133", "msg1": "접근토큰 발급 잠시 후 다시 시도하세요(1분당 1회)"},
        )

        self.assertEqual(meta["category"], "RATE_LIMIT")
        self.assertTrue(meta["retryable"])
        self.assertEqual(meta["code"], "EGW00133")


    def test_token_cache_is_saved_encrypted_and_not_plaintext(self):
        client = self._client(min_interval=0.0, retries=0, backoff=0.0)
        with tempfile.TemporaryDirectory() as td:
            cache_path = str(Path(td) / "kis_token_cache_mock.json")
            old_cache = os.environ.get("KIS_TOKEN_CACHE_FILE")
            os.environ["KIS_TOKEN_CACHE_FILE"] = cache_path
            try:
                client._token = "plain-token-value"
                client._token_expire_at = time.time() + 3600
                client._save_cached_token()

                text = Path(cache_path).read_text(encoding="utf-8")
                obj = json.loads(text)
                self.assertEqual(obj.get("token_protection"), "dpapi")
                self.assertIn("access_token_enc_b64", obj)
                self.assertNotIn("access_token", obj)
                self.assertNotIn("plain-token-value", text)
            finally:
                if old_cache is None:
                    os.environ.pop("KIS_TOKEN_CACHE_FILE", None)
                else:
                    os.environ["KIS_TOKEN_CACHE_FILE"] = old_cache

    def test_load_cached_token_supports_encrypted_cache(self):
        client = self._client(min_interval=0.0, retries=0, backoff=0.0)
        with tempfile.TemporaryDirectory() as td:
            cache_path = str(Path(td) / "kis_token_cache_mock.json")
            old_cache = os.environ.get("KIS_TOKEN_CACHE_FILE")
            os.environ["KIS_TOKEN_CACHE_FILE"] = cache_path
            try:
                client._token = "roundtrip-token"
                client._token_expire_at = time.time() + 3600
                client._save_cached_token()

                client2 = self._client(min_interval=0.0, retries=0, backoff=0.0)
                self.assertTrue(client2._load_cached_token())
                self.assertEqual(client2._token, "roundtrip-token")
            finally:
                if old_cache is None:
                    os.environ.pop("KIS_TOKEN_CACHE_FILE", None)
                else:
                    os.environ["KIS_TOKEN_CACHE_FILE"] = old_cache


if __name__ == "__main__":
    unittest.main()
