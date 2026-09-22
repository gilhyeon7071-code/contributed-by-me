from __future__ import annotations

import base64
import ctypes
import hashlib
import json
import os
import threading
import time
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
try:
    import error_pattern_analyzer
except ImportError:
    from tools import error_pattern_analyzer


PROD_BASE_URL = "https://openapi.koreainvestment.com:9443"
MOCK_BASE_URL = "https://openapivts.koreainvestment.com:29443"
ROOT = Path(__file__).resolve().parents[1]


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    ]


_CRYPTPROTECT_UI_FORBIDDEN = 0x01


def _bytes_to_blob(raw: bytes) -> _DATA_BLOB:
    if not raw:
        return _DATA_BLOB(0, None)
    buf = (ctypes.c_byte * len(raw)).from_buffer_copy(raw)
    return _DATA_BLOB(len(raw), ctypes.cast(buf, ctypes.POINTER(ctypes.c_byte)))


def _blob_to_bytes(blob: _DATA_BLOB) -> bytes:
    if not blob.cbData or not blob.pbData:
        return b""
    return ctypes.string_at(blob.pbData, blob.cbData)


def _dpapi_encrypt_text(text: str) -> str:
    raw = str(text or "").encode("utf-8")
    if not raw:
        return ""
    in_blob = _bytes_to_blob(raw)
    out_blob = _DATA_BLOB()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    ok = crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        None,
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("CryptProtectData failed")
    try:
        return base64.b64encode(_blob_to_bytes(out_blob)).decode("ascii")
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def _dpapi_decrypt_text(token_b64: str) -> str:
    raw = base64.b64decode(str(token_b64 or "").encode("ascii"))
    if not raw:
        return ""
    in_blob = _bytes_to_blob(raw)
    out_blob = _DATA_BLOB()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    ok = crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("CryptUnprotectData failed")
    try:
        return _blob_to_bytes(out_blob).decode("utf-8")
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)

class KISApiError(RuntimeError):
    """Raised when KIS API call fails."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "",
        category: str = "UNKNOWN",
        retryable: bool = False,
        status_code: Optional[int] = None,
        path: str = "",
        body: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.code = str(code or "").strip()
        self.category = str(category or "UNKNOWN").strip().upper()
        self.retryable = bool(retryable)
        self.status_code = int(status_code) if status_code is not None else None
        self.path = str(path or "").strip()
        self.body = dict(body or {})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message": str(self),
            "code": self.code,
            "category": self.category,
            "retryable": self.retryable,
            "status_code": self.status_code,
            "path": self.path,
            "body": self.body,
        }


def _split_account_no(account_no: str) -> Tuple[str, str]:
    s = str(account_no or "").strip()
    if not s:
        raise ValueError("KIS_ACCOUNT_NO is required (ex: 12345678-01)")

    if "-" in s:
        left, right = s.split("-", 1)
    else:
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) == 8:
            # Accept plain 8-digit account and default product code to 01.
            left, right = digits, "01"
        elif len(digits) >= 10:
            left, right = digits[:8], digits[8:10]
        else:
            raise ValueError("KIS_ACCOUNT_NO must include 8-digit account + 2-digit product code")

    left = left.strip()
    right = right.strip()
    if len(left) != 8 or len(right) != 2 or (not left.isdigit()) or (not right.isdigit()):
        raise ValueError("Invalid account format. Use 12345678-01")
    return left, right


def _read_secret_direct_or_file(value: str, file_path: str) -> str:
    v = str(value or "").strip()
    if v:
        return v
    fp = str(file_path or "").strip()
    if not fp:
        return ""
    p = Path(fp)
    if not p.exists():
        raise ValueError(f"secret file not found: {p}")
    txt = p.read_text(encoding="utf-8-sig").strip()
    return txt


def _resolve_secrets_dir() -> Path:
    env_dir = str(os.getenv("KIS_SECRETS_DIR", "")).strip()
    if env_dir:
        return Path(env_dir).expanduser()
    return ROOT / ".secrets"


def _resolve_secret_file_path(env_file_path: str, default_name: str) -> str:
    fp = str(env_file_path or "").strip()
    if fp:
        return fp
    p = _resolve_secrets_dir() / str(default_name)
    return str(p) if p.exists() else ""


@dataclass
class KISConfig:
    app_key: str
    app_secret: str
    cano: str
    acnt_prdt_cd: str
    mock: bool = False
    timeout_sec: float = 10.0
    min_request_interval_sec: float = 0.2
    rate_limit_retries: int = 2
    rate_limit_backoff_sec: float = 0.5
    shared_budget_file: str = ""
    shared_budget_lock_timeout_sec: float = 5.0

    @property
    def base_url(self) -> str:
        return MOCK_BASE_URL if self.mock else PROD_BASE_URL


class KISOrderClient:
    """Minimal KIS REST client for domestic stock trading APIs."""

    def __init__(self, cfg: KISConfig):
        self.cfg = cfg
        self._token = ""
        self._token_expire_at = 0.0
        self._session = requests.Session()
        self._rate_lock = threading.Lock()
        self._last_request_monotonic = 0.0

    @classmethod
    def from_env(cls, mock: Optional[bool] = None) -> "KISOrderClient":
        env_raw = os.getenv("KIS_MOCK")
        # [2026-09-10] 기본값을 **prod -> mock 으로 뒤집었다.**
        #   종전: os.getenv("KIS_MOCK", "0") -> 아무것도 안 주면 **실계좌**.
        #   그리고 대부분 도구의 `--mock` 기본값이 "auto"(=None) 라 이 경로가 쉽게 열렸다.
        #   실제로 열렸다: `run_daily.bat` 이 fill sync 에만 --mock 을 안 넘겨 체결 조회가
        #   실계좌를 봤고, 계좌 전환(09-07) 이후 **매일 "체결 0건"** 으로 기록됐다.
        #   그 사이 모의계좌에는 09-08 에 7건이 체결돼 있었다.
        #
        #   뒤집기 전 실측 (2026-09-10):
        #     KIS_MOCK=0 을 쓰는 곳        run_intraday_paper_prod_hr05.bat 하나. **예약 안 됨**
        #     활성 예약 작업 25개          실계좌를 요구하는 것 0개
        #   -> 명시적 설정(--mock / KIS_MOCK)은 그대로 이긴다. 바뀌는 건 **아무것도 안 준 경우**뿐이고,
        #      그 경우 안전한 쪽은 모의계좌다. 실계좌는 실수로 닿을 자리가 아니다.
        env_default = "1"
        env_mock = str(env_raw if env_raw is not None else env_default).strip().lower() in {"1", "true", "y", "yes"}
        effective_mock = env_mock if mock is None else bool(mock)
        mode_tag = "MOCK" if effective_mock else "PROD"
        if mock is None and env_raw is None:
            print(
                "[KIS_MODE] mock 인자도 KIS_MOCK 환경변수도 없어서 **모의계좌**로 붙어요(안전 기본값). "
                "실계좌가 필요하면 --mock false 또는 KIS_MOCK=0 을 명시하세요.",
                flush=True,
            )
        require_mode_secrets = str(os.getenv("KIS_REQUIRE_MODE_SECRETS", "1")).strip().lower() in {"1", "true", "y", "yes"}
        default_min_interval = "0.35" if effective_mock else "0.2"
        default_rate_retries = "4" if effective_mock else "2"
        default_rate_backoff = "1.0" if effective_mock else "0.5"

        def _pick(mode_name: str, common_name: str) -> str:
            mv = str(os.getenv(mode_name, "")).strip()
            if mv:
                return mv
            return str(os.getenv(common_name, "")).strip()

        if (not effective_mock) and require_mode_secrets:
            prod_key_mode = str(os.getenv("KIS_APP_KEY_PROD", "")).strip()
            prod_secret_mode = str(os.getenv("KIS_APP_SECRET_PROD", "")).strip()
            prod_account_mode = str(os.getenv("KIS_ACCOUNT_NO_PROD", "")).strip()
            prod_key_file_mode = str(os.getenv("KIS_APP_KEY_FILE_PROD", "")).strip()
            prod_secret_file_mode = str(os.getenv("KIS_APP_SECRET_FILE_PROD", "")).strip()
            prod_account_file_mode = str(os.getenv("KIS_ACCOUNT_NO_FILE_PROD", "")).strip()

            default_prod_key_file = _resolve_secret_file_path("", "kis_app_key_prod.txt")
            default_prod_secret_file = _resolve_secret_file_path("", "kis_app_secret_prod.txt")
            default_prod_account_file = _resolve_secret_file_path("", "kis_account_no_prod.txt")

            has_prod_mode_secret = any(
                [
                    bool(prod_key_mode),
                    bool(prod_secret_mode),
                    bool(prod_account_mode),
                    bool(prod_key_file_mode and Path(prod_key_file_mode).exists()),
                    bool(prod_secret_file_mode and Path(prod_secret_file_mode).exists()),
                    bool(prod_account_file_mode and Path(prod_account_file_mode).exists()),
                    bool(default_prod_key_file),
                    bool(default_prod_secret_file),
                    bool(default_prod_account_file),
                ]
            )
            if not has_prod_mode_secret:
                raise ValueError(
                    "prod mode requires mode-specific secrets: set KIS_APP_KEY_PROD/KIS_APP_SECRET_PROD/"
                    "KIS_ACCOUNT_NO_PROD (or *_FILE_PROD), or create .secrets\\kis_*_prod.txt"
                )

        app_key_file = _resolve_secret_file_path(
            _pick(f"KIS_APP_KEY_FILE_{mode_tag}", "KIS_APP_KEY_FILE"),
            "kis_app_key_mock.txt" if effective_mock else "kis_app_key_prod.txt",
        ) or _resolve_secret_file_path("", "kis_app_key.txt")

        app_secret_file = _resolve_secret_file_path(
            _pick(f"KIS_APP_SECRET_FILE_{mode_tag}", "KIS_APP_SECRET_FILE"),
            "kis_app_secret_mock.txt" if effective_mock else "kis_app_secret_prod.txt",
        ) or _resolve_secret_file_path("", "kis_app_secret.txt")

        account_no_file = _resolve_secret_file_path(
            _pick(f"KIS_ACCOUNT_NO_FILE_{mode_tag}", "KIS_ACCOUNT_NO_FILE"),
            "kis_account_no_mock.txt" if effective_mock else "kis_account_no_prod.txt",
        ) or _resolve_secret_file_path("", "kis_account_no.txt")

        app_key = _read_secret_direct_or_file(
            _pick(f"KIS_APP_KEY_{mode_tag}", "KIS_APP_KEY"),
            app_key_file,
        )
        app_secret = _read_secret_direct_or_file(
            _pick(f"KIS_APP_SECRET_{mode_tag}", "KIS_APP_SECRET"),
            app_secret_file,
        )
        account_no = _read_secret_direct_or_file(
            _pick(f"KIS_ACCOUNT_NO_{mode_tag}", "KIS_ACCOUNT_NO"),
            account_no_file,
        )
        if not app_key or not app_secret or not account_no:
            raise ValueError(
                "KIS_APP_KEY(or KIS_APP_KEY_FILE), KIS_APP_SECRET(or KIS_APP_SECRET_FILE), "
                "KIS_ACCOUNT_NO(or KIS_ACCOUNT_NO_FILE) are required"
            )

        cano, acnt_prdt_cd = _split_account_no(account_no)
        cfg = KISConfig(
            app_key=app_key,
            app_secret=app_secret,
            cano=cano,
            acnt_prdt_cd=acnt_prdt_cd,
            mock=effective_mock,
            min_request_interval_sec=max(
                0.0,
                float(os.getenv("KIS_MIN_REQUEST_INTERVAL_SEC", default_min_interval) or float(default_min_interval)),
            ),
            rate_limit_retries=max(
                0,
                int(float(os.getenv("KIS_RATE_LIMIT_RETRIES", default_rate_retries) or float(default_rate_retries))),
            ),
            rate_limit_backoff_sec=max(
                0.0,
                float(os.getenv("KIS_RATE_LIMIT_BACKOFF_SEC", default_rate_backoff) or float(default_rate_backoff)),
            ),
            shared_budget_file=str(os.getenv("KIS_SHARED_BUDGET_FILE", "")).strip(),
            shared_budget_lock_timeout_sec=max(
                0.1,
                float(os.getenv("KIS_SHARED_BUDGET_LOCK_TIMEOUT_SEC", "5.0") or 5.0),
            ),
        )
        return cls(cfg)

    def _wait_rate_limit(self) -> None:
        min_interval = max(0.0, float(self.cfg.min_request_interval_sec or 0.0))
        if min_interval <= 0.0:
            return
        with self._rate_lock:
            self._wait_shared_rate_limit_locked(min_interval)
            now = time.monotonic()
            wait_sec = (self._last_request_monotonic + min_interval) - now
            if wait_sec > 0.0:
                time.sleep(wait_sec)
            self._last_request_monotonic = time.monotonic()

    def _shared_budget_path(self) -> Path:
        p = str(self.cfg.shared_budget_file or "").strip()
        if p:
            return Path(p).expanduser()
        suffix = "mock" if self.cfg.mock else "prod"
        return ROOT / "2_Logs" / f"kis_rate_budget_{suffix}.json"

    def _shared_budget_scope(self) -> str:
        return f"{self.cfg.base_url}|{self._app_key_hash()[:12]}"

    @staticmethod
    def _lock_file_path(path: Path) -> Path:
        return path.with_suffix(path.suffix + ".lock")

    def _acquire_lock_file(self, path: Path) -> Optional[int]:
        deadline = time.time() + max(0.1, float(self.cfg.shared_budget_lock_timeout_sec or 5.0))
        stale_lock_sec = max(
            0.0,
            float(os.getenv("KIS_SHARED_BUDGET_STALE_LOCK_SEC", "30.0") or 30.0),
        )
        while time.time() < deadline:
            try:
                fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                try:
                    os.write(fd, str(os.getpid()).encode("ascii", errors="ignore"))
                except Exception:
                    pass
                return fd
            except FileExistsError:
                try:
                    raw_pid = path.read_text(encoding="ascii", errors="ignore").strip()
                    lock_pid = int(raw_pid) if raw_pid.isdigit() else 0
                except Exception:
                    lock_pid = 0
                if lock_pid > 0 and not self._is_pid_alive(lock_pid):
                    try:
                        path.unlink(missing_ok=True)
                        continue
                    except Exception:
                        pass
                if stale_lock_sec > 0.0:
                    try:
                        age_sec = time.time() - float(path.stat().st_mtime)
                    except Exception:
                        age_sec = 0.0
                    if age_sec > stale_lock_sec:
                        try:
                            path.unlink(missing_ok=True)
                            continue
                        except Exception:
                            pass
                time.sleep(0.02)
            except Exception:
                return None
        return None

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        try:
            pid_i = int(pid)
        except Exception:
            return False
        if pid_i <= 0:
            return False
        if os.name == "nt":
            try:
                process_query_limited_information = 0x1000
                handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid_i)
                if handle:
                    ctypes.windll.kernel32.CloseHandle(handle)
                    return True
                return False
            except Exception:
                return True
        try:
            os.kill(pid_i, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return True

    @staticmethod
    def _release_lock_file(fd: Optional[int], path: Path) -> None:
        if fd is None:
            return
        try:
            os.close(fd)
        except Exception:
            pass
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

    def _wait_shared_rate_limit_locked(self, min_interval: float) -> None:
        path = self._shared_budget_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self._lock_file_path(path)
        fd = self._acquire_lock_file(lock_path)
        if fd is None:
            return
        try:
            try:
                state = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else {}
            except Exception:
                state = {}

            scopes = state.get("scopes", {})
            if not isinstance(scopes, dict):
                scopes = {}
            scope = self._shared_budget_scope()
            entry = scopes.get(scope, {})
            if not isinstance(entry, dict):
                entry = {}

            now_epoch = time.time()
            last_epoch = float(entry.get("last_request_epoch", 0.0) or 0.0)
            wait_sec = (last_epoch + float(min_interval)) - now_epoch
            if wait_sec > 0.0:
                time.sleep(wait_sec)
                now_epoch = time.time()

            scopes[scope] = {
                "base_url": self.cfg.base_url,
                "updated_at": int(now_epoch),
                "last_request_epoch": float(now_epoch),
                "min_interval_sec": float(min_interval),
            }
            state = {
                "updated_at": int(now_epoch),
                "scopes": scopes,
            }
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(str(tmp_path), str(path))
        finally:
            self._release_lock_file(fd, lock_path)

    @staticmethod
    def _is_rate_limited(resp: requests.Response, body: Dict[str, Any]) -> bool:
        if int(resp.status_code) == 429:
            return True
        hint = " ".join(
            [
                str(body.get("msg1", "")),
                str(body.get("msg_cd", "")),
                str(body.get("error_description", "")),
                str(body.get("raw_text", "")),
            ]
        ).lower()
        return any(
            token in hint
            for token in (
                "rate limit",
                "too many requests",
                "호출 제한",
                "요청 제한",
                # [2026-09-22 실측] 실제 응답은 "원장에서 허용 가능한 **초당 거래건수를 초과**하였습니다."
                #   종전 토큰 `초당 거래건수 초과` 는 조사(를) 하나 때문에 안 맞았고,
                #   `EGW00215` 도 목록에 없어 **재시도가 한 번도 안 걸렸다**.
                #   09-22 10:36 E2E 3회차가 그래서 죽었다(경보는 토큰 두절로 사용자에게 안 갔다).
                #   조사가 붙어도 걸리도록 어간까지만 본다.
                "초당 거래건수",
                "egw00215",
                "접근토큰 발급 잠시 후 다시 시도",
                "egw00201",
                "egw00133",
            )
        )

    @staticmethod
    def _normalize_broker_error(status_code: int, body: Dict[str, Any], path: str) -> Dict[str, Any]:
        msg_cd = str(body.get("msg_cd", "") or body.get("error_code", "") or "").strip().upper()
        rt_cd = str(body.get("rt_cd", "") or "").strip()
        msg1 = str(body.get("msg1", "") or body.get("error_description", "") or body.get("raw_text", "") or "").strip()
        hint = " ".join([msg_cd, rt_cd, msg1]).lower()

        category = "HTTP_ERROR"
        retryable = False

        if msg_cd in {"EGW00133", "EGW00201"}:
            category = "RATE_LIMIT"
            retryable = True
        elif int(status_code) == 429 or any(token in hint for token in ("rate limit", "too many requests", "초당 거래건수 초과", "egw00201")):
            category = "RATE_LIMIT"
            retryable = True
        elif any(token in hint for token in ("접근토큰", "approval", "token")):
            category = "AUTH"
            retryable = True
        elif any(token in hint for token in ("장운영", "장시간", "session", "거래정지", "정지", "delist", "suspend")):
            category = "MARKET_STATE"
        elif any(token in hint for token in ("insufficient", "잔고", "margin", "예수금")):
            category = "RISK_LIMIT"
        elif any(token in hint for token in ("invalid", "필수", "format", "입력값")):
            category = "INVALID_REQUEST"
        elif 500 <= int(status_code) <= 599:
            category = "SERVER"
            retryable = True
        elif int(status_code) >= 400:
            category = "HTTP_ERROR"

        return {
            "code": msg_cd or rt_cd,
            "message": msg1,
            "category": category,
            "retryable": retryable,
            "status_code": int(status_code),
            "path": str(path or ""),
        }

    @classmethod
    def normalize_broker_response(cls, *, status_code: int, body: Dict[str, Any], path: str = "") -> Dict[str, Any]:
        return cls._normalize_broker_error(int(status_code), dict(body or {}), path)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        headers: Dict[str, str],
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        url = f"{self.cfg.base_url}{path}"
        retries = max(0, int(self.cfg.rate_limit_retries or 0))
        for attempt in range(retries + 1):
            self._wait_rate_limit()
            try:
                resp = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    json=payload,
                    params=params,
                    timeout=self.cfg.timeout_sec,
                )
            except requests.exceptions.Timeout as e:
                try:
                    is_post = method.upper() == "POST"
                    err_code = "ERR_KIS_ORDER_TIMEOUT" if is_post else "ERR_KIS_QUOTE_TIMEOUT"
                    if "oauth2" in path:
                        err_code = "ERR_KIS_AUTH_TIMEOUT"
                    error_pattern_analyzer.log_error(err_code, "kis_order_client.py", str(e))
                except Exception:
                    pass

                if method.upper() != "POST" and attempt < retries:
                    backoff = max(0.0, float(self.cfg.rate_limit_backoff_sec or 0.0)) * float(attempt + 1)
                    if backoff > 0.0:
                        time.sleep(backoff)
                    continue
                raise KISApiError(
                    f"timeout on {path}: {e}",
                    code="TIMEOUT",
                    category="UNKNOWN_PENDING" if method.upper() == "POST" else "NETWORK",
                    retryable=method.upper() != "POST",
                    status_code=0,
                    path=path,
                    body={},
                ) from e
            except requests.exceptions.RequestException as e:
                try:
                    is_post = method.upper() == "POST"
                    err_code = "ERR_KIS_ORDER_REQ" if is_post else "ERR_KIS_QUOTE_REQ"
                    if "oauth2" in path:
                        err_code = "ERR_KIS_AUTH_REQ"
                    error_pattern_analyzer.log_error(err_code, "kis_order_client.py", str(e))
                except Exception:
                    pass

                raise KISApiError(
                    f"network error on {path}: {e}",
                    code=type(e).__name__,
                    category="NETWORK",
                    retryable=True,
                    status_code=0,
                    path=path,
                    body={},
                ) from e
            try:
                body = resp.json()
            except Exception:
                body = {"raw_text": resp.text}

            if self._is_rate_limited(resp, body) and attempt < retries:
                backoff = max(0.0, float(self.cfg.rate_limit_backoff_sec or 0.0)) * float(attempt + 1)
                if backoff > 0.0:
                    time.sleep(backoff)
                continue

            if resp.status_code >= 400:
                meta = self._normalize_broker_error(resp.status_code, body, path)
                raise KISApiError(
                    f"HTTP {resp.status_code} on {path}: {body}",
                    code=str(meta.get("code") or ""),
                    category=str(meta.get("category") or "HTTP_ERROR"),
                    retryable=bool(meta.get("retryable")),
                    status_code=int(meta.get("status_code") or resp.status_code),
                    path=path,
                    body=body,
                )

            hdrs = {str(k).lower(): str(v) for k, v in resp.headers.items()}
            return body, hdrs
        raise KISApiError(f"rate limit retry exhausted on {path}")

    def _token_cache_path(self) -> Path:
        p = str(os.getenv("KIS_TOKEN_CACHE_FILE", "")).strip()
        if p:
            return Path(p).expanduser()
        suffix = "mock" if self.cfg.mock else "prod"
        return ROOT / "2_Logs" / f"kis_token_cache_{suffix}.json"

    def _token_issue_lock_path(self) -> Path:
        return self._token_cache_path().with_suffix(".issue.lock")

    def _app_key_hash(self) -> str:
        return hashlib.sha256(self.cfg.app_key.encode("utf-8")).hexdigest()

    def _load_cached_token(self) -> bool:
        p = self._token_cache_path()
        if not p.exists():
            return False
        try:
            obj = json.loads(p.read_text(encoding="utf-8-sig"))
            token_protection = str(obj.get("token_protection", "")).strip().lower()
            token_enc_b64 = str(obj.get("access_token_enc_b64", "")).strip()
            if token_protection == "dpapi" and token_enc_b64:
                token = _dpapi_decrypt_text(token_enc_b64).strip()
            else:
                token = str(obj.get("access_token", "")).strip()
            expires_at = float(obj.get("expires_at", 0) or 0)
            base_url = str(obj.get("base_url", "")).strip()
            app_key_hash = str(obj.get("app_key_hash", "")).strip()
            if (not token) or (expires_at <= (time.time() + 30)):
                return False
            if base_url and base_url != self.cfg.base_url:
                return False
            if app_key_hash and app_key_hash != self._app_key_hash():
                return False
            self._token = token
            self._token_expire_at = expires_at
            return True
        except Exception:
            return False

    def _save_cached_token(self) -> None:
        p = self._token_cache_path()
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            obj = {
                "saved_at": int(time.time()),
                "base_url": self.cfg.base_url,
                "app_key_hash": self._app_key_hash(),
                "token_protection": "dpapi",
                "access_token_enc_b64": _dpapi_encrypt_text(self._token),
                "expires_at": float(self._token_expire_at),
            }
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            # Cache failure should never block trading logic.
            pass

    def _issue_token(self) -> str:
        if self._load_cached_token():
            return self._token
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.app_key,
            "appsecret": self.cfg.app_secret,
        }
        headers = {"content-type": "application/json"}

        retry_max = max(0, int(float(os.getenv("KIS_TOKEN_RETRY_MAX", "1") or 1)))
        retry_wait = max(0.0, float(os.getenv("KIS_TOKEN_RETRY_WAIT_SEC", "61") or 61))

        lock_path = self._token_issue_lock_path()
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = self._acquire_lock_file(lock_path)
        if fd is None:
            if self._load_cached_token():
                return self._token

        try:
            if self._load_cached_token():
                return self._token

            for attempt in range(retry_max + 1):
                try:
                    body, _ = self._request_json("POST", "/oauth2/tokenP", headers=headers, payload=payload)
                    token = str(body.get("access_token", "")).strip()
                    if not token:
                        raise KISApiError(f"Token response missing access_token: {body}")
                    expires_in = int(body.get("expires_in", 60 * 60))
                    self._token = token
                    self._token_expire_at = time.time() + max(60, expires_in - 60)
                    self._save_cached_token()
                    return token
                except KISApiError as e:
                    if str(e.code).upper() != "EGW00133" or attempt >= retry_max:
                        raise
                    if retry_wait > 0:
                        time.sleep(retry_wait)
                    if self._load_cached_token():
                        return self._token
        finally:
            self._release_lock_file(fd, lock_path)
        raise KISApiError("token issue failed after retries", code="EGW00133", category="RATE_LIMIT", retryable=True)

    def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expire_at:
            return self._token
        if self._load_cached_token():
            return self._token
        return self._issue_token()

    def invalidate_token_cache(self) -> None:
        self._token = ""
        self._token_expire_at = 0.0
        try:
            self._token_cache_path().unlink(missing_ok=True)
        except Exception:
            pass

    def issue_ws_approval_key(self) -> str:
        """Issue websocket approval key for real-time subscription."""
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.app_key,
            "secretkey": self.cfg.app_secret,
        }
        headers = {"content-type": "application/json"}
        body, _ = self._request_json("POST", "/oauth2/Approval", headers=headers, payload=payload)
        key = str(body.get("approval_key", "")).strip()
        if not key:
            raise KISApiError(f"Approval response missing approval_key: {body}")
        return key

    def _hashkey(self, payload: Dict[str, Any]) -> str:
        headers = {
            "content-type": "application/json",
            "appkey": self.cfg.app_key,
            "appsecret": self.cfg.app_secret,
        }
        body, _ = self._request_json("POST", "/uapi/hashkey", headers=headers, payload=payload)
        key = str(body.get("HASH", "")).strip()
        if not key:
            raise KISApiError(f"Hashkey response missing HASH: {body}")
        return key

    def _auth_headers(
        self,
        *,
        tr_id: str,
        payload_for_hash: Optional[Dict[str, Any]] = None,
        tr_cont: str = "",
    ) -> Dict[str, str]:
        token = self._ensure_token()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.cfg.app_key,
            "appsecret": self.cfg.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        if tr_cont:
            headers["tr_cont"] = str(tr_cont)
        if payload_for_hash is not None:
            headers["hashkey"] = self._hashkey(payload_for_hash)
        return headers

    def place_order_cash(
        self,
        *,
        side: str,
        code: str,
        qty: int,
        order_type: str = "market",
        price: int = 0,
        exchange: str = "KRX",
    ) -> Dict[str, Any]:
        side_u = str(side).strip().upper()
        if side_u not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported side: {side}")
        if int(qty) <= 0:
            raise ValueError(f"qty must be > 0: {qty}")

        code_s = str(code).strip().zfill(6)
        if not code_s.isdigit():
            raise ValueError(f"Invalid stock code: {code}")

        order_type_l = str(order_type).strip().lower()
        if order_type_l == "market":
            ord_dvsn = "01"
            ord_unpr = "0"
        elif order_type_l == "limit":
            if int(price) <= 0:
                raise ValueError("limit order requires price > 0")
            ord_dvsn = "00"
            ord_unpr = str(int(price))
        else:
            raise ValueError(f"Unsupported order_type: {order_type}")

        if self.cfg.mock:
            tr_id = "VTTC0012U" if side_u == "BUY" else "VTTC0011U"
        else:
            tr_id = "TTTC0012U" if side_u == "BUY" else "TTTC0011U"

        payload = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "PDNO": code_s,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": ord_unpr,
            "EXCG_ID_DVSN_CD": str(exchange).strip().upper() or "KRX",
            "SLL_TYPE": "01" if side_u == "SELL" else "",
            "CNDT_PRIC": "",
        }

        headers = self._auth_headers(tr_id=tr_id, payload_for_hash=payload)
        body, _ = self._request_json(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-cash",
            headers=headers,
            payload=payload,
        )

        rt_cd = str(body.get("rt_cd", ""))
        msg1 = str(body.get("msg1", ""))
        output = body.get("output", {}) or {}
        ord_no = str(output.get("ODNO", "") or output.get("odno", "")).strip()
        org_no = str(output.get("KRX_FWDG_ORD_ORGNO", "") or output.get("krx_fwdg_ord_orgno", "")).strip()
        err_meta = self._normalize_broker_error(
            200 if rt_cd == "0" else 400,
            body,
            "/uapi/domestic-stock/v1/trading/order-cash",
        )

        return {
            "ok": rt_cd == "0",
            "rt_cd": rt_cd,
            "msg1": msg1,
            "error_code": str(body.get("msg_cd", "") or ""),
            "error_category": str(err_meta.get("category", "") or ""),
            "error_status_code": int(err_meta.get("status_code") or 0),
            "error_path": str(err_meta.get("path", "") or ""),
            "retryable": bool(err_meta.get("retryable", False)),
            "ord_no": ord_no,
            "org_no": org_no,
            "tr_id": tr_id,
            "payload": payload,
            "raw": body,
        }

    def inquire_psbl_order(
        self,
        *,
        code: str,
        order_price: int,
        ord_dvsn: str = "01",
        cma_evlu_amt_icld_yn: str = "N",
        ovrs_icld_yn: str = "N",
    ) -> Dict[str, Any]:
        code_s = str(code).strip().zfill(6)
        if not code_s.isdigit():
            raise ValueError(f"Invalid stock code: {code}")

        tr_id = "VTTC8908R" if self.cfg.mock else "TTTC8908R"
        params = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "PDNO": code_s,
            "ORD_UNPR": str(max(0, int(order_price))),
            "ORD_DVSN": str(ord_dvsn),
            "CMA_EVLU_AMT_ICLD_YN": str(cma_evlu_amt_icld_yn),
            "OVRS_ICLD_YN": str(ovrs_icld_yn),
        }

        headers = self._auth_headers(tr_id=tr_id)
        body, _ = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            headers=headers,
            params=params,
        )

        rt_cd = str(body.get("rt_cd", ""))
        msg1 = str(body.get("msg1", ""))
        if rt_cd != "0":
            raise KISApiError(f"inquire_psbl_order rejected rt_cd={rt_cd} msg1={msg1} body={body}")

        output = body.get("output", {}) or {}
        return {
            "ok": True,
            "tr_id": tr_id,
            "rt_cd": rt_cd,
            "msg1": msg1,
            "output": output,
            "raw": body,
        }

    def inquire_balance_positions(
        self,
        *,
        afhr_flpr_yn: str = "N",
        ofl_yn: str = "",
        inqr_dvsn: str = "01",
        unpr_dvsn: str = "01",
        fund_sttl_icld_yn: str = "N",
        fncg_amt_auto_rdpt_yn: str = "N",
        prcs_dvsn: str = "00",
        max_pages: int = 30,
    ) -> Dict[str, Any]:
        tr_id = "VTTC8434R" if self.cfg.mock else "TTTC8434R"

        pages = 0
        tr_cont = ""
        fk100 = ""
        nk100 = ""
        rows: list[Dict[str, Any]] = []
        output2: list[Dict[str, Any]] = []

        while True:
            pages += 1
            if pages > int(max_pages):
                break

            params = {
                "CANO": self.cfg.cano,
                "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
                "AFHR_FLPR_YN": str(afhr_flpr_yn),
                "OFL_YN": str(ofl_yn),
                "INQR_DVSN": str(inqr_dvsn),
                "UNPR_DVSN": str(unpr_dvsn),
                "FUND_STTL_ICLD_YN": str(fund_sttl_icld_yn),
                "FNCG_AMT_AUTO_RDPT_YN": str(fncg_amt_auto_rdpt_yn),
                "PRCS_DVSN": str(prcs_dvsn),
                "CTX_AREA_FK100": str(fk100),
                "CTX_AREA_NK100": str(nk100),
            }

            headers = self._auth_headers(tr_id=tr_id, tr_cont=tr_cont)
            body, hdrs = self._request_json(
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-balance",
                headers=headers,
                params=params,
            )

            rt_cd = str(body.get("rt_cd", ""))
            msg1 = str(body.get("msg1", ""))
            if rt_cd != "0":
                raise KISApiError(f"inquire_balance rejected rt_cd={rt_cd} msg1={msg1} body={body}")

            out1 = body.get("output1", []) or []
            out2 = body.get("output2", []) or []
            if isinstance(out1, dict):
                out1 = [out1]
            if isinstance(out2, dict):
                out2 = [out2]
            rows.extend(out1)
            output2.extend(out2)

            fk100 = str(body.get("ctx_area_fk100", "") or "")
            nk100 = str(body.get("ctx_area_nk100", "") or "")
            tr_cont_resp = str(hdrs.get("tr_cont", "") or "").upper()
            if tr_cont_resp not in {"M", "F"}:
                break

            tr_cont = "N"
            if not fk100 and not nk100:
                break

        return {
            "ok": True,
            "tr_id": tr_id,
            "rows": rows,
            "summary_rows": output2,
            "pages": pages,
        }

    def inquire_daily_ccld(
        self,
        *,
        start_ymd: str,
        end_ymd: str,
        pd_dv: str = "inner",
        sll_buy_dvsn_cd: str = "00",
        ccld_dvsn: str = "01",
        inqr_dvsn: str = "00",
        inqr_dvsn_3: str = "00",
        pdno: str = "",
        excg_id_dvsn_cd: str = "KRX",
        max_pages: int = 30,
    ) -> Dict[str, Any]:
        pd_dv_s = str(pd_dv).strip().lower()
        if pd_dv_s not in {"before", "inner"}:
            raise ValueError("pd_dv must be before or inner")

        if self.cfg.mock:
            tr_id = "VTSC9215R" if pd_dv_s == "before" else "VTTC0081R"
        else:
            tr_id = "CTSC9215R" if pd_dv_s == "before" else "TTTC0081R"

        all_rows: list[Dict[str, Any]] = []
        pages = 0
        fk100 = ""
        nk100 = ""
        tr_cont = ""

        while True:
            pages += 1
            if pages > int(max_pages):
                break

            params: Dict[str, Any] = {
                "CANO": self.cfg.cano,
                "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
                "INQR_STRT_DT": str(start_ymd),
                "INQR_END_DT": str(end_ymd),
                "SLL_BUY_DVSN_CD": str(sll_buy_dvsn_cd),
                "PDNO": str(pdno),
                "CCLD_DVSN": str(ccld_dvsn),
                "INQR_DVSN": str(inqr_dvsn),
                "INQR_DVSN_3": str(inqr_dvsn_3),
                "ORD_GNO_BRNO": "",
                "ODNO": "",
                "INQR_DVSN_1": "",
                "CTX_AREA_FK100": str(fk100),
                "CTX_AREA_NK100": str(nk100),
            }
            if excg_id_dvsn_cd:
                params["EXCG_ID_DVSN_CD"] = str(excg_id_dvsn_cd)

            headers = self._auth_headers(tr_id=tr_id, tr_cont=tr_cont)
            body, hdrs = self._request_json(
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                headers=headers,
                params=params,
            )

            rt_cd = str(body.get("rt_cd", ""))
            msg1 = str(body.get("msg1", ""))
            if rt_cd != "0":
                raise KISApiError(f"inquire_daily_ccld rejected rt_cd={rt_cd} msg1={msg1} body={body}")

            out1 = body.get("output1", []) or []
            if isinstance(out1, dict):
                out1 = [out1]
            all_rows.extend(out1)

            fk100 = str(body.get("ctx_area_fk100", "") or "")
            nk100 = str(body.get("ctx_area_nk100", "") or "")
            tr_cont_resp = str(hdrs.get("tr_cont", "") or "").upper()
            if tr_cont_resp not in {"M", "F"}:
                break

            tr_cont = "N"
            if not fk100 and not nk100:
                break

        return {
            "ok": True,
            "tr_id": tr_id,
            "rows": all_rows,
            "pages": pages,
            "start_ymd": str(start_ymd),
            "end_ymd": str(end_ymd),
        }

    def inquire_period_trade_profit(
        self,
        *,
        start_ymd: str,
        end_ymd: str,
        pdno: str = "",
        sort_dvsn: str = "00",
        inqr_dvsn: str = "00",
        cblc_dvsn: str = "00",
        max_pages: int = 30,
    ) -> Dict[str, Any]:
        """Realized P&L by period, including the broker-charged fee and tax.

        Why this exists (2026-08-22, PLANS 56):
          inquire_daily_ccld gives qty and price but NOT the amounts the broker
          actually charged. The cost constants in this system were therefore
          hardcoded guesses and they disagree with each other:
              paper_engine_config.json   fee 0.004%  tax 0.15%
              paper_fills_ledger.csv     fee 0.02%   tax 0.20%
          A single real round trip (2026-08, 71,950 -> 72,350) showed the truth is
          fee 0 and tax 0.20%, so both were wrong in opposite directions.
          This endpoint is the only one that reports those numbers.

        NOTE: field names in the response are NOT verified against the KIS docs in
        this codebase yet. Callers should keep the raw rows. The known real trade
        above is the fixture - if 144 (tax) and 256 (realized P&L) show up in the
        response, the mapping is confirmed. Do not hardcode a mapping before that.

        Realized-P&L inquiry is generally a live-account API; the mock endpoint may
        not exist. tr_id is chosen the same way as the other methods so a mock call
        fails loudly rather than silently returning something else.
        """
        tr_id = "VTTC8715R" if self.cfg.mock else "TTTC8715R"

        all_rows: list[Dict[str, Any]] = []
        summary: Dict[str, Any] = {}
        pages = 0
        fk100 = ""
        nk100 = ""
        tr_cont = ""

        while True:
            pages += 1
            if pages > int(max_pages):
                break

            params: Dict[str, Any] = {
                "CANO": self.cfg.cano,
                "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
                "SORT_DVSN": str(sort_dvsn),
                "PDNO": str(pdno),
                "INQR_STRT_DT": str(start_ymd),
                "INQR_END_DT": str(end_ymd),
                "CBLC_DVSN": str(cblc_dvsn),
                "INQR_DVSN": str(inqr_dvsn),
                "CTX_AREA_FK100": str(fk100),
                "CTX_AREA_NK100": str(nk100),
            }

            headers = self._auth_headers(tr_id=tr_id, tr_cont=tr_cont)
            body, hdrs = self._request_json(
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-period-trade-profit",
                headers=headers,
                params=params,
            )

            rt_cd = str(body.get("rt_cd", ""))
            msg1 = str(body.get("msg1", ""))
            if rt_cd != "0":
                raise KISApiError(
                    f"inquire_period_trade_profit rejected rt_cd={rt_cd} msg1={msg1} body={body}"
                )

            out1 = body.get("output1", []) or []
            if isinstance(out1, dict):
                out1 = [out1]
            all_rows.extend(out1)

            out2 = body.get("output2", {}) or {}
            if isinstance(out2, list):
                out2 = out2[0] if out2 else {}
            if out2:
                summary = out2

            fk100 = str(body.get("ctx_area_fk100", "") or "")
            nk100 = str(body.get("ctx_area_nk100", "") or "")
            tr_cont_resp = str(hdrs.get("tr_cont", "") or "").upper()
            if tr_cont_resp not in {"M", "F"}:
                break

            tr_cont = "N"
            if not fk100 and not nk100:
                break

        return {
            "ok": True,
            "tr_id": tr_id,
            "rows": all_rows,
            "summary": summary,
            "pages": pages,
            "start_ymd": str(start_ymd),
            "end_ymd": str(end_ymd),
        }

    @staticmethod
    def _pick_first_int(row: Dict[str, Any], keys: list[str], default: int = 0) -> int:
        for k in keys:
            if k not in row:
                continue
            try:
                return int(float(str(row.get(k, "")).replace(",", "").strip()))
            except Exception:
                continue
        return int(default)

    def inquire_open_orders(
        self,
        *,
        ymd: str,
        sll_buy_dvsn_cd: str = "00",
        pdno: str = "",
        max_pages: int = 30,
    ) -> Dict[str, Any]:
        """Return open(unfilled) orders for a day by filtering inquire_daily_ccld rows."""
        rsp = self.inquire_daily_ccld(
            start_ymd=str(ymd),
            end_ymd=str(ymd),
            sll_buy_dvsn_cd=str(sll_buy_dvsn_cd),
            ccld_dvsn="00",
            inqr_dvsn="00",
            inqr_dvsn_3="00",
            pdno=str(pdno or ""),
            max_pages=int(max_pages),
        )
        rows = rsp.get("rows", []) or []
        out_rows: list[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            ord_qty = self._pick_first_int(r, ["ord_qty", "tot_ord_qty", "qty", "ORD_QTY"], default=0)
            ccld_qty = self._pick_first_int(r, ["tot_ccld_qty", "ccld_qty", "exec_qty", "CCLD_QTY"], default=0)
            rem_qty = self._pick_first_int(
                r,
                ["rmn_qty", "ord_psbl_qty", "open_qty", "unccld_qty", "UNCCLD_QTY"],
                default=max(0, int(ord_qty) - int(ccld_qty)),
            )
            if int(rem_qty) <= 0:
                continue
            code = str(r.get("pdno", "") or r.get("code", "")).strip().zfill(6)
            odno = str(r.get("odno", "") or r.get("ord_no", "") or r.get("order_no", "")).strip()
            orgno = str(r.get("ord_gno_brno", "") or r.get("order_branch_no", "") or r.get("orgn_odno", "")).strip()
            if not odno:
                continue
            out_rows.append(
                {
                    "code": code,
                    "odno": odno,
                    "ord_gno_brno": orgno,
                    "ord_qty": int(ord_qty),
                    "ccld_qty": int(ccld_qty),
                    "rmn_qty": int(rem_qty),
                    "ord_tmd": str(r.get("ord_tmd", "") or r.get("order_time", "") or ""),
                    "side_raw": str(r.get("sll_buy_dvsn_cd", "") or r.get("side", "")),
                    "raw": r,
                }
            )

        return {
            "ok": True,
            "tr_id": rsp.get("tr_id", ""),
            "ymd": str(ymd),
            "rows": out_rows,
            "rows_raw": rows,
            "pages": int(rsp.get("pages", 0) or 0),
        }

    def cancel_order(
        self,
        *,
        org_order_no: str,
        org_order_branch_no: str,
        qty: int = 0,
        cancel_all: bool = True,
        order_price: int = 0,
        order_dvsn: str = "00",
        tr_id: str = "",
    ) -> Dict[str, Any]:
        """Cancel a previously submitted cash order."""
        odno = str(org_order_no or "").strip()
        brno = str(org_order_branch_no or "").strip()
        if not odno:
            raise ValueError("org_order_no is required")
        if not brno:
            raise ValueError("org_order_branch_no is required")

        q = int(qty or 0)
        if q < 0:
            raise ValueError("qty must be >= 0")

        tr_id_use = str(tr_id or "").strip()
        if not tr_id_use:
            tr_id_use = "VTTC0803U" if self.cfg.mock else "TTTC0803U"

        payload = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "KRX_FWDG_ORD_ORGNO": brno,
            "ORGN_ODNO": odno,
            "ORD_DVSN": str(order_dvsn or "00"),
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": str(q),
            "ORD_UNPR": str(max(0, int(order_price))),
            "QTY_ALL_ORD_YN": "Y" if cancel_all else "N",
        }
        headers = self._auth_headers(tr_id=tr_id_use, payload_for_hash=payload)
        body, _ = self._request_json(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-rvsecncl",
            headers=headers,
            payload=payload,
        )
        rt_cd = str(body.get("rt_cd", ""))
        msg1 = str(body.get("msg1", ""))
        output = body.get("output", {}) or {}
        rvsecncl_no = str(output.get("ODNO", "") or output.get("odno", "")).strip()
        rvsecncl_org_no = str(output.get("KRX_FWDG_ORD_ORGNO", "") or output.get("krx_fwdg_ord_orgno", "")).strip()
        err_meta = self._normalize_broker_error(
            200 if rt_cd == "0" else 400,
            body,
            "/uapi/domestic-stock/v1/trading/order-rvsecncl",
        )

        return {
            "ok": rt_cd == "0",
            "rt_cd": rt_cd,
            "msg1": msg1,
            "error_code": str(body.get("msg_cd", "") or ""),
            "error_category": str(err_meta.get("category", "") or ""),
            "error_status_code": int(err_meta.get("status_code") or 0),
            "error_path": str(err_meta.get("path", "") or ""),
            "retryable": bool(err_meta.get("retryable", False)),
            "ord_no": rvsecncl_no,
            "org_no": rvsecncl_org_no,
            "tr_id": tr_id_use,
            "payload": payload,
            "raw": body,
        }

    def inquire_price(
        self,
        *,
        code: str,
        fid_cond_mrkt_div_code: str = "J",
        tr_id: str = "FHKST01010100",
    ) -> Dict[str, Any]:
        """Get current quote snapshot for domestic stock."""
        code_s = str(code).strip().zfill(6)
        if not code_s.isdigit() or len(code_s) != 6:
            raise ValueError(f"Invalid stock code: {code}")

        params = {
            "FID_COND_MRKT_DIV_CODE": str(fid_cond_mrkt_div_code or "J"),
            "FID_INPUT_ISCD": code_s,
        }
        headers = self._auth_headers(tr_id=str(tr_id or "FHKST01010100"))
        body, _ = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=headers,
            params=params,
        )

        rt_cd = str(body.get("rt_cd", "")).strip()
        msg1 = str(body.get("msg1", "")).strip()
        if rt_cd and rt_cd != "0":
            raise KISApiError(f"inquire_price rejected rt_cd={rt_cd} msg1={msg1} body={body}")

        output = body.get("output", {}) or {}
        return {
            "ok": True,
            "rt_cd": rt_cd,
            "msg1": msg1,
            "tr_id": str(tr_id or "FHKST01010100"),
            "code": code_s,
            "output": output,
            "raw": body,
        }

    def inquire_hoga(
        self,
        *,
        code: str,
        fid_cond_mrkt_div_code: str = "J",
        tr_id: str = "FHKST01010200",
    ) -> Dict[str, Any]:
        """Get level-1 orderbook (ask/bid and queue) snapshot for domestic stock."""
        code_s = str(code).strip().zfill(6)
        if not code_s.isdigit() or len(code_s) != 6:
            raise ValueError(f"Invalid stock code: {code}")

        params = {
            "FID_COND_MRKT_DIV_CODE": str(fid_cond_mrkt_div_code or "J"),
            "FID_INPUT_ISCD": code_s,
        }
        headers = self._auth_headers(tr_id=str(tr_id or "FHKST01010200"))
        body, _ = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
            headers=headers,
            params=params,
        )

        rt_cd = str(body.get("rt_cd", "")).strip()
        msg1 = str(body.get("msg1", "")).strip()
        if rt_cd and rt_cd != "0":
            raise KISApiError(f"inquire_hoga rejected rt_cd={rt_cd} msg1={msg1} body={body}")

        # KIS may return either output1(list/dict) or output(dict), depending on mode/version.
        output = body.get("output1")
        if output is None:
            output = body.get("output", {})
        if isinstance(output, list):
            output = output[0] if len(output) > 0 and isinstance(output[0], dict) else {}
        if not isinstance(output, dict):
            output = {}

        return {
            "ok": True,
            "rt_cd": rt_cd,
            "msg1": msg1,
            "tr_id": str(tr_id or "FHKST01010200"),
            "code": code_s,
            "output": output,
            "raw": body,
        }









