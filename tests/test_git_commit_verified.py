"""검증된 커밋 도구 — 이상에서 우는지 본다 (2026-09-20).

막으려는 것: 2026-09-20 에 세 번 나온 '담기지 않았는데 담았다고 보고' 다.
그래서 정상 침묵보다 **이상 경보**가 본체다."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import git_commit_verified as T  # noqa: E402


def git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.fixture()
def repo(tmp_path):
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.email", "t@t")
    git(tmp_path, "config", "user.name", "t")
    (tmp_path / "seed.txt").write_text("seed\n", encoding="utf-8")
    git(tmp_path, "add", "seed.txt")
    git(tmp_path, "commit", "-qm", "seed")
    return tmp_path


def call(repo, *paths, msg="m"):
    return T.main(["-m", msg, "-C", str(repo), *paths])


def test_clean_commit_reports_ok(repo, capsys):
    (repo / "a.py").write_text("x=1\n", encoding="utf-8")
    assert call(repo, "a.py") == 0
    assert "[OK]" in capsys.readouterr().out
    assert T.dirty_paths(repo) == set()


def test_nothing_to_do_is_not_a_success_claim(repo, capsys):
    assert call(repo) == 0
    assert "[SKIP]" in capsys.readouterr().out


def test_ignored_file_fails_loudly_instead_of_silently_skipping(repo, capsys):
    """.gitignore 에 걸린 파일을 담으라고 하면 — 오늘의 실패 형태 —
    조용히 넘어가지 않고 커밋 자체를 하지 않는다."""
    (repo / ".gitignore").write_text("data.csv\n", encoding="utf-8")
    git(repo, "add", ".gitignore")
    git(repo, "commit", "-qm", "ignore")
    (repo / "data.csv").write_text("1\n", encoding="utf-8")
    assert call(repo, "data.csv") == 1
    assert "[FAIL] add 실패" in capsys.readouterr().out
    # 커밋이 만들어지지 않았다
    log = subprocess.run(["git", "log", "--oneline"], cwd=repo, capture_output=True, text=True).stdout
    assert log.count("\n") == 2


def test_count_mismatch_fails(repo, capsys):
    """대상 2건 중 1건만 실제 변경이면 커밋에는 1건만 들어간다 -> 수가 안 맞으므로 실패."""
    (repo / "a.py").write_text("x=1\n", encoding="utf-8")
    assert call(repo, "a.py", "seed.txt") == 1
    assert "수가 맞지 않는다" in capsys.readouterr().out


def test_target_left_dirty_fails(repo, capsys, monkeypatch):
    """커밋 뒤에도 대상이 더러우면(동시 수정 등) 성공이라 말하지 않는다."""
    (repo / "a.py").write_text("x=1\n", encoding="utf-8")
    real = T.dirty_paths
    calls = {"n": 0}

    def fake(cwd):
        calls["n"] += 1
        return real(cwd) | {"a.py"} if calls["n"] > 1 else real(cwd)

    monkeypatch.setattr(T, "dirty_paths", fake)
    assert call(repo, "a.py") == 1
    assert "커밋 뒤에도 남아 있는 대상" in capsys.readouterr().out


def test_timeout_is_not_reported_as_success(monkeypatch):
    """파이프가 124 를 0 으로 바꾸던 자리 — run() 은 시간 초과를 그대로 돌려준다."""
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="git", timeout=1)

    monkeypatch.setattr(T.subprocess, "run", boom)
    rc, _, err = T.run(["status"], ".", timeout=1)
    assert rc == 124 and "TIMEOUT" in err


def test_korean_commit_message_does_not_crash_the_verifier(repo, capsys):
    """2026-09-20 실측: Windows cp949 로 git 출력을 읽어 검증 단계에서 터졌다.
    커밋은 나갔는데 검증이 죽으면 '됐는지' 를 알 수 없다 — 그 자리를 막는다."""
    (repo / "b.py").write_text("y=2\n", encoding="utf-8")
    assert call(repo, "b.py", msg="한글 메시지 — 검증된 커밋 확인") == 0
    assert "[OK]" in capsys.readouterr().out
