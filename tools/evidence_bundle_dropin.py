import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run_text(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip()


def write_python_env(out_dir: Path) -> None:
    info: dict[str, object] = {
        "python_version": sys.version,
        "executable": sys.executable,
        "python_sha256": sha256_file(Path(sys.executable)),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
        },
    }

    try:
        import torch  # type: ignore

        info["torch_version"] = torch.__version__
    except Exception as exc:
        info["torch_version"] = f"NA ({exc})"

    try:
        import tensorflow as tf  # type: ignore

        info["tensorflow_version"] = tf.__version__
    except Exception as exc:
        info["tensorflow_version"] = f"NA ({exc})"

    try:
        info["pip_freeze"] = run_text([sys.executable, "-m", "pip", "freeze"]).splitlines()
    except Exception as exc:
        info["pip_freeze_error"] = str(exc)

    (out_dir / "python_env.txt").write_text(
        json.dumps(info, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_events(out_dir: Path, seed: int) -> None:
    events: dict[str, object] = {"events": []}
    rows: list[dict[str, object]] = events["events"]  # type: ignore[assignment]

    try:
        import tensorflow as tf  # type: ignore

        generator = tf.random.Generator.from_seed(seed, alg="philox")
        tf.random.set_global_generator(generator)
        sample = generator.normal(shape=(4,))
        rows.append({"tf_first_normal": sample.numpy().tolist()})
    except Exception as exc:
        rows.append({"tf_error": str(exc)})

    try:
        import torch  # type: ignore

        torch.manual_seed(seed)
        torch.use_deterministic_algorithms(True)
        sample = torch.randn(4)
        rows.append({"torch_first_normal": sample.tolist()})
    except Exception as exc:
        rows.append({"torch_error": str(exc)})

    blob = json.dumps(events, ensure_ascii=False, sort_keys=True).encode("utf-8")
    (out_dir / "events.json").write_bytes(blob)
    (out_dir / "events.sha256").write_text(
        f"{hashlib.sha256(blob).hexdigest()}  events.json\n",
        encoding="utf-8",
    )


def load_operation_artifacts(path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    if not path.exists():
        return {
            "status": "MISSING",
            "path": str(path),
        }
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {
            "status": "ERROR",
            "path": str(path),
            "error": str(exc),
        }


def write_manifest(
    out_dir: Path,
    run_id: str,
    seed: str,
    operation_artifacts_json: Path | None = None,
) -> None:
    artifacts = {}
    for name in [
        "python_env.txt",
        "time_status.txt",
        "kernel32_meta.txt",
        "events.json",
        "events.sha256",
    ]:
        path = out_dir / name
        artifacts[name] = sha256_file(path) if path.exists() else "NA"

    manifest = {
        "schema": "evidence_manifest_v1",
        "run_id": run_id,
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seed": seed,
        "env": {
            "MKL_CBWR": os.environ.get("MKL_CBWR"),
            "MKL_NUM_THREADS": os.environ.get("MKL_NUM_THREADS"),
            "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS"),
            "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS"),
        },
        "artifacts": artifacts,
    }
    operation_artifacts = load_operation_artifacts(operation_artifacts_json)
    if operation_artifacts is not None:
        manifest["operation_artifacts"] = operation_artifacts
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build reproducibility evidence files")
    parser.add_argument("--mode", choices=["python-env", "events", "manifest"], required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--seed", required=True)
    parser.add_argument("--operation-artifacts-json")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "python-env":
        write_python_env(out_dir)
    elif args.mode == "events":
        write_events(out_dir, int(args.seed))
    elif args.mode == "manifest":
        operation_artifacts_json = (
            Path(args.operation_artifacts_json)
            if args.operation_artifacts_json
            else None
        )
        write_manifest(out_dir, args.run_id, args.seed, operation_artifacts_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
