#!/usr/bin/env python3
"""CLI entrypoint for the verification framework."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

DEFAULT_DASHBOARD_STATE = r"E:\vibe\buffett\runs\dashboard_state_latest.json"
DEFAULT_PENDING_STATUS = r"E:\1_Data\2_Logs\pending_entry_status_latest.json"
DEFAULT_DESIGN_EVIDENCE = r"E:\vibe\buffett\runs\design_evidence_latest.json"
DEFAULT_RUNTIME_EVIDENCE = r"E:\1_Data\2_Logs\verification_runtime_evidence_latest.json"

try:
    from . import (
        CHECKPOINTS,
        KoreanMarketConstants,
        TradingSystemConfig,
        TradingSystemVerifier,
        VerificationPhase,
        build_evidence_by_phase,
        evaluate_checkpoint_gate,
        generate_html_report,
        generate_json_report,
    )
except ImportError:  # pragma: no cover - convenience fallback
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from checkfile import (  # type: ignore
        CHECKPOINTS,
        KoreanMarketConstants,
        TradingSystemConfig,
        TradingSystemVerifier,
        VerificationPhase,
        build_evidence_by_phase,
        evaluate_checkpoint_gate,
        generate_html_report,
        generate_json_report,
    )


def print_banner() -> None:
    print("=" * 72)
    print("Trading System Verification Framework")
    print("=" * 72)


def print_market_info() -> None:
    print("Market cost constants (2025 policy)")
    print(f"KOSPI tax: {KoreanMarketConstants.KOSPI_TOTAL_TAX_RATE * 100:.2f}%")
    print(f"KOSDAQ tax: {KoreanMarketConstants.KOSDAQ_TOTAL_TAX_RATE * 100:.2f}%")
    print(f"KRX fee: {KoreanMarketConstants.KRX_FEE_RATE * 100:.4f}%")
    print(f"Typical broker fee: {KoreanMarketConstants.TYPICAL_BROKER_FEE * 100:.3f}%")


def load_config(config_path: str) -> TradingSystemConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        return TradingSystemConfig(**json.load(f))


def _normalize_api_provider(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    val = str(raw).strip()
    if not val:
        return None
    key = val.upper()
    aliases = {
        "KIWOOM": "KIWOOM",
        "KIS": "KIS",
        "EBEST": "EBEST",
        "LS": "EBEST",
        "LSSEC": "EBEST",
        "LS_SEC": "EBEST",
    }
    return aliases.get(key, key)


def _infer_api_provider_from_dashboard(dashboard_state_path: Optional[str]) -> Optional[str]:
    if not dashboard_state_path:
        return None
    try:
        with open(dashboard_state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        try:
            with open(dashboard_state_path, "r", encoding="cp949") as f:
                state = json.load(f)
        except Exception:
            return None

    candidates = [
        state.get("api_provider"),
        (state.get("api") or {}).get("provider") if isinstance(state.get("api"), dict) else None,
        (state.get("broker") or {}).get("provider") if isinstance(state.get("broker"), dict) else None,
        (state.get("system") or {}).get("api_provider") if isinstance(state.get("system"), dict) else None,
    ]
    for c in candidates:
        normalized = _normalize_api_provider(c)
        if normalized:
            return normalized
    return None


def create_default_config() -> TradingSystemConfig:
    return TradingSystemConfig(
        system_name="Korean Stock Trading System",
        version="1.0.0",
        market="KOSPI",
        architecture_type="DUAL_PROCESS",
        api_provider="KIS",
        target_latency_ms=100.0,
        target_tps=10,
        max_single_order_ratio=0.05,
        max_daily_loss_ratio=0.02,
        max_position_ratio=0.20,
        price_deviation_limit=0.03,
        slippage_bp=5,
        backtest_start_date="2015-01-01",
        backtest_end_date="2024-12-31",
        include_delisted=True,
        max_drawdown_limit=0.30,
    )


def run_verification(args: argparse.Namespace):
    config = load_config(args.config) if args.config else create_default_config()

    cli_api_provider = _normalize_api_provider(args.api_provider)
    inferred_api_provider = _infer_api_provider_from_dashboard(args.dashboard_state)

    if cli_api_provider:
        config.api_provider = cli_api_provider
    elif str(config.api_provider or "").upper() in {"", "UNSPECIFIED", "UNKNOWN", "N/A"} and inferred_api_provider:
        config.api_provider = inferred_api_provider

    evidence_by_phase = {}
    if not args.disable_auto_evidence:
        evidence_by_phase = build_evidence_by_phase(
            dashboard_state_path=args.dashboard_state,
            pending_status_path=args.pending_status,
            design_evidence_path=args.design_evidence,
            runtime_evidence_path=args.runtime_evidence,
        )

    verifier = TradingSystemVerifier(config, evidence_by_phase=evidence_by_phase)

    if args.phase:
        phase = VerificationPhase[args.phase.upper()]
        report = verifier.run_phase_verification(phase)
        print(f"Phase completed: {phase.value}")
        print(
            f"pass={report.passed_count} fail={report.failed_count} "
            f"warn={report.warning_count} skip={report.skipped_count}"
        )
        return report

    report = verifier.run_full_verification()

    if args.report:
        output_dir = Path(args.output) if args.output else Path(".")
        output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        if args.report in {"html", "all"}:
            html_path = output_dir / f"verification_report_{ts}.html"
            html_path.write_text(generate_html_report(report), encoding="utf-8")
            print(f"HTML report generated: {html_path}")

        if args.report in {"json", "all"}:
            json_path = output_dir / f"verification_report_{ts}.json"
            json_path.write_text(generate_json_report(report), encoding="utf-8")
            print(f"JSON report generated: {json_path}")

    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Trading system verification framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m checkfile.main\n"
            "  python -m checkfile.main --phase PLANNING\n"
            "  python -m checkfile.main --report all --output ./outputs\n"
            "  python -m checkfile.main --checkpoint pre_order_send\n"
            "  python -m checkfile.main --dashboard-state E:/vibe/buffett/runs/dashboard_state_latest.json\n"
            "  python -m checkfile.main --design-evidence E:/vibe/buffett/runs/design_evidence_latest.json\n"
            "  python -m checkfile.main --runtime-evidence E:/1_Data/2_Logs/verification_runtime_evidence_latest.json\n"
        ),
    )
    parser.add_argument("--phase", "-p", choices=[p.name for p in VerificationPhase])
    parser.add_argument("--config", "-c", help="config JSON path")
    parser.add_argument("--report", "-r", choices=["html", "json", "all"])
    parser.add_argument("--output", "-o", help="report output directory")
    parser.add_argument("--checkpoint", choices=CHECKPOINTS)
    parser.add_argument("--dashboard-state", default=DEFAULT_DASHBOARD_STATE, help="dashboard state JSON path")
    parser.add_argument("--api-provider", help="override API provider (KIWOOM/KIS/EBEST)")
    parser.add_argument("--pending-status", default=DEFAULT_PENDING_STATUS, help="pending status JSON path")
    parser.add_argument("--design-evidence", default=DEFAULT_DESIGN_EVIDENCE, help="design evidence JSON path")
    parser.add_argument("--runtime-evidence", default=DEFAULT_RUNTIME_EVIDENCE, help="runtime evidence JSON path")
    parser.add_argument("--disable-auto-evidence", action="store_true", help="disable automatic evidence loading")
    parser.add_argument("--info", action="store_true", help="print market constants and exit")
    parser.add_argument("--quiet", "-q", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not args.quiet:
        print_banner()

    if args.info:
        print_market_info()
        raise SystemExit(0)

    report = run_verification(args)

    if args.checkpoint and hasattr(report, "phase_reports"):
        decision = evaluate_checkpoint_gate(report, args.checkpoint)
        print(f"checkpoint={decision.checkpoint} action={decision.action}")
        print("reasons:")
        for reason in decision.reasons:
            print(f"- {reason}")
        if decision.action == "BLOCK":
            raise SystemExit(2)
        if decision.action == "REDUCE":
            raise SystemExit(3)
        if decision.action == "DEFERRED":
            raise SystemExit(4)

    if hasattr(report, "is_deployment_ready"):
        raise SystemExit(0 if report.is_deployment_ready else 1)
    if hasattr(report, "is_phase_passed"):
        raise SystemExit(0 if report.is_phase_passed else 1)
    raise SystemExit(0)


if __name__ == "__main__":
    main()


