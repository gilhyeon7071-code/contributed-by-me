#!/usr/bin/env python3
"""Demo script for the verification package."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

try:
    from . import (
        CHECKPOINTS,
        KoreanMarketConstants,
        TradingSystemConfig,
        TradingSystemVerifier,
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
        evaluate_checkpoint_gate,
        generate_html_report,
        generate_json_report,
    )


def print_header(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def build_demo_config() -> TradingSystemConfig:
    return TradingSystemConfig(
        system_name="Demo Trading System",
        version="1.1.0",
        market="KOSPI",
        architecture_type="DUAL_PROCESS",
        api_provider="KIWOOM",
        target_latency_ms=100.0,
        target_tps=10,
        max_single_order_ratio=0.05,
        max_daily_loss_ratio=0.02,
        max_position_ratio=0.20,
        price_deviation_limit=0.03,
        slippage_bp=5,
        include_delisted=True,
        max_drawdown_limit=0.30,
    )


def show_constants() -> None:
    print_header("Market Constants")
    print(f"KOSPI tax: {KoreanMarketConstants.KOSPI_TOTAL_TAX_RATE*100:.2f}%")
    print(f"KOSDAQ tax: {KoreanMarketConstants.KOSDAQ_TOTAL_TAX_RATE*100:.2f}%")
    print(f"KRX fee: {KoreanMarketConstants.KRX_FEE_RATE*100:.4f}%")
    print(f"Broker fee: {KoreanMarketConstants.TYPICAL_BROKER_FEE*100:.3f}%")


def write_reports(report, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = output_dir / f"verification_report_{ts}.html"
    json_path = output_dir / f"verification_report_{ts}.json"

    html_path.write_text(generate_html_report(report), encoding="utf-8")
    json_path.write_text(generate_json_report(report), encoding="utf-8")

    print_header("Reports")
    print(f"HTML: {html_path}")
    print(f"JSON: {json_path}")


def show_summary(report) -> None:
    print_header("Summary")
    print(f"total={report.total_tests}")
    print(f"passed={report.total_passed}")
    print(f"failed={report.total_failed}")
    print(f"warnings={report.total_warnings}")
    print(f"skipped={report.total_skipped}")
    print(f"pass_rate={report.overall_pass_rate:.1f}%")
    print(f"deployment_ready={report.is_deployment_ready}")


def show_checkpoint_gates(report) -> None:
    print_header("Checkpoint Gates")
    for cp in CHECKPOINTS:
        decision = evaluate_checkpoint_gate(report, cp)
        print(f"- {cp}: {decision.action} ({', '.join(decision.reasons)})")


def main() -> None:
    show_constants()

    config = build_demo_config()
    verifier = TradingSystemVerifier(config)

    print_header("Run Verification")
    report = verifier.run_full_verification()

    outputs = Path(__file__).resolve().parent / "outputs"
    write_reports(report, outputs)
    show_summary(report)
    show_checkpoint_gates(report)


if __name__ == "__main__":
    main()

