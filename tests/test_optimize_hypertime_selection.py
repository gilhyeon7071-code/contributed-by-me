import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from optimize_params_v41_1 import HPO_WORST_FOLD_PF_HURDLE, WindowResult, _fold_selection_metrics


def _win(year: int, pf: float) -> WindowResult:
    return WindowResult(
        start=f"{year}-01-01",
        end=f"{year}-12-31",
        n_trades=20,
        pf=pf,
        mean_ret=0.01,
        # [2026-09-09] 이 테스트는 HPO **선정** 로직을 검사한다. 그런데 optimize_params 는
        #   2026-08 수정으로 **OOS 폴드를 전부 홀드아웃으로 예약**해 선정에서 제외한다
        #   (optimize_params_v41_1.py:474-475, 홀드아웃 오염 차단).
        #   split="OOS" 로 두면 선정 대상이 0개가 되어 항상 insufficient_folds 가 나온다.
        #   테스트의 의도(worst fold 가 허들 아래면 막힌다)를 지키려면 선정 대상 split 을 쓴다.
        split="VAL",
        year=year,
    )


class HyperTimeSelectionTests(unittest.TestCase):
    def test_worst_fold_below_hurdle_blocks_candidate(self) -> None:
        metrics = _fold_selection_metrics(
            [_win(2022, 1.2), _win(2023, HPO_WORST_FOLD_PF_HURDLE - 0.01), _win(2024, 1.1)],
            base_score=12.0,
        )

        self.assertFalse(metrics["worst_fold_pass"])
        self.assertEqual(metrics["hypertime_reason"], "worst_fold_below_hurdle")
        self.assertLess(metrics["hypertime_score"], -1e7)

    def test_recent_weighted_prefers_later_folds(self) -> None:
        improving = _fold_selection_metrics([_win(2022, 0.8), _win(2023, 1.0), _win(2024, 1.4)], base_score=5.0)
        fading = _fold_selection_metrics([_win(2022, 1.4), _win(2023, 1.0), _win(2024, 0.8)], base_score=5.0)

        self.assertTrue(improving["worst_fold_pass"])
        self.assertTrue(fading["worst_fold_pass"])
        self.assertGreater(improving["recent_weighted"], fading["recent_weighted"])
        self.assertGreater(improving["hypertime_score"], fading["hypertime_score"])

    def test_minimum_folds_fail_closed(self) -> None:
        metrics = _fold_selection_metrics([_win(2023, 1.2), _win(2024, 1.3)], base_score=12.0)

        self.assertFalse(metrics["worst_fold_pass"])
        self.assertEqual(metrics["hypertime_reason"], "insufficient_folds")
        self.assertLess(metrics["hypertime_score"], -1e8)


if __name__ == "__main__":
    unittest.main()
