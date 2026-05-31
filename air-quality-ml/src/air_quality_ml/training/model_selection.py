"""Model selection: train nhieu thuat toan + sieu tham so, chon model tot nhat tren validation.

Tuan thu spec: khong dung random CV cho time-series. Moi candidate duoc fit tren
train split (cu) va danh gia tren validation split (moi hon) theo thoi gian. Model
"tot nhat" duoc chon theo metric validation phu hop voi tung task:
  - regression: minimize MAE
  - classification: maximize AUPRC (threshold-independent, hop voi du lieu mat can bang)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from pyspark.sql import DataFrame

from air_quality_ml.training.classification import train_and_predict_classification
from air_quality_ml.training.evaluate_classification import evaluate_classification
from air_quality_ml.training.evaluate_regression import evaluate_regression
from air_quality_ml.training.regression import train_and_predict_regression


# Metric nho hon = tot hon (regression)
_LOWER_IS_BETTER = {"mae", "rmse", "mape", "mae_high_pollution"}


@dataclass(frozen=True)
class Candidate:
    model_type: str
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def label(self) -> str:
        if not self.params:
            return self.model_type
        param_str = ",".join(f"{k}={v}" for k, v in sorted(self.params.items()))
        return f"{self.model_type}[{param_str}]"


@dataclass
class CandidateResult:
    candidate: Candidate
    model: Any
    val_pred: DataFrame
    test_pred: DataFrame
    feature_importance: list[dict[str, Any]]
    val_metrics: dict[str, float]


@dataclass
class SelectionResult:
    best: CandidateResult
    selection_metric: str
    candidates: list[CandidateResult]
    baseline_metrics: Optional[dict[str, float]]

    @property
    def report(self) -> list[dict[str, Any]]:
        best_label = self.best.candidate.label
        return [
            {
                "candidate": result.candidate.label,
                "model_type": result.candidate.model_type,
                "params": result.candidate.params,
                "val_metrics": result.val_metrics,
                "is_best": result.candidate.label == best_label,
            }
            for result in self.candidates
        ]


def default_regression_candidates() -> list[Candidate]:
    """linear = baseline, RF va GBT la cac ung vien chinh."""
    return [
        Candidate("linear", {}),
        Candidate("rf", {"numTrees": 120, "maxDepth": 10, "subsamplingRate": 0.85}),
        Candidate("rf", {"numTrees": 200, "maxDepth": 12, "subsamplingRate": 0.8}),
        Candidate("gbt", {"maxIter": 100, "maxDepth": 5, "stepSize": 0.05, "subsamplingRate": 0.85}),
        Candidate("gbt", {"maxIter": 120, "maxDepth": 8, "stepSize": 0.05, "subsamplingRate": 0.85}),
    ]


def default_classification_candidates() -> list[Candidate]:
    """logistic = baseline, RF va GBT la cac ung vien chinh."""
    return [
        Candidate("logistic", {"maxIter": 100}),
        Candidate("rf", {"numTrees": 150, "maxDepth": 10}),
        Candidate("gbt", {"maxIter": 100, "maxDepth": 5, "stepSize": 0.05}),
        Candidate("gbt", {"maxIter": 120, "maxDepth": 8, "stepSize": 0.05}),
    ]


def candidates_from_config(task: str, search_cfg: dict[str, Any]) -> list[Candidate]:
    """Xac dinh danh sach candidate tu config.

    - Neu search_cfg.candidates ton tai -> dung danh sach do.
    - Nguoc lai -> dung luoi mac dinh theo task.
    """
    raw = search_cfg.get("candidates")
    if raw:
        candidates = []
        for item in raw:
            model_type = str(item["model_type"])
            params = dict(item.get("params", {}))
            candidates.append(Candidate(model_type, params))
        return candidates

    return default_regression_candidates() if task == "regression" else default_classification_candidates()


def _is_better(metric: str, new_value: float, current_best: float) -> bool:
    if metric in _LOWER_IS_BETTER:
        return new_value < current_best
    return new_value > current_best


def _initial_best(metric: str) -> float:
    return float("inf") if metric in _LOWER_IS_BETTER else float("-inf")


def select_best_regression(
    train_df: DataFrame,
    val_df: DataFrame,
    test_df: DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
    label_col: str,
    prediction_col: str,
    candidates: list[Candidate],
    high_pollution_threshold: float = 75.0,
    selection_metric: str = "mae",
    on_candidate: Optional[Callable[[Candidate, dict[str, float]], None]] = None,
) -> SelectionResult:
    results: list[CandidateResult] = []
    best_result: Optional[CandidateResult] = None
    best_score = _initial_best(selection_metric)
    baseline_metrics: Optional[dict[str, float]] = None

    for candidate in candidates:
        model, val_pred, test_pred, feature_importance = train_and_predict_regression(
            train_df=train_df,
            val_df=val_df,
            test_df=test_df,
            numeric_features=numeric_features,
            categorical_features=categorical_features,
            label_col=label_col,
            prediction_col=prediction_col,
            model_type=candidate.model_type,
            params=candidate.params,
        )
        val_metrics = evaluate_regression(
            val_pred,
            label_col=label_col,
            prediction_col=prediction_col,
            high_pollution_threshold=high_pollution_threshold,
        )
        result = CandidateResult(candidate, model, val_pred, test_pred, feature_importance, val_metrics)
        results.append(result)

        if candidate.model_type in {"linear", "lr"} and baseline_metrics is None:
            baseline_metrics = val_metrics

        if on_candidate is not None:
            on_candidate(candidate, val_metrics)

        score = float(val_metrics.get(selection_metric, _initial_best(selection_metric)))
        if best_result is None or _is_better(selection_metric, score, best_score):
            best_result = result
            best_score = score

    if best_result is None:
        raise ValueError("No regression candidate was trained")

    return SelectionResult(best_result, selection_metric, results, baseline_metrics)


def select_best_classification(
    train_df: DataFrame,
    val_df: DataFrame,
    test_df: DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
    label_col: str,
    prediction_col: str,
    probability_col: str,
    candidates: list[Candidate],
    selection_metric: str = "auprc",
    on_candidate: Optional[Callable[[Candidate, dict[str, float]], None]] = None,
) -> SelectionResult:
    results: list[CandidateResult] = []
    best_result: Optional[CandidateResult] = None
    best_score = _initial_best(selection_metric)
    baseline_metrics: Optional[dict[str, float]] = None

    for candidate in candidates:
        model, val_pred, test_pred, feature_importance = train_and_predict_classification(
            train_df=train_df,
            val_df=val_df,
            test_df=test_df,
            numeric_features=numeric_features,
            categorical_features=categorical_features,
            label_col=label_col,
            prediction_col=prediction_col,
            probability_col=probability_col,
            model_type=candidate.model_type,
            params=candidate.params,
        )
        # AUPRC/AUC doc lap nguong (dung rawPrediction); cac metric khac o nguong mac dinh 0.5
        val_metrics = evaluate_classification(
            val_pred,
            label_col=label_col,
            prediction_col=prediction_col,
            raw_prediction_col="rawPrediction",
        )
        result = CandidateResult(candidate, model, val_pred, test_pred, feature_importance, val_metrics)
        results.append(result)

        if candidate.model_type in {"logistic", "lr"} and baseline_metrics is None:
            baseline_metrics = val_metrics

        if on_candidate is not None:
            on_candidate(candidate, val_metrics)

        score = float(val_metrics.get(selection_metric, _initial_best(selection_metric)))
        if best_result is None or _is_better(selection_metric, score, best_score):
            best_result = result
            best_score = score

    if best_result is None:
        raise ValueError("No classification candidate was trained")

    return SelectionResult(best_result, selection_metric, results, baseline_metrics)
