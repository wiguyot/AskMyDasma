"""mDragStream Point-based streaming model wrapper.

This module intentionally implements only the Point-based variant. The
Subsequence-based variant has different temporal semantics and is deferred.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np

from dasma_kafka.anomaly.thresholds import (
    SlidingPercentileEstimator,
    WelfordEstimator,
    WindowedWelfordEstimator,
    classify_score,
    classify_score_percentile,
)


def classify_mdragpoint_score(
    mdragpoint_score: float | None,
    estimator: WelfordEstimator | WindowedWelfordEstimator,
    *,
    is_warming_up: bool,
    min_samples: int,
    k_sigma: float,
) -> tuple[str, Optional[bool], Optional[float]]:
    """Classify an mDragPoint score with the Welford mean+k*std threshold."""
    return classify_score(
        mdragpoint_score,
        estimator,
        is_warming_up=is_warming_up,
        min_samples=min_samples,
        k_sigma=k_sigma,
    )


def classify_mdragpoint_score_percentile(
    mdragpoint_score: float | None,
    estimator: SlidingPercentileEstimator,
    *,
    is_warming_up: bool,
    min_samples: int,
) -> tuple[str, Optional[bool], Optional[float]]:
    """Classify an mDragPoint score with the sliding-window percentile threshold."""
    return classify_score_percentile(
        mdragpoint_score,
        estimator,
        is_warming_up=is_warming_up,
        min_samples=min_samples,
    )


def classify_mdragpoint_score_internal(
    mdragpoint_score: float | None,
    internal_is_anomaly: bool | None,
    internal_threshold: float | None,
    *,
    is_warming_up: bool,
) -> tuple[str, Optional[bool], Optional[float]]:
    """Classify a score with mDragStream's native Point verdict."""
    if (
        is_warming_up
        or mdragpoint_score is None
        or internal_is_anomaly is None
        or internal_threshold is None
    ):
        return "warming_up", None, None

    is_anomaly = bool(internal_is_anomaly)
    return ("anomaly" if is_anomaly else "normal"), is_anomaly, float(internal_threshold)


@dataclass(frozen=True)
class _Candidate:
    norm: float
    stream_idx: int
    point: tuple[float, ...]


class MDragPointGlobalModel:
    """Point-based mDragStream wrapper matching the DASMA model contract.

    The initial implementation keeps the historical ablation behavior for the
    Point variant, including the deferred B2/B7 threshold quirks documented in
    `runtime/detectors/mdragpoint/mdragstream_point_analysis.md` under
    Known Scientific Limits.
    """

    _SUPPORTED_METRICS = {"mahalanobis", "euclidean", "manhattan", "minkowski"}

    def __init__(
        self,
        num_features: int,
        *,
        dynamic_threshold: bool = False,
        lof_init: bool = False,
        n_init: int = 500,
        max_nb_clusters: int = 30,
        max_cluster_size: int = 30,
        metric: str = "mahalanobis",
        threshold_method: str = "std",
        threshold_t: float = 2.0,
        lof_neighbors: int = 10,
        seed: int = 42,
    ) -> None:
        if num_features < 1:
            raise ValueError(f"num_features must be >= 1 (got {num_features})")
        if n_init < 2:
            raise ValueError(f"n_init must be >= 2 (got {n_init})")
        if max_nb_clusters < 1:
            raise ValueError(
                f"max_nb_clusters must be >= 1 (got {max_nb_clusters})"
            )
        if max_cluster_size < 1:
            raise ValueError(
                f"max_cluster_size must be >= 1 (got {max_cluster_size})"
            )
        if metric not in self._SUPPORTED_METRICS:
            raise ValueError(f"Unsupported metric: {metric}")
        if threshold_method not in {"std", "percentile"}:
            raise ValueError(
                "threshold_method must be either 'std' or 'percentile'"
            )
        if lof_neighbors < 1:
            raise ValueError(f"lof_neighbors must be >= 1 (got {lof_neighbors})")

        self.num_features = int(num_features)
        self.dynamic_threshold = bool(dynamic_threshold)
        self.lof_init = bool(lof_init)
        self.n_init = int(n_init)
        self.max_nb_clusters = int(max_nb_clusters)
        self.max_cluster_size = int(max_cluster_size)
        self.metric = metric
        self.threshold_method = threshold_method
        self.threshold_t = float(threshold_t)
        self.lof_neighbors = int(lof_neighbors)
        self.seed = int(seed)

        self.r: float | None = None
        self.inv_cov: np.ndarray | None = None
        self.clusters: list[list[np.ndarray]] = []
        self.cluster_activity: list[int] = []
        self.candidates: list[_Candidate] = []
        self.anomaly_status: dict[int, int] = {}
        self.anomaly_scores: dict[int, float] = {}
        self.full_scores: dict[int, float] = {}
        self.scores_in_clusters: list[list[float]] = []

        self._init_buffer: list[np.ndarray] = []
        self._stream_idx = 0
        self._initialized = False
        self._last_score: float | None = None
        self._last_is_anomaly: bool | None = None
        self._rng = random.Random(self.seed)

    @property
    def is_warming_up(self) -> bool:
        return not self._initialized

    @property
    def samples_buffered(self) -> int:
        return len(self._init_buffer)

    @property
    def n_seen(self) -> int:
        return self._stream_idx

    @property
    def last_score(self) -> float | None:
        return self._last_score

    @property
    def last_is_anomaly(self) -> bool | None:
        return self._last_is_anomaly

    def get_internal_threshold(self) -> float | None:
        return self.r

    def _ensure_vector(self, x: Iterable[float]) -> np.ndarray:
        vec = np.asarray(list(float(v) for v in x), dtype=np.float64).ravel()
        if len(vec) != self.num_features:
            raise ValueError(
                f"Unexpected feature dimension for MDragPointGlobalModel: "
                f"expected {self.num_features}, got {len(vec)}"
            )
        if not np.all(np.isfinite(vec)):
            raise ValueError("mDragPoint input vector contains non-finite values")
        return vec

    def _compute_inv_cov(self, points: np.ndarray) -> np.ndarray:
        if points.ndim != 2:
            raise ValueError("points must be a 2D array")
        cov = np.cov(points, rowvar=False)
        cov = np.asarray(cov, dtype=np.float64)
        if cov.ndim == 0:
            cov = cov.reshape(1, 1)
        cov_reg = cov + 1e-6 * np.eye(cov.shape[0])
        try:
            return np.linalg.inv(cov_reg)
        except np.linalg.LinAlgError:
            return np.linalg.pinv(cov_reg)

    def _distance(self, p1: np.ndarray, p2: np.ndarray) -> float:
        diff = p1 - p2
        if self.metric == "mahalanobis":
            if self.inv_cov is None:
                raise ValueError("Mahalanobis distance requires inv_cov.")
            value = float(diff.T @ self.inv_cov @ diff)
            return math.sqrt(max(value, 0.0))
        if self.metric == "euclidean":
            return float(np.linalg.norm(diff))
        if self.metric == "manhattan":
            return float(np.abs(diff).sum())
        if self.metric == "minkowski":
            return float(np.power(np.power(np.abs(diff), 3).sum(), 1 / 3))
        raise ValueError(f"Unsupported metric: {self.metric}")

    def _filter_inliers_for_threshold(self, points: np.ndarray) -> np.ndarray:
        if not self.lof_init:
            return points

        from sklearn.neighbors import LocalOutlierFactor

        n_neighbors = min(self.lof_neighbors, len(points) - 1)
        lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=0.01)
        labels = lof.fit_predict(points)
        inliers = points[labels == 1]
        if len(inliers) < 2:
            raise ValueError("Too few inliers after LOF filtering.")
        return inliers

    def _threshold_distance(self, points: np.ndarray) -> float:
        inliers = self._filter_inliers_for_threshold(points)
        distances: list[float] = []

        # Historical ablation behavior: due to bug B2, only the last distance
        # of each outer-loop row is appended. Keep this until the scientific
        # correction phase explicitly changes the baseline.
        for i in range(len(inliers)):
            dist: float | None = None
            for j in range(i + 1, len(inliers)):
                dist = self._distance(inliers[i], inliers[j])
            if dist is not None:
                distances.append(dist)

        if not distances:
            raise ValueError("Too few distances to initialize mDragPoint threshold.")

        scores = np.asarray(distances, dtype=np.float64)
        if self.threshold_method == "std":
            return float(scores.mean() + self.threshold_t * scores.std())
        return float(np.percentile(scores, 95))

    def _initialize(self) -> None:
        data = np.asarray(self._init_buffer, dtype=np.float64)
        if self.metric == "mahalanobis" and self.inv_cov is None:
            # Historical B7 behavior: LOF filters threshold distances only;
            # covariance remains computed on the full init buffer.
            self.inv_cov = self._compute_inv_cov(data)

        if self.r is None:
            self.r = self._threshold_distance(data)

        first_point = data[0]
        self.clusters.append([first_point])
        self.cluster_activity.append(0)
        self.candidates.append(
            _Candidate(float(np.linalg.norm(first_point)), 0, tuple(first_point))
        )
        self.scores_in_clusters.append([0.0])
        self._initialized = True

    def _compute_score(self, point: np.ndarray) -> float:
        if not self.clusters:
            return 0.0
        min_dist_to_cluster = float("inf")
        for cluster in self.clusters:
            for member in cluster:
                distance = self._distance(point, member)
                if distance < min_dist_to_cluster:
                    min_dist_to_cluster = distance
        return float(min_dist_to_cluster)

    def _update_threshold(self) -> None:
        all_scores = [score for scores in self.scores_in_clusters for score in scores]
        if not all_scores:
            return
        scores = np.asarray(all_scores, dtype=np.float64)
        if self.threshold_method == "std":
            self.r = float(scores.mean() + self.threshold_t * scores.std())
        else:
            self.r = float(np.percentile(scores, 95))

    def _replace_oldest_cluster_if_needed(self) -> None:
        if len(self.clusters) < self.max_nb_clusters:
            return
        oldest_idx = self.cluster_activity.index(min(self.cluster_activity))
        self.clusters.pop(oldest_idx)
        self.cluster_activity.pop(oldest_idx)
        self.scores_in_clusters.pop(oldest_idx)

    def _process_point(self, point: np.ndarray, *, compute_score: bool) -> float | None:
        current_idx = self._stream_idx
        self._stream_idx += 1
        is_anomaly = 0

        if not self._initialized:
            self._init_buffer.append(point)
            if len(self._init_buffer) >= self.n_init:
                self._initialize()
            if compute_score:
                self._last_score = 0.0
                self._last_is_anomaly = None
                return 0.0
            return None

        annscore = float("inf")
        is_far_from_anomalies = True
        to_remove: list[_Candidate] = []

        for candidate in self.candidates:
            candidate_point = np.asarray(candidate.point, dtype=np.float64)
            distance = self._distance(point, candidate_point)
            if distance < annscore:
                annscore = distance
            if self.r is not None and distance < self.r:
                to_remove.append(candidate)
                is_far_from_anomalies = False

        for candidate in to_remove:
            self.candidates.remove(candidate)
            self.anomaly_status[candidate.stream_idx] = 0

        min_dist_to_cluster = float("inf")
        closest_cluster: list[np.ndarray] | None = None
        closest_cluster_idx = -1

        for cluster_idx, cluster in enumerate(self.clusters):
            for member in cluster:
                distance = self._distance(point, member)
                if distance < min_dist_to_cluster:
                    min_dist_to_cluster = distance
                    closest_cluster = cluster
                    closest_cluster_idx = cluster_idx

        final_score = float(min_dist_to_cluster)
        self.full_scores[current_idx] = final_score

        if self.r is not None and min_dist_to_cluster > self.r:
            if is_far_from_anomalies:
                is_anomaly = 1
                self.candidates.append(
                    _Candidate(
                        float(np.linalg.norm(point)), current_idx, tuple(point)
                    )
                )
                self.anomaly_status[current_idx] = 1
                self.anomaly_scores[current_idx] = (
                    float(annscore) if annscore != float("inf") else -1.0
                )
            else:
                self._replace_oldest_cluster_if_needed()
                self.clusters.append([point])
                self.cluster_activity.append(current_idx)
                self.scores_in_clusters.append([final_score])
                if self.dynamic_threshold:
                    self._update_threshold()
        elif closest_cluster is not None:
            if len(closest_cluster) < self.max_cluster_size:
                closest_cluster.append(point)
                self.scores_in_clusters[closest_cluster_idx].append(final_score)
                self.cluster_activity[closest_cluster_idx] = current_idx
            else:
                replace_idx = self._choose_replacement_idx(closest_cluster, final_score)
                if replace_idx is not None:
                    closest_cluster[replace_idx] = point
                    self.scores_in_clusters[closest_cluster_idx][
                        replace_idx
                    ] = final_score
                    self.cluster_activity[closest_cluster_idx] = current_idx
            self.anomaly_status.setdefault(current_idx, 0)

        if compute_score:
            self._last_score = final_score
            self._last_is_anomaly = bool(is_anomaly)
            return final_score
        return None

    def _choose_replacement_idx(
        self, cluster: list[np.ndarray], min_dist_to_cluster: float
    ) -> int | None:
        left_idx = None
        right_idx = None
        closest_pair_distance = float("inf")

        for i in range(len(cluster)):
            for j in range(i + 1, len(cluster)):
                distance = self._distance(cluster[i], cluster[j])
                if distance < closest_pair_distance:
                    left_idx = i
                    right_idx = j
                    closest_pair_distance = distance

        if (
            left_idx is not None
            and right_idx is not None
            and closest_pair_distance < min_dist_to_cluster
        ):
            return self._rng.choice([left_idx, right_idx])
        return None

    def partial_fit_score(self, x: Iterable[float]) -> float:
        """Fit the model on one point and return its anomaly score."""
        point = self._ensure_vector(x)
        score = self._process_point(point, compute_score=True)
        return float(score if score is not None else 0.0)

    def fit_partial(self, x: Iterable[float]) -> "MDragPointGlobalModel":
        """Fit the model on one point without returning a score."""
        point = self._ensure_vector(x)
        self._process_point(point, compute_score=False)
        return self

    def score_partial(self, x: Iterable[float]) -> float:
        """Return the current distance-to-cluster score without updating state."""
        point = self._ensure_vector(x)
        return self._compute_score(point)
