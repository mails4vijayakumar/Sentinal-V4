from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass, field

import hdbscan
import numpy as np

logger = logging.getLogger("agent8.cluster")


@dataclass
class ClusterRaw:
    assignment_group: str
    member_indices: list[int]
    cohesion: float
    medoid_index: int
    signature: str = field(default="")


def _cosine_cohesion(vectors: np.ndarray) -> float:
    """Median pairwise cosine similarity within the cluster."""
    if len(vectors) < 2:
        return 1.0
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    normed = vectors / np.clip(norms, 1e-12, None)
    sim = normed @ normed.T
    iu = np.triu_indices(len(vectors), k=1)
    return float(np.median(sim[iu]))


def _medoid(vectors: np.ndarray) -> int:
    """Index of the point with the smallest sum of cosine distances to the rest."""
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    normed = vectors / np.clip(norms, 1e-12, None)
    sim = normed @ normed.T
    dist = 1.0 - sim
    return int(np.argmin(dist.sum(axis=1)))


def _signature(team: str, member_numbers: list[str]) -> str:
    canon = ",".join(sorted(member_numbers))
    h = hashlib.sha1(f"{team}:{canon}".encode()).hexdigest()[:12]
    return f"{team}_{h}"


def cluster_per_team(
    incidents: list[dict],
    embeddings: list[list[float]],
    *,
    min_cluster_size: int = 5,
    min_samples: int = 3,
) -> list[ClusterRaw]:
    """Run HDBSCAN once per assignment_group; return one ClusterRaw per surviving cluster."""
    if len(incidents) != len(embeddings):
        raise ValueError("incidents and embeddings must be same length")

    by_team: dict[str, list[int]] = defaultdict(list)
    for idx, inc in enumerate(incidents):
        by_team[inc.get("assignment_group") or "_unknown"].append(idx)

    all_clusters: list[ClusterRaw] = []
    for team, idxs in by_team.items():
        if len(idxs) < min_cluster_size:
            continue
        vecs = np.array([embeddings[i] for i in idxs])
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            metric="euclidean",  # we'll feed L2-normalised vectors so euclidean ≈ cosine ordering
            cluster_selection_method="eom",
        )
        # Normalise to unit length so euclidean distance ranks like cosine distance
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        normed = vecs / np.clip(norms, 1e-12, None)
        labels = clusterer.fit_predict(normed)

        for label in sorted(set(labels)):
            if label == -1:  # noise
                continue
            local_members = [i for i, lab in enumerate(labels) if lab == label]
            global_members = [idxs[i] for i in local_members]
            cluster_vecs = vecs[local_members]
            cohesion = _cosine_cohesion(cluster_vecs)
            medoid_local = _medoid(cluster_vecs)
            medoid_global = idxs[local_members[medoid_local]]
            sig = _signature(team, [incidents[g]["number"] for g in global_members])
            all_clusters.append(ClusterRaw(
                assignment_group=team,
                member_indices=global_members,
                cohesion=cohesion,
                medoid_index=medoid_global,
                signature=sig,
            ))
            logger.info("cluster_built", extra={
                "team": team, "size": len(global_members), "cohesion": cohesion,
            })
    return all_clusters


def apply_quality_gate(
    clusters: list[ClusterRaw],
    *,
    min_cohesion: float = 0.65,
) -> list[ClusterRaw]:
    """Drop clusters whose pairwise cohesion is below the threshold."""
    return [c for c in clusters if c.cohesion >= min_cohesion]


def pick_representatives(
    cluster_vectors: list[list[float]],
    *,
    medoid_local_index: int,
    k: int = 4,
) -> list[int]:
    """Return local indices: [medoid] + k nearest neighbours by cosine similarity."""
    vecs = np.array(cluster_vectors)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    normed = vecs / np.clip(norms, 1e-12, None)
    medoid_vec = normed[medoid_local_index]
    sims = normed @ medoid_vec  # higher = closer
    sims[medoid_local_index] = -np.inf  # exclude self from neighbour pick
    top_k = np.argsort(-sims)[:k].tolist()
    return [medoid_local_index, *top_k]
