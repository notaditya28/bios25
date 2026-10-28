"""Explainable fuzzy name scoring (pure functions, no I/O).

score = token-alignment F-score of the two names (each token weighted, generic business words
down-weighted), then small, named adjustments for entity type, country and alias quality.
Every adjustment is returned in the breakdown so a reviewer can see why a score is what it is.
"""

from dataclasses import dataclass, field

import jellyfish
from rapidfuzz import fuzz

from app.models.sanctions import EntityType
from app.screening.normalize import GENERIC, NormalizedName, normalize, skeleton

SKELETON_SIM = 0.9  # similarity credited when two tokens share a phonetic skeleton
DISTINCTIVE_HIT = 0.88  # at least one non-generic token must match this well
NO_DISTINCTIVE_CAP = 0.6
TOKEN_FLOOR = 0.75
CONTAINMENT_FACTOR = 0.9  # one name wholly inside the other ("Rosneft" ⊂ "Rosneft Trading")


def _weight(token: str) -> float:
    if token in GENERIC:
        # Low enough that "Rosneft" still flags "Rosneft Trading", high enough that a single
        # shared word ("Reliance Industries" vs "Reliance Marine Services") does not.
        return 0.5
    if len(token) <= 2:
        return 0.5
    return 1.0


def token_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    sim = jellyfish.jaro_winkler_similarity(a, b)
    # Glued Arabic article: "alrashid" vs "rashid".
    for x, y in ((a, b), (b, a)):
        if len(x) >= 6 and x[:2] in ("al", "el") and x[2:] == y:
            return 0.97
    # Short skeletons ("nr" for both Noor and Naria) are too lossy to count as evidence.
    key = skeleton(a)
    if len(key) >= 3 and key == skeleton(b):
        sim = max(sim, SKELETON_SIM)
    # Unrelated words still get ~0.5-0.65 Jaro-Winkler ("industries"/"services"); that is
    # noise, not evidence, so it earns no credit.
    return sim if sim >= TOKEN_FLOOR else 0.0


@dataclass
class NameScore:
    score: float
    pairs: list[tuple[str, str, float]] = field(default_factory=list)
    method: str = "token_alignment"


def name_score(subject: NormalizedName, candidate: NormalizedName) -> NameScore:
    s_tokens, c_tokens = list(subject.tokens), list(candidate.tokens)
    if not s_tokens or not c_tokens:
        return NameScore(0.0)

    pairs = []
    s_num = s_den = 0.0
    distinctive_hit = False
    for s in s_tokens:
        best_c, best = max(((c, token_similarity(s, c)) for c in c_tokens), key=lambda x: x[1])
        w = _weight(s)
        s_num += w * best
        s_den += w
        pairs.append((s, best_c, round(best, 3)))
        if s not in GENERIC and len(s) >= 3 and best >= DISTINCTIVE_HIT:
            distinctive_hit = True
    c_num = c_den = 0.0
    for c in c_tokens:
        best = max(token_similarity(s, c) for s in s_tokens)
        w = _weight(c)
        c_num += w * best
        c_den += w
    s_cov, c_cov = s_num / s_den, c_num / c_den
    aligned = 0.0 if s_cov + c_cov == 0 else 2 * s_cov * c_cov / (s_cov + c_cov)
    method = "token_alignment"
    contained = max(s_cov, c_cov) * CONTAINMENT_FACTOR
    if contained > aligned:
        aligned, method = contained, "containment"

    # Split/joined tokens ("abdul rahman" vs "abdulrahman") are caught on the compact form.
    compact = fuzz.ratio("".join(s_tokens), "".join(c_tokens)) / 100 * 0.97
    result = NameScore(aligned, pairs, method)
    if compact > aligned:
        result = NameScore(compact, pairs, "compact_string")
        distinctive_hit = distinctive_hit or compact >= DISTINCTIVE_HIT
    if not distinctive_hit:
        result.score = min(result.score, NO_DISTINCTIVE_CAP)
        result.method += "+no_distinctive_token"
    return result


@dataclass
class Subject:
    name: str
    kind: str  # "partner" | "company" | "vessel"
    country: str | None = None
    imo: str | None = None
    looks_like_company: bool = False


@dataclass
class Candidate:
    entry_id: object
    name: str
    entity_type: EntityType
    countries: list[str]
    imo_numbers: list[str]
    is_weak: bool = False


@dataclass
class MatchResult:
    score: float  # 0-100
    matched_name: str
    breakdown: dict


def score_candidate(subject: Subject, subj_norm: NormalizedName, cand: Candidate) -> MatchResult:
    adjustments: list[tuple[str, float]] = []

    if subject.imo and subject.imo in cand.imo_numbers:
        return MatchResult(
            100.0,
            cand.name,
            {"method": "imo_exact", "imo": subject.imo, "name_score": None, "adjustments": []},
        )

    ns = name_score(subj_norm, normalize(cand.name))
    score = ns.score

    if subject.kind == "vessel":
        if cand.entity_type != EntityType.vessel:
            adjustments.append(("subject is a vessel, list entry is not", -0.15))
    else:
        if cand.entity_type in (EntityType.vessel, EntityType.aircraft):
            adjustments.append((f"list entry is a {cand.entity_type.value}", -0.10))
        elif cand.entity_type == EntityType.individual and subject.looks_like_company:
            adjustments.append(("subject looks like a company, list entry is a person", -0.08))

    if subject.country and cand.countries:
        if subject.country in cand.countries:
            adjustments.append((f"country {subject.country} also on list entry", +0.03))
        else:
            adjustments.append(("country differs from list entry", -0.05))
    if cand.is_weak:
        adjustments.append(("matched a weak / low-quality alias", -0.05))

    final = max(0.0, min(1.0, score + sum(d for _, d in adjustments)))
    return MatchResult(
        round(final * 100, 1),
        cand.name,
        {
            "method": ns.method,
            "name_score": round(ns.score * 100, 1),
            "token_pairs": ns.pairs,
            "adjustments": [{"reason": r, "delta": round(d * 100, 1)} for r, d in adjustments],
        },
    )
