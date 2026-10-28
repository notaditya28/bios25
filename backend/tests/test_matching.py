"""Matcher quality checks: transliteration variants must match; unrelated names must not."""

import pytest

from app.models.sanctions import EntityType
from app.screening.match import Candidate, Subject, name_score, score_candidate
from app.screening.normalize import looks_like_company, normalize, skeleton, tokenize

THRESHOLD = 85


def ns(a: str, b: str) -> float:
    return name_score(normalize(a), normalize(b)).score * 100


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ("Mohammed", "Muhammad"),
        ("Mohamad", "Mukhammad"),
        ("Yevgeny", "Evgeniy"),
        ("Osama", "Usama"),
    ],
)
def test_skeleton_variants(a: str, b: str) -> None:
    assert skeleton(a.lower()) == skeleton(b.lower())


def test_tokenize_strips_noise() -> None:
    assert tokenize("M/s. Al-Rashid Trading Co. (Pvt.) Ltd.") == ["m", "s", "rashid", "trading"]
    assert tokenize("Sheikh Mohammed bin Rashid") == ["mohammed", "rashid"]
    assert tokenize("ООО Ромашка") == ["romashka"]  # Cyrillic → Latin, legal form dropped


@pytest.mark.parametrize(
    ("subject", "listed"),
    [
        ("Mohammed Hassan Akhund", "AKHUND, Muhammad Hasan"),
        ("Yevgeny Prigozhin", "PRIGOZHIN, Evgeniy Viktorovich"),
        ("Евгений Пригожин", "PRIGOZHIN, Yevgeniy"),
        ("Al Rashid Trading Company LLC", "AL-RASHID TRADING COMPANY"),
        ("Abdulrahman Al Qahtani", "Abdul Rahman AL-QAHTANI"),
        ("Rosneft Trading S.A.", "ROSNEFT TRADING SA"),
        ("Islamic Revolutionary Guard Corps", "Islamic Revolutionary Guards Corps"),
        ("Saddam Hussein Al Tikriti", "Saddam Hussein Al-Tikriti"),
        ("Rosneft", "Rosneft Trading"),
        # A one-letter spelling variant must still reach review.
        ("Tata Steel Limited", "Tat Steel Trading"),
    ],
)
def test_true_matches_score_high(subject: str, listed: str) -> None:
    assert ns(subject, listed) >= THRESHOLD, ns(subject, listed)


@pytest.mark.parametrize(
    ("subject", "listed"),
    [
        ("Global Trading LLC", "Global Shipping Co"),
        ("Shree Ganesh Textiles Pvt Ltd", "Ganesh Shipping"),
        ("Van Dijk Trading B.V.", "Dijkstra Holdings"),
        ("Bharat Forge Ltd", "Bharat Shipping Lines"),
        ("Hamburg Chemie GmbH", "Hamburg Shipping"),
        ("Reliance Industries", "Reliance Marine Services"),
        # Regression: "general" was dropped as a title and "noor"/"naria" share skeleton "nr".
        ("Al Noor General Trading LLC", "NARIA GENERAL TRADING LLC"),
    ],
)
def test_unrelated_names_score_low(subject: str, listed: str) -> None:
    assert ns(subject, listed) < THRESHOLD, ns(subject, listed)


def test_generic_only_overlap_is_capped() -> None:
    r = name_score(normalize("International Trading Services"), normalize("Trading International"))
    assert r.score <= 0.6 and "no_distinctive_token" in r.method


def _cand(name: str, et: EntityType = EntityType.entity, countries=None, imo=None, weak=False):  # type: ignore[no-untyped-def]
    return Candidate("x", name, et, countries or [], imo or [], weak)


def test_imo_exact_match_is_100() -> None:
    subj = Subject("Some Other Name", "vessel", imo="9562233")
    r = score_candidate(
        subj, normalize(subj.name), _cand("PETREL 8", EntityType.vessel, imo=["9562233"])
    )
    assert r.score == 100 and r.breakdown["method"] == "imo_exact"


def test_adjustments_are_explained() -> None:
    subj = Subject(
        "Rashid Trading LLC",
        "partner",
        country="AE",
        looks_like_company=looks_like_company("Rashid Trading LLC"),
    )
    person = score_candidate(
        subj,
        normalize(subj.name),
        _cand("Rashid TRADING", EntityType.individual, ["IR"], weak=True),
    )
    entity = score_candidate(
        subj, normalize(subj.name), _cand("Rashid TRADING", EntityType.entity, ["AE"])
    )
    reasons = [a["reason"] for a in person.breakdown["adjustments"]]
    assert any("person" in r for r in reasons) and any("weak" in r for r in reasons)
    assert entity.score > person.score
