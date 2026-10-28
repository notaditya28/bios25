"""Name normalisation for sanctions screening.

The goal is that spelling variants of the same name produce the same (or very similar)
normalised form, across scripts and transliteration conventions:

    "Mohammed Al-Rashid Trading LLC"  -> tokens ["mohammed", "rashid", "trading"]
    "МУХАММАД РАШИД"                  -> tokens ["mukhammad", "rashid"]   (via anyascii)

and every token also gets a phonetic *skeleton* (`token_key`) so that
Mohammed / Muhammad / Mohamad / Mukhammad all map to "mhmd", and
Yevgeny / Evgeniy / Evgeni map to "avgn".
"""

import re
import unicodedata
from dataclasses import dataclass

from anyascii import anyascii

# Bump whenever tokenisation or skeleton rules change: stored normalised names are then
# rebuilt on the next list refresh even if the published files are unchanged.
NORMALIZER_VERSION = "2"

# Legal-form and corporate boilerplate: removed entirely (they never distinguish a match).
LEGAL_FORMS = {
    "pvt",
    "private",
    "ltd",
    "limited",
    "llc",
    "llp",
    "lp",
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "co",
    "company",
    "cos",
    "plc",
    "gmbh",
    "mbh",
    "ag",
    "kg",
    "bv",
    "nv",
    "sa",
    "sas",
    "sarl",
    "srl",
    "spa",
    "sl",
    "oy",
    "ab",
    "as",
    "aps",
    "kft",
    "zrt",
    "doo",
    "dd",
    "fze",
    "fzco",
    "fzc",
    "fz",
    "fzllc",
    "dmcc",
    "wll",
    "est",
    "establishment",
    "ooo",
    "oao",
    "zao",
    "pao",
    "ao",
    "pjsc",
    "ojsc",
    "cjsc",
    "jsc",
    "tov",
    "pte",
    "sdn",
    "bhd",
    "tbk",
    "pt",
    "cv",
    "kk",
    "yk",
    "jv",
    "opc",
    "the",
    "and",
    "of",
    "de",
    "del",
    "la",
    "le",
    "et",
    "und",
    "y",
    "for",
}
# Honorifics / titles that appear inconsistently in lists and customer records.
TITLES = {
    "mr",
    "mrs",
    "ms",
    "miss",
    "dr",
    "prof",
    "sir",
    "shri",
    "sri",
    "smt",
    "kumari",
    "haji",
    "hajji",
    "hadji",
    "mullah",
    "mulla",
    "sheikh",
    "shaikh",
    "sheik",
    "shaykh",
    "maulana",
    "maulvi",
    "mawlawi",
    "col",
    "colonel",
    "capt",
    "captain",
    "admiral",
}
# Arabic-name particles that are often written, dropped or glued on ("al-Tikriti").
PARTICLES = {"al", "el", "ul", "bin", "ibn", "bint", "ben"}
# Common but low-information business words: kept, but weighted down when scoring.
GENERIC = {
    "trading",
    "trade",
    "traders",
    "international",
    "intl",
    "general",
    "group",
    "global",
    "industries",
    "industry",
    "industrial",
    "shipping",
    "enterprises",
    "enterprise",
    "exports",
    "export",
    "exporters",
    "imports",
    "import",
    "importers",
    "overseas",
    "holding",
    "holdings",
    "services",
    "service",
    "logistics",
    "marine",
    "maritime",
    "petroleum",
    "oil",
    "gas",
    "energy",
    "investment",
    "investments",
    "commercial",
    "manufacturing",
    "textiles",
    "textile",
    "chemicals",
    "chemical",
    "foods",
    "agro",
    "solutions",
    "technologies",
    "technology",
    "tech",
    "systems",
    "national",
    "united",
    "new",
    "east",
    "west",
    "middle",
    "far",
    "bank",
    "fund",
    "foundation",
    "organization",
    "organisation",
    "society",
    "association",
    "agency",
    "office",
    "center",
    "centre",
    "line",
    "lines",
    "navigation",
    "transport",
    "express",
    "star",
    "sea",
    "ocean",
}

_PUNCT = re.compile(r"[^\w\s]", re.UNICODE)
_SPACE = re.compile(r"\s+")


def transliterate(name: str) -> str:
    """Any script → ASCII (Cyrillic, Arabic, CJK, Greek, Devanagari, ...)."""
    return anyascii(unicodedata.normalize("NFKC", name))


def tokenize(name: str) -> list[str]:
    s = transliterate(name).lower()
    s = s.replace("'", "").replace("`", "")  # O'Neil → oneil, Ma'ruf → maruf
    s = _PUNCT.sub(" ", s)
    tokens = []
    for t in _SPACE.split(s.strip()):
        if not t:
            continue
        # Glued Arabic articles: "altikriti" is left alone, but "al" as its own token goes.
        if t in LEGAL_FORMS or t in TITLES or t in PARTICLES:
            continue
        tokens.append(t)
    return tokens


def skeleton(token: str) -> str:
    """Phonetic key tolerant to transliteration differences.

    Leading iotation is dropped (ye/yu/ya → e/u/a), digraphs are folded (kh→h, ph→f, th→t,
    ck→k, ...), the first letter is kept (vowels collapse to 'a'), later vowels and doubled
    letters are removed. Skeletons are a retrieval/boost signal, not proof of a match:
    Mahmud and Mohammed share "mhmd", and the string-similarity score separates them.
    """
    t = token.lower()
    if t.isdigit():
        return t
    t = re.sub(r"^y(?=[aeiou])", "", t)
    for a, b in (
        ("sch", "sh"),
        ("tch", "ch"),
        ("zh", "j"),
        ("dzh", "j"),
        ("dj", "j"),
        ("ph", "f"),
        ("kh", "h"),
        ("gh", "g"),
        ("dh", "d"),
        ("th", "t"),
        ("ck", "k"),
        ("q", "k"),
        ("x", "ks"),
        ("w", "v"),
        ("z", "s"),
        ("c", "k"),
        ("y", "i"),
        ("j", "i"),
    ):
        t = t.replace(a, b)
    if not t:
        return ""
    first = "a" if t[0] in "aeiou" else t[0]
    rest = re.sub(r"[aeiou]", "", t[1:])
    out = first + rest
    return re.sub(r"(.)\1+", r"\1", out)


@dataclass(frozen=True)
class NormalizedName:
    tokens: tuple[str, ...]
    keys: tuple[str, ...]

    @property
    def norm(self) -> str:
        """Order-independent canonical string (used for trigram retrieval)."""
        return " ".join(sorted(self.tokens))

    @property
    def distinctive_keys(self) -> list[str]:
        return sorted({skeleton(t) for t in self.tokens if t not in GENERIC and len(t) >= 3})


def normalize(name: str) -> NormalizedName:
    tokens = tuple(tokenize(name))
    return NormalizedName(tokens=tokens, keys=tuple(skeleton(t) for t in tokens))


_LEGAL_HINT = {
    "pvt",
    "private",
    "ltd",
    "limited",
    "llc",
    "llp",
    "inc",
    "corp",
    "corporation",
    "co",
    "company",
    "plc",
    "gmbh",
    "fze",
    "fzco",
    "ooo",
    "jsc",
    "pjsc",
    "bv",
    "sa",
    "srl",
    "pte",
}


def looks_like_company(name: str) -> bool:
    raw = _PUNCT.sub(" ", transliterate(name).lower()).split()
    return any(t in _LEGAL_HINT for t in raw) or any(t in GENERIC for t in raw)
