"""Finding and redacting personal detail in Storm Events narratives.

The plan for this layer said scrub person names. Measured over the 336,014 narratives
in the 2019 to 2023 release, person names are almost absent: the honorific shape
matches three times and two of those three are shop signs. What the narratives do
carry, on roughly 2,300 of them, is the age and sex of somebody who was killed or
injured, which next to a county and a date is the part that identifies a person.

So there are two detectors and they are reported separately. Collapsing them into one
rate would hide that one of them has almost nothing to find.

Both detectors are built to be wrong in the safe direction for this dataset, which is
to under-redact rather than destroy place names. A capitalised bigram before a
reporting verb looks exactly like a person and is a place or an agency essentially
every time. `Rock River measured` and `Highway Patrol reported` are the shape, and a
detector that takes them redacts the geography this dataset exists to carry.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Dr. is deliberately not an honorific here. Over the five years it matches 272 event
# narratives and 269 of them are Drive in a street name, so treating it as Doctor
# redacts roads.
HONORIFIC = re.compile(r"\b(?P<title>Mr|Mrs|Ms|Miss)\.?\s+(?P<surname>[A-Z][a-z]{1,})\b")

# A shop called Mr Clean is the failure mode for the rule above. The guard reads the
# word after the candidate surname rather than keeping a list of brands.
ORGANISATION_WORDS = frozenset({
    "supermarket", "market", "store", "shop", "car", "wash", "cleaners", "diner",
    "restaurant", "cafe", "pizza", "grill", "bar", "motel", "hotel", "station",
    "company", "inc", "llc", "center", "centre", "plaza", "mall", "lube", "tire",
    "rental", "storage", "laundry", "bbq", "donut", "burger", "chicken",
})

# A capitalised word before 's residence is a role as often as a surname in this data.
ROLE_WORDS = frozenset({
    "observer", "commissioner", "commisioner", "sheriff", "deputy", "mayor", "chief",
    "manager", "spotter", "resident", "neighbor", "neighbour", "owner", "victim",
    "trooper", "officer", "ranger", "operator", "coordinator", "director",
})

POSSESSIVE_HOME = re.compile(
    r"\b(?P<word>[A-Z][a-z]{2,})'s\s+(?P<thing>home|house|residence|property|barn|farm)\b"
)

# Lowe's home improvement store got through the role guard, because Lowe is a surname
# and nothing about the word says shop. What says shop is the word after home, which
# turns it into a compound. A home somebody lives in is not followed by one of these.
COMPOUND_AFTER_HOME = frozenset({
    "improvement", "depot", "center", "centre", "goods", "furnishings", "supply",
    "appliance", "appliances", "hardware", "decor", "fitness", "health",
})

# First [M.] Last immediately followed by an age, or by a word only a person does.
FULL_NAME_WITH_AGE = re.compile(
    r"\b(?P<name>[A-Z][a-z]{2,}(?:\s+[A-Z]\.)?\s+[A-Z][a-z]{2,}),\s*\d{1,3}\s*,"
)

# The age itself, then up to three following lowercase words. The tail is read to
# decide whether this is a person and to extend the span over a sex word, and it is
# not part of the span unless that word is there.
AGE = re.compile(
    r"\b(?P<age>\d{1,3})[-\s]?year[-\s]?old\b(?P<tail>(?:[-\s]+[a-z]+){0,3})",
    re.IGNORECASE,
)

SEX_WORDS = frozenset({"male", "female", "man", "woman", "boy", "girl", "men", "women"})

# An oak does not have a sex and it does have an age. Two narratives in the 97 row
# labelled sample are a 60 year old and a 100 year old tree.
NOT_A_PERSON = frozenset({
    "tree", "oak", "pine", "maple", "elm", "cottonwood", "home", "house", "building",
    "barn", "dam", "bridge", "levee", "church", "school", "structure", "roof", "mobile",
})


@dataclass(frozen=True)
class Span:
    start: int
    end: int
    kind: str
    text: str


def person_names(text: str) -> list[Span]:
    """Name spans, found honorific first and then seeded from the surnames it gives.

    The seeding is what makes the one narrative in the labelled sample that carries
    real names fully covered. Once `Mr. Kitburi` is in hand, `Benjamin Z. Kitburi`
    three sentences later is the same person and nothing else in the text looks like
    that. Without the seed a full name on its own is indistinguishable from a road.
    """
    if not text:
        return []
    found: list[Span] = []
    surnames: set[str] = set()

    for m in HONORIFIC.finditer(text):
        after = text[m.end():m.end() + 40].strip().split()
        nxt = after[0].strip(".,;:").lower() if after else ""
        if nxt in ORGANISATION_WORDS:
            continue
        surnames.add(m.group("surname"))
        found.append(Span(m.start(), m.end(), "name", m.group(0)))

    for m in FULL_NAME_WITH_AGE.finditer(text):
        name = m.group("name")
        if name.split()[-1] in surnames or name.split()[0] in surnames:
            found.append(Span(m.start("name"), m.end("name"), "name", name))

    # Any remaining mention of a surname the honorific already confirmed.
    for sn in surnames:
        for m in re.finditer(rf"\b(?:[A-Z][a-z]{{2,}}\s+(?:[A-Z]\.\s+)?)?{re.escape(sn)}\b", text):
            if any(s.start <= m.start() < s.end for s in found):
                continue
            found.append(Span(m.start(), m.end(), "name", m.group(0)))

    for m in POSSESSIVE_HOME.finditer(text):
        if m.group("word").lower() in ROLE_WORDS or m.group("word").lower() in ORGANISATION_WORDS:
            continue
        after = text[m.end():m.end() + 30].strip().split()
        if after and after[0].strip(".,;:").lower() in COMPOUND_AFTER_HOME:
            continue
        found.append(Span(m.start("word"), m.end("word"), "name", m.group("word")))

    return _dedupe(found)


def casualty_descriptors(text: str) -> list[Span]:
    """Age spans that describe a person, with the non-person nouns guarded out."""
    if not text:
        return []
    found = []
    for m in AGE.finditer(text):
        tail = m.group("tail") or ""
        tail_words = [w.lower() for w in re.findall(r"[A-Za-z]+", tail)]
        if any(w in NOT_A_PERSON for w in tail_words):
            continue
        # The span covers the age phrase, and the sex word too when one follows it.
        # Anything past that is the sentence and belongs to the reader.
        stop = m.end("age") + len(m.group(0)) - (m.end("age") - m.start("age")) - len(tail)
        for w in tail_words:
            if w in SEX_WORDS:
                hit = re.search(rf"\b{w}\b", tail, re.IGNORECASE)
                stop = m.end("tail") - (len(tail) - hit.end())
                break
        found.append(Span(m.start("age"), stop, "casualty", text[m.start("age"):stop]))
    return _dedupe(found)


def _dedupe(spans: list[Span]) -> list[Span]:
    """Longest span wins where two overlap, so a full name beats its own surname."""
    out: list[Span] = []
    for s in sorted(spans, key=lambda s: (s.start, -(s.end - s.start))):
        if out and s.start < out[-1].end:
            if (s.end - s.start) > (out[-1].end - out[-1].start):
                out[-1] = s
            continue
        out.append(s)
    return out


PLACEHOLDER = {"name": "[NAME]", "casualty": "[AGE]"}


def scrub(text: str) -> tuple[str, list[Span]]:
    """The narrative with every found span replaced, and the spans that were replaced."""
    spans = _dedupe(person_names(text) + casualty_descriptors(text))
    out = []
    cursor = 0
    for s in spans:
        out.append(text[cursor:s.start])
        out.append(PLACEHOLDER[s.kind])
        cursor = s.end
    out.append(text[cursor:])
    return "".join(out), spans
