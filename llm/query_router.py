import re
from dataclasses import dataclass
from typing import List, Optional


SYMBOL_PATTERN = re.compile(r"^[A-Z]{1,10}$")
DATE_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")

IGNORED_WORDS = {
    "A",
    "ALL",
    "AND",
    "BY",
    "DAILY",
    "FOR",
    "GIVE",
    "I",
    "LATEST",
    "ME",
    "OF",
    "ON",
    "PRICE",
    "PRICES",
    "SHOW",
    "SUMMARY",
    "SUMMARIZE",
    "THE",
    "WHAT",
    "WITH",
}


@dataclass(frozen=True)
class AssistantRequest:
    intent: str
    symbols: List[str]
    event_date: Optional[str] = None


def _symbols_from_text(text: str) -> List[str]:
    candidates = re.findall(r"\b[A-Za-z]{1,10}\b", text.upper())
    symbols: List[str] = []

    for candidate in candidates:
        if candidate in IGNORED_WORDS:
            continue

        if SYMBOL_PATTERN.fullmatch(candidate) and candidate not in symbols:
            symbols.append(candidate)

    return symbols[:20]


def parse_question(question: str) -> AssistantRequest:
    normalized = " ".join(question.strip().lower().split())

    if not normalized:
        return AssistantRequest(
            intent="unsupported",
            symbols=[],
        )

    pipeline_words = {
        "summary",
        "summarize",
        "status",
        "output",
        "health",
    }

    if "pipeline" in normalized and any(
        word in normalized for word in pipeline_words
    ):
        return AssistantRequest(
            intent="pipeline_summary",
            symbols=[],
        )

    symbols = _symbols_from_text(question)
    date_match = DATE_PATTERN.search(question)

    is_daily_summary = (
        "summary" in normalized
        and bool(symbols)
        and date_match is not None
    )

    if is_daily_summary:
        return AssistantRequest(
            intent="daily_summary",
            symbols=[symbols[0]],
            event_date=date_match.group(0),
        )

    price_phrases = (
        "latest price",
        "latest prices",
        "current price",
        "current prices",
    )

    if symbols and any(
        phrase in normalized for phrase in price_phrases
    ):
        intent = (
            "latest_prices"
            if len(symbols) > 1
            else "latest_price"
        )

        return AssistantRequest(
            intent=intent,
            symbols=symbols,
        )

    return AssistantRequest(
        intent="unsupported",
        symbols=[],
    )