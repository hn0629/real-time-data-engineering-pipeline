from typing import Any, Dict, List

from llm.analytics_backend import (
    daily_summary,
    latest_prices,
    load_pipeline_status,
)
from llm.query_router import AssistantRequest, parse_question


HELP_MESSAGE = (
    "I can summarize the latest pipeline output, retrieve "
    "the latest price for one or more symbols, or retrieve "
    "a daily summary for a symbol and date.\n\n"
    "Examples:\n"
    "- Summarize the latest pipeline output\n"
    "- What is the latest price for AAPL?\n"
    "- Show latest prices for AAPL, MSFT, and NVDA\n"
    "- Give me AAPL summary for 2026-09-15\n\n"
    "This assistant provides pipeline-data lookup only. "
    "It does not provide financial advice, forecasts, or "
    "trading recommendations."
)


def _value(
    payload: Dict[str, Any],
    *names: str,
) -> Any:
    for name in names:
        if name in payload:
            return payload[name]

    return None


def _format_status() -> str:
    response = load_pipeline_status()

    if not response["available"]:
        return response["message"]

    status = response["status"]
    lines: List[str] = ["Latest pipeline status:"]

    overall_status = _value(status, "status", "state")

    checked_at = _value(
        status,
        "checked_at_utc",
        "generated_at",
        "checked_at",
        "timestamp",
        "updated_at",
    )

    services = status.get("services")

    errors_seen = _value(
        status,
        "errors_seen",
        "errors",
    )

    if overall_status:
        lines.append(
            f"- Overall status: {overall_status}"
        )

    if checked_at:
        lines.append(
            f"- Checked at (UTC): {checked_at}"
        )

    if isinstance(services, dict):
        lines.append("- Service checks:")

        for service_name, service in services.items():
            if isinstance(service, dict):
                service_status = _value(
                    service,
                    "status",
                    "state",
                )

                message = _value(
                    service,
                    "message",
                    "detail",
                    "reason",
                )

                line = (
                    f"  - {service_name}: "
                    f"{service_status or 'unknown'}"
                )

                if message:
                    line += f" — {message}"

                lines.append(line)
            else:
                lines.append(
                    f"  - {service_name}: {service}"
                )

    if isinstance(errors_seen, list):
        if errors_seen:
            joined_errors = "; ".join(
                str(error)
                for error in errors_seen
            )

            lines.append(
                f"- Errors seen: {joined_errors}"
            )
        else:
            lines.append("- Errors seen: none")

    ignored_keys = {
        "status",
        "state",
        "checked_at_utc",
        "generated_at",
        "checked_at",
        "timestamp",
        "updated_at",
        "services",
        "errors_seen",
        "errors",
    }

    for name, result in status.items():
        if name not in ignored_keys:
            lines.append(f"- {name}: {result}")

    return "\n".join(lines)


def _format_number(value: Any) -> str:
    if value is None:
        return "unknown"

    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def _format_latest_prices(
    rows: List[Dict[str, Any]],
) -> str:
    if not rows:
        return (
            "No matching symbols were found in the latest "
            "analytics partition."
        )

    lines = ["Latest available prices:"]

    for row in rows:
        lines.append(
            "- {symbol}: {price} from {source} "
            "(event date {event_date}, last event "
            "{last_event_time}, {tick_count} ticks)".format(
                symbol=row["symbol"],
                price=_format_number(row["latest_price"]),
                source=row["source"],
                event_date=row["event_date"],
                last_event_time=row["last_event_time"],
                tick_count=row["tick_count"],
            )
        )

    return "\n".join(lines)


def _format_daily_summary(
    symbol: str,
    event_date: str,
    rows: List[Dict[str, Any]],
) -> str:
    if not rows:
        return (
            f"No analytics summary was found for {symbol} "
            f"on {event_date}. Check whether that "
            "event_date partition exists."
        )

    lines = [
        f"Daily summary for {symbol} on {event_date}:"
    ]

    for row in rows:
        lines.append(
            "- Source {source}: {tick_count} ticks; "
            "min={min_price}, max={max_price}, "
            "average={avg_price}, latest={latest_price}; "
            "window={first_event_time} to "
            "{last_event_time}.".format(
                source=row["source"],
                tick_count=row["tick_count"],
                min_price=_format_number(row["min_price"]),
                max_price=_format_number(row["max_price"]),
                avg_price=_format_number(row["avg_price"]),
                latest_price=_format_number(
                    row["latest_price"]
                ),
                first_event_time=row["first_event_time"],
                last_event_time=row["last_event_time"],
            )
        )

    return "\n".join(lines)


def answer(question: str) -> str:
    request: AssistantRequest = parse_question(question)

    if request.intent == "pipeline_summary":
        return _format_status()

    if request.intent in {
        "latest_price",
        "latest_prices",
    }:
        return _format_latest_prices(
            latest_prices(request.symbols)
        )

    if (
        request.intent == "daily_summary"
        and request.event_date
        and request.symbols
    ):
        return _format_daily_summary(
            request.symbols[0],
            request.event_date,
            daily_summary(
                request.symbols[0],
                request.event_date,
            ),
        )

    return HELP_MESSAGE