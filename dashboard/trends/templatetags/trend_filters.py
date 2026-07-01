"""Custom template filters for CineTrends."""
from django import template

register = template.Library()


@register.filter
def trend_arrow(value):
    """Return trend arrow based on position change. Positive = up."""
    try:
        val = int(value)
        if val > 0:
            return "↑"
        elif val < 0:
            return "↓"
        return "→"
    except (ValueError, TypeError):
        return "→"


@register.filter
def trend_color(value):
    """Return CSS class based on trend direction."""
    try:
        val = int(value)
        if val > 0:
            return "trend-up"
        elif val < 0:
            return "trend-down"
        return "trend-neutral"
    except (ValueError, TypeError):
        return "trend-neutral"


@register.filter
def format_currency(value):
    """Format number as currency: $160M, $1.2B."""
    try:
        val = float(value)
        if val >= 1_000_000_000:
            return f"${val / 1_000_000_000:.1f}B"
        elif val >= 1_000_000:
            return f"${val / 1_000_000:.0f}M"
        elif val > 0:
            return f"${val:,.0f}"
        return "N/A"
    except (ValueError, TypeError):
        return "N/A"


@register.filter
def format_number(value):
    """Add commas to large numbers."""
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return str(value)


@register.filter
def abs(value):
    """Return absolute value."""
    try:
        return builtins_abs(int(value))
    except (ValueError, TypeError):
        return value


# Keep Python's builtin abs accessible
import builtins
builtins_abs = builtins.abs


@register.filter
def star_rating(value):
    """Convert vote_average (0-10) to star display."""
    try:
        val = float(value)
        full = int(val / 2)
        half = 1 if (val / 2) - full >= 0.5 else 0
        return "★" * full + "½" * half + "☆" * (5 - full - half)
    except (ValueError, TypeError):
        return "☆☆☆☆☆"
