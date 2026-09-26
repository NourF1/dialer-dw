"""Tests for pure function gap calculation in gap_finder module."""

from datetime import date, timedelta
import pytest

from extractor.gap_finder import compute_missing, generate_expected_weekdays


def test_generate_expected_weekdays_excludes_weekends():
    """Verifies that Saturday and Sunday are excluded from expected range."""
    # 2026-09-18 is Friday, 09-19 Sat, 09-20 Sun, 09-21 Mon
    start = date(2026, 9, 18)
    through = date(2026, 9, 21)

    expected = generate_expected_weekdays(start, through)

    assert expected == [date(2026, 9, 18), date(2026, 9, 21)]


def test_compute_missing_all_completed():
    """Returns empty list when all expected dates are completed."""
    expected = [date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)]
    completed = {date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)}

    result = compute_missing(expected=expected, completed=completed, max_dates=10)
    assert result == []


def test_compute_missing_some_absent():
    """Returns missing dates in original order (oldest first)."""
    d1 = date(2026, 9, 21)
    d2 = date(2026, 9, 22)
    d3 = date(2026, 9, 23)
    d4 = date(2026, 9, 24)

    expected = [d1, d2, d3, d4]
    completed = {d1, d3}

    result = compute_missing(expected=expected, completed=completed, max_dates=10)
    assert result == [d2, d4]


def test_compute_missing_with_weekend_filtering():
    """End-to-end pure test verifying weekend gaps are ignored and missing weekdays are detected."""
    # Range spanning Thursday 09-17 through Monday 09-21
    start = date(2026, 9, 17)    # Thursday
    through = date(2026, 9, 21)  # Monday

    expected = generate_expected_weekdays(start, through)  # Thu(17), Fri(18), Mon(21)
    
    # Simulate Friday (09-18) succeeded, but Thu (09-17) and Mon (09-21) were missed
    completed = {date(2026, 9, 18)}

    result = compute_missing(expected=expected, completed=completed, max_dates=10)
    
    assert result == [date(2026, 9, 17), date(2026, 9, 21)]


def test_compute_missing_truncates_to_max_dates():
    """Truncates result to max_dates when missing count exceeds limit."""
    base_date = date(2026, 9, 1)
    expected = [base_date + timedelta(days=i) for i in range(15)]
    completed = set()

    result = compute_missing(expected=expected, completed=completed, max_dates=5)
    assert len(result) == 5
    assert result == expected[:5]


def test_compute_missing_empty_expected():
    """Handles empty expected list gracefully."""
    result = compute_missing(expected=[], completed={date(2026, 9, 21)}, max_dates=10)
    assert result == []