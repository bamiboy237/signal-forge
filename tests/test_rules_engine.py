from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from signal_forge.models import ScoredReading
from signal_forge.rules import DEFAULT_RULES, evaluate, parse_expression, score_rules


def reading(value=80, sensor_id="kiln-1", kind="temperature"):
    return ScoredReading(
        event_id="event-1",
        sensor_id=sensor_id,
        observed_at=datetime(2026, 10, 23, tzinfo=UTC),
        kind=kind,
        value=value,
        unit="C" if kind == "temperature" else "%",
        severity="normal",
        reason="Unscored",
    )


def test_precedence_parentheses_and_boolean_comparisons():
    current = reading()
    for expression, expected in [
        ('kind == "temperature" or value < 0 and value > 100', True),
        ('(kind == "temperature" or value < 0) and value > 100', False),
        ("not not value >= 80", True),
        ("not value >= 80", False),
        ("(value > 70) == (value < 90)", True),
        ("value > -1.25", True),
    ]:
        assert evaluate(parse_expression(expression), current, [current]) is expected


@pytest.mark.parametrize(
    "expression,position",
    [
        ("value >=", 9),
        ("value > 1)", 10),
        ('kind == "unterminated', 9),
        ("value > 1 and", 14),
        ("value + 1", 7),
        ("value > .5", 9),
        ("value > 1e2", 10),
        ('kind == "bad\\n"', 13),
    ],
)
def test_syntax_positions(expression, position):
    with pytest.raises(ValueError, match=rf"at position {position}$"):
        parse_expression(expression)


@pytest.mark.parametrize(
    "expression",
    [
        "value",
        '"text"',
        "Value > 1",
        "temp > 3",
        "max(value) > 1",
        '__import__("os") == 1',
        "sensor_id > 5",
        'value matches "*"',
        "sensor_id matches kind",
        "kind == 3",
        "value > 1 > 0",
        "value and value > 1",
        "not value",
        "avg(value, 0) > 1",
        "avg(value, 21) > 1",
        "avg(value, 2.5) > 1",
        "avg(kind, 2) > 1",
        "delta(value, 2) > 1",
        "",
        " " * 501,
    ],
)
def test_invalid_expressions(expression):
    with pytest.raises(ValueError, match="at position"):
        parse_expression(expression)


@pytest.mark.parametrize(
    "text,pattern,expected",
    [
        ("kiln-1", "kiln-*", True),
        ("kiln-1", "kiln-?", True),
        ("kiln-12", "kiln-?", False),
        ("kiln-1", "kiln", False),
        ("a[b].+", "a[b].+", True),
        ("ab", "a[b]", False),
        ("abc", "a**?c", True),
        ("abc", "*b", False),
        ("", "*", True),
        ("", "?", False),
        ("a\nb", "a?b", True),
    ],
)
def test_glob_only_star_and_question_mark_are_special(text, pattern, expected):
    current = reading(sensor_id=text)
    assert evaluate(parse_expression(f'sensor_id matches "{pattern}"'), current, []) is expected


def test_string_escapes():
    current = reading(sensor_id='a"b\\c')
    assert evaluate(parse_expression(r'sensor_id == "a\"b\\c"'), current, [])


def test_history_windows_filter_sensor_and_kind_without_mutation():
    current = reading(80)
    history = [reading(60), reading(999, "other"), reading(100, kind="humidity"), current]
    original = list(history)
    assert evaluate(parse_expression("avg(value, 2) == 70"), current, history)
    assert evaluate(parse_expression("delta(value) == 20"), current, history)
    assert evaluate(parse_expression("avg(value, 1) == value"), current, history)
    assert history == original


@pytest.mark.parametrize("operator", ["==", "!=", "<", "<=", ">", ">="])
def test_null_comparisons_and_not(operator):
    current = reading()
    for function in ["delta(value)", "avg(value, 2)"]:
        for expression in [f"{function} {operator} 1", f"1 {operator} {function}"]:
            assert not evaluate(parse_expression(expression), current, [current])
            assert evaluate(parse_expression(f"not ({expression})"), current, [current])


def test_first_match_fallback_and_literal_reason_substitution():
    current = reading()
    rules = [
        SimpleNamespace(
            when="value >= 80",
            severity="warning",
            reason="{sensor_id}: {value}{unit}, {value}; {unknown}; {{literal}}",
        ),
        SimpleNamespace(when="value >= 80", severity="critical", reason="Second"),
    ]
    assert score_rules(rules, current, [current]) == (
        "warning",
        "kiln-1: 80C, 80; {unknown}; {{literal}}",
    )
    assert score_rules([], current, [current]) == ("normal", "No rule matched.")
    assert score_rules(rules, reading(10), []) == ("normal", "No rule matched.")


@pytest.mark.parametrize(
    "kind,value,severity,rule_index",
    [
        ("temperature", 59.999, "normal", 2),
        ("temperature", 60, "warning", 1),
        ("temperature", 79.999, "warning", 1),
        ("temperature", 80, "critical", 0),
        ("humidity", 19.999, "warning", 3),
        ("humidity", 20, "normal", 5),
        ("humidity", 80, "normal", 5),
        ("humidity", 80.001, "warning", 4),
    ],
)
def test_defaults_at_thresholds(kind, value, severity, rule_index):
    rules = [SimpleNamespace(**rule) for rule in DEFAULT_RULES]
    current = reading(value, kind=kind)
    assert score_rules(rules, current, [current]) == (
        severity,
        DEFAULT_RULES[rule_index]["reason"],
    )
