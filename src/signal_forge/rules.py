"""Typed rule expressions over normalized readings and caller-ordered history."""

from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal, Protocol

from .models import ScoredReading, Severity

DEFAULT_RULES = [
    {
        "id": "temp-critical",
        "when": 'kind == "temperature" and value >= 80',
        "severity": "critical",
        "reason": "Temperature is at or above 80 C.",
    },
    {
        "id": "temp-warning",
        "when": 'kind == "temperature" and value >= 60',
        "severity": "warning",
        "reason": "Temperature is at or above 60 C and below 80 C.",
    },
    {
        "id": "temp-normal",
        "when": 'kind == "temperature"',
        "severity": "normal",
        "reason": "Temperature is below 60 C.",
    },
    {
        "id": "humidity-low",
        "when": 'kind == "humidity" and value < 20',
        "severity": "warning",
        "reason": "Humidity is below 20 %.",
    },
    {
        "id": "humidity-high",
        "when": 'kind == "humidity" and value > 80',
        "severity": "warning",
        "reason": "Humidity is above 80 %.",
    },
    {
        "id": "humidity-normal",
        "when": 'kind == "humidity"',
        "severity": "normal",
        "reason": "Humidity is between 20 % and 80 %, inclusive.",
    },
]

_Type = Literal["string", "number", "boolean"]
_Value = str | int | float | bool | None
_FIELDS: dict[str, _Type] = {
    "kind": "string",
    "sensor_id": "string",
    "unit": "string",
    "value": "number",
}
_COMPARISONS = {"==", "!=", "<", "<=", ">", ">="}


@dataclass(frozen=True)
class Node:
    """Immutable expression tree. Obtain validated trees with parse_expression."""

    op: str
    position: int
    value: str | int | float | None = None
    children: tuple[Node, ...] = ()


@dataclass(frozen=True)
class _Token:
    kind: str
    text: str
    position: int


def _error(message: str, position: int) -> ValueError:
    return ValueError(f"{message} at position {position}")


def _tokenize(text: str) -> list[_Token]:
    tokens = []
    index = 0
    while index < len(text):
        char = text[index]
        if char.isspace():
            index += 1
            continue
        start = index
        if char == '"':
            index += 1
            chars = []
            while index < len(text) and text[index] != '"':
                if text[index] == "\\":
                    index += 1
                    if index == len(text) or text[index] not in {'"', "\\"}:
                        raise _error("Invalid string escape", index)
                chars.append(text[index])
                index += 1
            if index == len(text):
                raise _error("Unterminated string", start + 1)
            index += 1
            tokens.append(_Token("string", "".join(chars), start + 1))
            continue
        if char in "0123456789-":
            if char == "-":
                index += 1
            digits_start = index
            while index < len(text) and text[index] in "0123456789":
                index += 1
            if index == digits_start:
                raise _error("Expected digits", start + 1)
            if index < len(text) and text[index] == ".":
                index += 1
                digits_start = index
                while index < len(text) and text[index] in "0123456789":
                    index += 1
                if index == digits_start:
                    raise _error("Expected digits after decimal point", index + 1)
            tokens.append(_Token("number", text[start:index], start + 1))
            continue
        if char.isascii() and (char.isalpha() or char == "_"):
            index += 1
            while (
                index < len(text)
                and text[index].isascii()
                and (text[index].isalnum() or text[index] == "_")
            ):
                index += 1
            tokens.append(_Token("name", text[start:index], start + 1))
            continue
        pair = text[index : index + 2]
        if pair in {"==", "!=", "<=", ">="}:
            tokens.append(_Token(pair, pair, start + 1))
            index += 2
        elif char in "<>(),":
            tokens.append(_Token(char, char, start + 1))
            index += 1
        else:
            raise _error(f"Unexpected character {char!r}", start + 1)
    tokens.append(_Token("end", "", len(text) + 1))
    return tokens


class _Parser:
    def __init__(self, tokens: list[_Token]):
        self.tokens = tokens
        self.index = 0

    @property
    def current(self) -> _Token:
        return self.tokens[self.index]

    def take(self) -> _Token:
        token = self.current
        self.index += 1
        return token

    def accept(self, text: str) -> _Token | None:
        token = self.current
        if token.kind == text or (token.kind == "name" and token.text == text):
            return self.take()
        return None

    def expect(self, text: str) -> _Token:
        token = self.accept(text)
        if token is None:
            raise _error(f"Expected {text!r}", self.current.position)
        return token

    def parse_or(self) -> Node:
        node = self.parse_and()
        while token := self.accept("or"):
            node = Node("or", token.position, children=(node, self.parse_and()))
        return node

    def parse_and(self) -> Node:
        node = self.parse_not()
        while token := self.accept("and"):
            node = Node("and", token.position, children=(node, self.parse_not()))
        return node

    def parse_not(self) -> Node:
        if token := self.accept("not"):
            return Node("not", token.position, children=(self.parse_not(),))
        return self.parse_comparison()

    def parse_comparison(self) -> Node:
        node = self.parse_term()
        if self.current.kind in _COMPARISONS:
            token = self.take()
            return Node(token.kind, token.position, children=(node, self.parse_term()))
        if token := self.accept("matches"):
            pattern = self.expect("string")
            return Node(
                "matches",
                token.position,
                children=(node, Node("string", pattern.position, pattern.text)),
            )
        return node

    def parse_term(self) -> Node:
        token = self.current
        if self.accept("("):
            node = self.parse_or()
            self.expect(")")
            return node
        if self.accept("number"):
            value = float(token.text) if "." in token.text else int(token.text)
            return Node("number", token.position, value)
        if self.accept("string"):
            return Node("string", token.position, token.text)
        if self.accept("name"):
            if token.text in {"avg", "delta"}:
                self.expect("(")
                field = self.expect("name")
                if field.text != "value":
                    raise _error("Expected value", field.position)
                window = None
                if token.text == "avg":
                    self.expect(",")
                    size = self.expect("number")
                    if "." in size.text or not 1 <= int(size.text) <= 20:
                        raise _error("avg window must be an integer from 1 to 20", size.position)
                    window = int(size.text)
                self.expect(")")
                return Node(token.text, token.position, window)
            if token.text in _FIELDS:
                return Node("field", token.position, token.text)
            raise _error(f"Unknown field or function {token.text!r}", token.position)
        raise _error("Expected a term", token.position)


def _check_type(node: Node) -> _Type:
    if node.op in {"number", "avg", "delta"}:
        return "number"
    if node.op == "string":
        return "string"
    if node.op == "field":
        return _FIELDS[node.value]
    types = tuple(_check_type(child) for child in node.children)
    if node.op in {"and", "or", "not"}:
        valid = all(kind == "boolean" for kind in types)
    elif node.op in {"==", "!="}:
        valid = types[0] == types[1]
    elif node.op == "matches":
        valid = types == ("string", "string") and node.children[1].op == "string"
    else:
        valid = types == ("number", "number")
    if not valid:
        raise _error(f"Invalid operand types for {node.op!r}", node.position)
    return "boolean"


@lru_cache(maxsize=256)
def parse_expression(text: str) -> Node:
    """Parse and type-check a 1–500 character boolean expression.

    Invalid expressions raise ValueError with a 1-based character position.
    Trees are immutable and cached; callers may safely reuse them.
    """
    if not isinstance(text, str) or not 1 <= len(text) <= 500:
        raise _error("Expression must contain 1 to 500 characters", 1)
    parser = _Parser(_tokenize(text))
    try:
        node = parser.parse_or()
        parser.expect("end")
        if _check_type(node) != "boolean":
            raise _error("Expression must be boolean", node.position)
    except RecursionError as exc:
        raise _error("Expression nesting is too deep", parser.current.position) from exc
    return node


def _matches(text: str, pattern: str) -> bool:
    # Track reachable prefix lengths. Only * and ? have special meaning.
    previous = [True] + [False] * len(text)
    for char in pattern:
        current = [char == "*" and previous[0]] + [False] * len(text)
        for index, actual in enumerate(text, 1):
            if char == "*":
                current[index] = previous[index] or current[index - 1]
            else:
                current[index] = previous[index - 1] and (char == "?" or char == actual)
        previous = current
    return previous[-1]


def _evaluate(node: Node, reading: ScoredReading, history: list[ScoredReading]) -> _Value:
    op = node.op
    if op in {"number", "string"}:
        return node.value
    if op == "field":
        return getattr(reading, node.value)
    if op in {"avg", "delta"}:
        values = [
            item.value
            for item in history
            if item.sensor_id == reading.sensor_id and item.kind == reading.kind
        ]
        if op == "delta":
            return reading.value - values[-2] if len(values) >= 2 else None
        window = node.value
        return sum(values[-window:]) / window if len(values) >= window else None
    left = _evaluate(node.children[0], reading, history)
    if op == "not":
        return not left
    if op == "and":
        return bool(left) and bool(_evaluate(node.children[1], reading, history))
    if op == "or":
        return bool(left) or bool(_evaluate(node.children[1], reading, history))
    right = _evaluate(node.children[1], reading, history)
    if left is None or right is None:
        return False
    if op == "matches":
        return _matches(left, right)
    if op == "==":
        return left == right
    if op == "!=":
        return left != right
    if op == "<":
        return left < right
    if op == "<=":
        return left <= right
    if op == ">":
        return left > right
    if op == ">=":
        return left >= right
    raise ValueError(f"Unknown expression operation {op!r}")


def evaluate(node: Node, reading: ScoredReading, history: list[ScoredReading]) -> bool:
    """Evaluate a parsed tree without mutating readings or history.

    History contains accepted, normalized readings in observation-time/batch-position
    order, ending with the current reading for its sensor ID and kind. The caller
    supplies this order; this function filters history but does not sort or append.
    """
    return bool(_evaluate(node, reading, history))


class Rule(Protocol):
    @property
    def when(self) -> str: ...

    @property
    def severity(self) -> Severity: ...

    @property
    def reason(self) -> str: ...


def score_rules(
    rules: Iterable[Rule], reading: ScoredReading, history: list[ScoredReading]
) -> tuple[Severity, str]:
    """Return the first match, or normal / No rule matched.

    Rules expose when, severity, and reason attributes. Rule-set and reason-template
    validation belong to the caller. Substitution is literal, not Python formatting.
    """
    for rule in rules:
        if evaluate(parse_expression(rule.when), reading, history):
            reason = rule.reason
            for placeholder, value in (
                ("{sensor_id}", reading.sensor_id),
                ("{unit}", reading.unit),
                ("{value}", format(reading.value, "g")),
            ):
                reason = reason.replace(placeholder, value)
            return rule.severity, reason
    return "normal", "No rule matched."
