from prefect.utilities.collections import distinct


def test_key_is_evaluated_once_per_visited_item():
    calls = []

    def key(item):
        calls.append(item)
        return item.lower()

    assert list(distinct(["A", "b", "a", "C", "B"], key=key)) == ["A", "b", "C"]
    assert calls == ["A", "b", "a", "C", "B"]


def test_distinct_keeps_input_and_key_evaluation_lazy():
    visited = []
    calls = []

    def items():
        for item in [1, 1, 2]:
            visited.append(item)
            yield item

    def key(item):
        calls.append(item)
        return item

    result = distinct(items(), key=key)
    assert visited == calls == []
    assert next(result) == 1
    assert visited == calls == [1]
    assert list(result) == [2]
    assert visited == calls == [1, 1, 2]


def test_default_key_preserves_first_seen_order():
    assert list(distinct([3, 1, 3, 2, 1])) == [3, 1, 2]


def test_empty_input_does_not_call_key():
    def key(item):
        raise AssertionError("empty input must not call the key")

    assert list(distinct([], key=key)) == []


def test_key_supports_unhashable_items():
    items = [{"id": 2, "value": "first"}, {"id": 2, "value": "duplicate"},
             {"id": 3, "value": "third"}]
    assert list(distinct(items, key=lambda item: item["id"])) == [items[0], items[2]]
