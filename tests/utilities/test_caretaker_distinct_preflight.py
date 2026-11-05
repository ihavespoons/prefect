from prefect.utilities.collections import distinct


def test_distinct_calls_key_once_per_consumed_item():
    items = [1, 2, 3, 3, 4]
    calls = []

    def key(item):
        calls.append(item)
        return item

    result = list(distinct(items, key=key))

    assert result == [1, 2, 3, 4]
    assert calls == [1, 2, 3, 3, 4]


def test_distinct_preserves_first_dictionary_object_for_custom_key():
    records = [
        {"id": 1, "value": "first"},
        {"id": 2, "value": "second"},
        {"id": 1, "value": "duplicate"},
    ]
    calls = []

    def key(record):
        calls.append(record["id"])
        return record["id"]

    result = list(distinct(records, key=key))

    assert result == [records[0], records[1]]
    assert result[0] is records[0]
    assert result[1] is records[1]
    assert calls == [1, 2, 1]


def test_distinct_one_shot_iterator_stops_after_two_yields():
    records = [1, 1, 2, 3]
    consumed = []
    calls = []

    def source():
        for record in records:
            consumed.append(record)
            yield record

    def key(record):
        calls.append(record)
        return record

    iterator = distinct(source(), key=key)

    assert next(iterator) == 1
    assert next(iterator) == 2

    assert consumed == [1, 1, 2]
    assert calls == [1, 1, 2]


def test_distinct_default_and_empty_inputs_are_unchanged():
    assert list(distinct([1, 2, 1, 3])) == [1, 2, 3]
    assert list(distinct([])) == []
