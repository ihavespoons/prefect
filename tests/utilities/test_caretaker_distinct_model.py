from prefect.utilities.collections import distinct


def test_distinct_key_called_once_per_consumed_item():
    calls = []

    def key(record):
        calls.append(record["id"])
        return record["id"]

    records = [
        {"id": 1, "value": "a"},
        {"id": 1, "value": "b"},
        {"id": 2, "value": "c"},
        {"id": 3, "value": "d"},
    ]

    result = distinct(records, key=key)

    first = next(result)
    assert first is records[0]
    assert calls == [1]

    second = next(result)
    assert second is records[2]
    assert calls == [1, 1, 2]

    third = next(result)
    assert third is records[3]
    assert calls == [1, 1, 2, 3]


def test_distinct_one_shot_iterator_stops_after_two_distinct_yields():
    records = [
        {"id": 1, "value": "a"},
        {"id": 1, "value": "b"},
        {"id": 2, "value": "c"},
        {"id": 3, "value": "d"},
    ]
    consumed = []
    calls = []

    def source():
        for record in records:
            consumed.append(record)
            yield record

    def key(record):
        calls.append(record["id"])
        return record["id"]

    result = distinct(source(), key=key)

    first = next(result)
    second = next(result)

    assert first is records[0]
    assert second is records[2]
    assert consumed == [records[0], records[1], records[2]]
    assert calls == [1, 1, 2]


def test_distinct_default_and_empty():
    assert list(distinct([])) == []
    assert list(distinct([1, 2, 1, 3, 2])) == [1, 2, 3]
