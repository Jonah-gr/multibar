import pytest
from multibar.utils import expand_task_params


def test_expand_no_varying():
    params = {"x": 10, "y": 20}
    expanded = expand_task_params(params)
    assert len(expanded) == 1
    assert expanded[0] == {"x": 10, "y": 20}


def test_expand_one_varying_list():
    params = {"x": 10, "y": [1, 2, 3]}
    expanded = expand_task_params(params)
    assert len(expanded) == 3
    for e, yval in zip(expanded, [1, 2, 3]):
        assert e == {"x": 10, "y": yval}


def test_expand_one_varying_tuple():
    params = {"a": (5, 6, 7), "b": 100}
    expanded = expand_task_params(params)
    assert len(expanded) == 3
    assert all(e["b"] == 100 for e in expanded)


def test_expand_two_varying():
    params = {"x": [1, 2], "y": (3, 4)}
    expanded = expand_task_params(params)
    assert len(expanded) == 4
    expected = [
        {"x": 1, "y": 3}, {"x": 1, "y": 4},
        {"x": 2, "y": 3}, {"x": 2, "y": 4},
    ]
    assert all(e in expected for e in expanded)


def test_expand_three_varying():
    params = {"a": [1, 2], "b": [3, 4], "c": [5, 6]}
    expanded = expand_task_params(params)
    assert len(expanded) == 8  # 2 * 2 * 2
    for combo in expanded:
        assert set(combo.keys()) == {"a", "b", "c"}


def test_expand_with_empty_list():
    params = {"x": [], "y": 10}
    expanded = expand_task_params(params)
    # Cartesian product on empty list yields zero combinations
    assert expanded == []


def test_expand_mixed_types():
    params = {"path": "/data", "ids": [1, 2, 3], "flag": True}
    expanded = expand_task_params(params)
    assert len(expanded) == 3
    for e, idval in zip(expanded, [1, 2, 3]):
        assert e == {"path": "/data", "ids": idval, "flag": True}


def test_expand_large_cartesian_product():
    params = {"x": [0, 1, 2, 3, 4], "y":  [0, 1, 2, 3, 4]}
    expanded = expand_task_params(params)
    assert len(expanded) == 25  # 5 x 5


@pytest.mark.parametrize("bad_value", [
    {"x": None, "y": 5},
    {"x": "string", "y": 1},
    {"x": 3.14, "y": True}
])
def test_expand_non_iterables_stay_static(bad_value):
    # non-lists / non-tuples should not expand
    expanded = expand_task_params(bad_value)
    assert isinstance(expanded, list)
    assert len(expanded) == 1
    assert expanded[0] == bad_value
