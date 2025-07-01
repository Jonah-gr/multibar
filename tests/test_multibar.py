import time
import random
import pytest
from multibar import multibar, auto_progress
from multibar.utils import expand_task_params


def test_expand_task_params_static_and_varying():
    params = {"a": 50, "b": [100, 150]}
    expanded = expand_task_params(params)
    assert len(expanded) == 2
    assert all("a" in e and "b" in e for e in expanded)
    assert expanded[0]["a"] == 50


def test_expand_task_params_multiple_varying():
    params = {"a": [10, 20], "b": [30, 40]}
    expanded = expand_task_params(params)
    assert len(expanded) == 4
    combos = [{e["a"], e["b"]} for e in expanded]
    expected = [{10,30}, {10,40}, {20,30}, {20,40}]
    assert all(any(c == exp for c in combos) for exp in expected)


def simple_task(a, b):
    total = random.randint(a, b)
    for i in auto_progress(range(total)):
        time.sleep(0.001)
    return total


@pytest.mark.parametrize("use_tqdm", [True, False])
def test_multibar_runs_with_progress(use_tqdm):
    results = multibar(
        simple_task,
        task_params={"a": [5, 6, 7], "b": 10},
        n_jobs=2,
        desc="Test",
        use_tqdm=use_tqdm
    )
    assert isinstance(results, list)
    assert len(results) == 3
    assert all(isinstance(r, int) for r in results)
