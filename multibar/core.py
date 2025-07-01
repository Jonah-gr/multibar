import time
import random
import threading
from joblib import Parallel, delayed
from multiprocessing import Manager
from .display import update_display
from .utils import expand_task_params
from .worker import set_worker_context, get_worker_context

def auto_progress(iterable):
    """
    A context manager to automatically update the progress of a task
    given an iterable.

    Args:
        iterable: An iterable to iterate over.

    Yields:
        item: The next item in the iterable.

    This context manager retrieves the worker context from the thread-local
    storage and updates the progress of the task by storing the current
    and total number of steps in the worker_statuses_dict. If the iterable
    is a sequence (i.e. has a __len__ method), the total number of steps is
    set to the length of the sequence. If the iterable is not a sequence,
    the total number of steps is set to None.

    When the iterable is exhausted, the progress is reset to 0.

    This context manager is intended to be used in tasks to automatically
    update the progress of the task. It is typically used in combination
    with the process_task function.
    """
    ctx = get_worker_context()
    worker_id = ctx['worker_id']
    worker_statuses_dict = ctx['worker_statuses_dict']

    total_steps = len(iterable) if hasattr(iterable, "__len__") else None

    for i, item in enumerate(iterable):
        if total_steps:
            worker_statuses_dict[worker_id] = (i + 1, total_steps)
        else:
            worker_statuses_dict[worker_id] = (i + 1, None)
        yield item

    # Final cleanup
    if total_steps:
        worker_statuses_dict[worker_id] = (total_steps, total_steps)


def process_task(task, task_params, worker_id, worker_statuses_dict, completed_tasks_counter, lock):
    """
    Process a task and update progress in the worker_statuses_dict.

    This function should be called from within a multiprocessing.Process.

    Args:
        task: A callable to call with the given task_params.
        task_params: A dictionary of keyword arguments to pass to the task.
        worker_id: The worker id assigned to this process.
        worker_statuses_dict: A dictionary to track the status of each worker.
        completed_tasks_counter: A multiprocessing.Value to increment when a task is completed.
        lock: A multiprocessing.Lock to protect access to the completed_tasks_counter.

    Returns:
        The result of calling the task with the given task_params.
    """
    set_worker_context(worker_id, worker_statuses_dict)
    results = task(**task_params)
    with lock:
        completed_tasks_counter.value += 1
        worker_statuses_dict[worker_id] = 0  # Reset to 0 when idle
    return results


def test_function(a, b):
    total_steps = random.randint(a, b)
    for i in auto_progress(list(range(total_steps))):
        time.sleep(0.2) # random.uniform(0.03, 0.10)
    return total_steps
 

def multibar(task, task_params, n_jobs=2, backend="loky", desc="Overall Progress", use_tqdm=True):
    """
    Execute a task with multiple parameters in parallel using joblib's Parallel.

    Args:
        task: A callable to call with the given task_params.
        task_params: A dictionary of keyword arguments to pass to the task.
        n_jobs: The number of jobs to run in parallel. Defaults to 2.
        backend: The backend to use with joblib. Defaults to "loky".
        desc: A description to use for the tqdm progress bar. Defaults to "Overall Progress".

    Returns:
        A list of results from calling the task with the given task_params.
    """
    expanded_params = expand_task_params(task_params)
    total_tasks = len(expanded_params)

    manager = Manager()
    completed_tasks = manager.Value('i', 0)
    lock = manager.Lock()
    stop_event = manager.Event()

    worker_statuses = manager.dict({i: 0 for i in range(n_jobs)})

    jobs = [
        delayed(process_task)(task, expanded_params[i], i % n_jobs, worker_statuses, completed_tasks, lock)
        for i in range(total_tasks)
    ]

    display_thread = threading.Thread(
        target=update_display,
        args=(stop_event, completed_tasks, total_tasks, worker_statuses, n_jobs, desc, use_tqdm),
        daemon=True
    )

    print(f"Starting job with {total_tasks} tasks and {n_jobs} parallel workers.")
    time.sleep(1)

    try:
        display_thread.start()
        results = Parallel(n_jobs=n_jobs, backend=backend)(jobs)
    finally:
        time.sleep(0.3)
        stop_event.set()
        display_thread.join()
        manager.shutdown()

    return results



if __name__ == "__main__":
    results = multibar(test_function, task_params={"a": [50, 50, 80, 100]*2, "b": 150},  n_jobs=3, desc="Test Progress")
    print(results)
