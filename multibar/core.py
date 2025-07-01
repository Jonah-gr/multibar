import os
import time
import random
import threading
from itertools import product
from joblib import Parallel, delayed
from multiprocessing import Manager

if "DATABRICKS_RUNTIME_VERSION" in os.environ: # and "DATABRICKS_JOB_RUN_ID" in os.environ:
    import sys
    from IPython.display import clear_output
    _running_in_databricks_job = True
    print("Recognized databricks. Just using simple progress bars now.")
else:
    from tqdm.auto import tqdm
    _running_in_databricks_job = False


_worker_context = threading.local()



def set_worker_context(worker_id, worker_statuses_dict):
    """
    Sets the worker context for the current thread.

    Args:
        worker_id: An identifier for the worker.
        worker_statuses_dict: A dictionary to track the status of each worker.
    """
    _worker_context.worker_id = worker_id
    _worker_context.worker_statuses_dict = worker_statuses_dict


def get_worker_context():
    """
    Returns the worker context for the current thread.

    Returns a dictionary with two possible keys:

        worker_id: an identifier for the worker
        worker_statuses_dict: a dictionary to track the status of each worker

    The values of worker_id and worker_statuses_dict are None if the worker context
    has not been set before.
    """
    return {
        "worker_id": getattr(_worker_context, "worker_id", None),
        "worker_statuses_dict": getattr(_worker_context, "worker_statuses_dict", None)
    }


def expand_task_params(task_params):
    """
    Expands task parameters by generating combinations of varying parameters.

    Args:
        task_params (dict): A dictionary of task parameters where values may be 
                            lists or tuples indicating varying parameters.

    Returns:
        list: A list of dictionaries, each representing a unique combination of 
              task parameters. If no parameters vary, returns a list with a single 
              dictionary identical to task_params. If only one parameter varies, 
              returns a list with each value of that parameter substituted.
              If multiple parameters vary, returns all possible combinations using 
              a cartesian product of the varying parameters.
    """
    varying_keys = [k for k, v in task_params.items() if isinstance(v, (list, tuple))]
    if len(varying_keys) == 0:
        # nothing varies, single task
        return [task_params]
    elif len(varying_keys) == 1:
        key = varying_keys[0]
        values = task_params[key]
        return [
            {**{k: v for k, v in task_params.items() if k != key}, key: value}
            for value in values
        ]
    else:
        # multiple varying parameters => do a cartesian product
        keys = varying_keys
        values_lists = [task_params[k] for k in keys]
        static_params = {k: v for k, v in task_params.items() if k not in keys}
        return [
            {**static_params, **dict(zip(keys, combo))}
            for combo in product(*values_lists)
        ]


def format_simple_progress_bar(current, total, width=30, desc=""):
    frac = current / total if total else 0
    filled = int(width * frac)
    bar = "#" * filled + "-" * (width - filled)
    percent = frac * 100
    return f"{desc} [{bar}] {current}/{total} ({percent:.1f}%)"


def update_simple_progress_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc):
    last_completed = 0

    while not stop_event.is_set():
        current_completed = completed_tasks.value
        overall_bar = format_simple_progress_bar(current_completed, total_tasks, desc=desc)

        lines = [overall_bar]
        for worker_id in range(num_workers):
            status = worker_statuses_dict.get(worker_id, (0, 0))
            if isinstance(status, tuple):
                current, total = status
                line = format_simple_progress_bar(current, total or 1, desc=f"Worker {worker_id:03d}")
                lines.append(line)

        output = "\n".join(lines)

        clear_output(wait=True)
        print(output)
        if current_completed == total_tasks:
            break
        time.sleep(0.2)
    print()


def update_tqdm_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc):
    overall_pbar = tqdm(total=total_tasks, desc=desc, position=0)
    worker_bars = {}

    last_completed = 0

    while not stop_event.is_set():
        current_completed = completed_tasks.value
        overall_pbar.update(current_completed - last_completed)
            
        last_completed = current_completed

        for worker_id in range(num_workers):
            status = worker_statuses_dict.get(worker_id, None)
            if status is None:
                continue

            if isinstance(status, tuple):
                current, total = status
                if worker_id not in worker_bars:
                    worker_bars[worker_id] = tqdm(total=total, desc=f"Worker {str(worker_id).zfill(3)}", position=worker_id + 1, leave=False)
                bar = worker_bars[worker_id]
                if total != bar.total:
                    bar.reset(total=total)
                bar.n = current
                bar.refresh()

        if current_completed == total_tasks:
            break

        time.sleep(0.2)

    for bar in worker_bars.values():
        bar.close()
    overall_pbar.close()


def update_display(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc):
    """
    Displays a progress bar for the overall progress of all tasks and 
    progress bars for each worker.

    Args:
        stop_event (threading.Event): An event that is set when all tasks have been completed.
        completed_tasks (int): The number of tasks that have been completed.
        total_tasks (int): The total number of tasks.
        worker_statuses_dict (dict): A dictionary where the keys are worker ids and the values
            are tuples of (current, total) representing the current and total number of tasks 
            each worker has completed.
        num_workers (int): The number of workers.
        desc (str): The description of the overall progress bar.

    Note: This function should be called in a separate thread because it blocks until all tasks
    have been completed.
    """
    if _running_in_databricks_job:
        update_simple_progress_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc)
    else:
        update_tqdm_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc)


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
 

def multibar(task, task_params, n_jobs=2, backend="multiprocessing", desc="Overall Progress"):
    """
    Execute a task with multiple parameters in parallel using joblib's Parallel.

    Args:
        task: A callable to call with the given task_params.
        task_params: A dictionary of keyword arguments to pass to the task.
        n_jobs: The number of jobs to run in parallel. Defaults to 2.
        backend: The backend to use with joblib. Defaults to "multiprocessing".
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
        args=(stop_event, completed_tasks, total_tasks, worker_statuses, n_jobs, desc),
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
