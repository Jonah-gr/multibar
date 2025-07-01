import time
from IPython.display import clear_output
from tqdm.auto import tqdm
from .utils import format_time



def format_simple_progress_bar(current, total, width=30, desc=""):
    """
    Formats a simple text-based progress bar.

    Args:
        current (int): The current progress count.
        total (int): The total count for completion.
        width (int, optional): The width of the progress bar. Defaults to 30.
        desc (str, optional): A description to prefix the progress bar with. Defaults to "".

    Returns:
        str: A formatted progress bar string including the description, bar, current/total count, 
             and percentage completion.
    """
    frac = current / total if total else 0
    filled = int(width * frac)
    bar = "#" * filled + "-" * (width - filled)
    percent = frac * 100
    return f"{desc} [{bar}] {current}/{total} ({percent:.1f}%)"


def update_simple_progress_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc):
    """
    Update simple text-based progress bars for overall progress and individual worker progress.

    Args:
        stop_event: An event to signal when to stop updating the progress bars.
        completed_tasks: A multiprocessing.Value representing the number of completed tasks.
        total_tasks: The total number of tasks to be completed.
        worker_statuses_dict: A dictionary where each key is a worker ID and the value is a tuple
            (current, total) representing the current progress and total tasks for each worker.
        num_workers: The number of workers.
        desc: A description for the overall progress bar.
    """
    overall_start_time = time.time()
    worker_start_times = {i: None for i in range(num_workers)}

    while not stop_event.is_set():
        current_completed = completed_tasks.value

        # Overall progress bar (no ETA here to keep it clean)
        overall_bar = format_simple_progress_bar(current_completed, total_tasks, desc=desc)
        lines = [overall_bar]

        # Worker bars with it/s and ETA
        for worker_id in range(num_workers):
            status = worker_statuses_dict.get(worker_id, (0, 0))
            if isinstance(status, tuple):
                current, total = status

                # Mark start time for this worker
                if worker_start_times[worker_id] is None and current > 0:
                    worker_start_times[worker_id] = time.time()

                elapsed = (time.time() - worker_start_times[worker_id]) if worker_start_times[worker_id] else 0
                speed = (current / elapsed) if elapsed > 0 else 0
                remaining = (total - current)
                eta = (remaining / speed) if speed > 0 else 0

                stats = f"{speed:.2f} it/s | ETA: {format_time(eta)}" if total > 0 else ""
                line = format_simple_progress_bar(current, total or 1, desc=f"Worker {worker_id:03d}")
                line += f" | {stats}"
                lines.append(line)

        # Print
        clear_output(wait=True)
        print("\n".join(lines))

        if current_completed == total_tasks:
            break
        time.sleep(0.2)

    print()


def update_tqdm_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc):
    """
    Update tqdm progress bars for overall progress and individual worker progress.

    Args:
        stop_event: An event to signal when to stop updating the progress bars.
        completed_tasks: A multiprocessing.Value representing the number of completed tasks.
        total_tasks: The total number of tasks to be completed.
        worker_statuses_dict: A dictionary where each key is a worker ID and the value is a tuple
            (current, total) representing the current progress and total tasks for each worker.
        num_workers: The number of workers.
        desc: A description for the overall progress bar.
    """
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


def update_display(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc, use_tqdm):
    """
    Update the display for a task based on the given parameters.

    Args:
        stop_event: An event to check if the task should be stopped.
        completed_tasks: The number of tasks that have been completed.
        total_tasks: The total number of tasks that need to be completed.
        worker_statuses_dict: A dictionary of statuses for each worker. The value
            for each key is a tuple of (current, total) tasks completed by the
            worker.
        num_workers: The number of workers.
        desc: A description to display for the task.
        use_tqdm: A boolean indicating whether to use tqdm for the progress bar.
    """
    if use_tqdm:
        update_tqdm_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc)
    else:
        update_simple_progress_bars(stop_event, completed_tasks, total_tasks, worker_statuses_dict, num_workers, desc)        

