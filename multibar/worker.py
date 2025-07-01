import threading

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
    
