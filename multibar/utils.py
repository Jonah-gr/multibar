from itertools import product


def format_time(seconds):
    """Formats seconds into M:SS or H:MM:SS as needed."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    else:
        h, rem = divmod(int(seconds), 3600)
        m, s = divmod(rem, 60)
        return f"{h}h {m}m {s}s"
    

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