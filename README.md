# multibar

📊 **multibar** displays progress bars for each parallel task across multiple environments making it easy to track the progress of all your processes at once.

It makes it easy to:

- Run many tasks in parallel (via `joblib`) while seeing a combined overall progress bar.
- See individual progress bars for each worker (threads or processes).
- Supports `tqdm`-based bars or simple text-based bars (great for minimal output or non-TTY environments like Databricks Workflow Jobs).
- Seamlessly works in notebooks and consoles.

---

## Features

✅ Parallel execution using `joblib`.  
✅ Auto-progress tracking inside tasks via `auto_progress`.  
✅ Combined overall progress + individual worker progress.  
✅ ETA + speed (it/s) estimates.  
✅ Works in Jupyter, Databricks, plain terminal.  
✅ Toggle `tqdm` or simple bars via `use_tqdm`.  

---

## Installation

```bash
pip install git+https://github.com/Jonah-gr/multibar.git
```

## Quickstart

```python
from multibar import multibar, auto_progress
import time
import random

def test_function(a, b):
    total_steps = random.randint(a, b)
    for i in auto_progress(range(total_steps)):
        time.sleep(0.1)
    return total_steps

results = multibar(
    task=test_function,
    task_params={"a": [10, 20, 30], "b": [50, 60]},
    n_jobs=3,
    desc="Running tasks",
    use_tqdm=True
)

print("Results:", results)
```

## 🎥 Demo

### 🖥️ Terminal
[![Terminal Demo](https://i.gyazo.com/546ef3ab06362a8c13b90448822cac10.gif)](https://gyazo.com/546ef3ab06362a8c13b90448822cac10)

### 📝 Jupyter
[![Jupyter Demo](https://i.gyazo.com/2278e2748875b464274a5f4aebcc9e38.gif)](https://gyazo.com/2278e2748875b464274a5f4aebcc9e38)

## License
MIT License.
See [LICENSE](LICENSE) for details.