from multibar.worker import set_worker_context, get_worker_context

def test_worker_context_storage():
    set_worker_context(42, {"status": "ok"})
    ctx = get_worker_context()
    assert ctx["worker_id"] == 42
    assert ctx["worker_statuses_dict"] == {"status": "ok"}
