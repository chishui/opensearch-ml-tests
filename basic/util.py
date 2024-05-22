import time
from basic import client
from decorator import decorator
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@decorator
def wait_task(func, timeout=120, task_type="ML", *args, **kw):
    res = func(*args, **kw)
    if "task_id" not in res:
        return res
    task_id = res["task_id"]
    if is_complete(res):
        return res
    
    uri = None
    if task_type == "ML":
        uri = "/_plugins/_ml/tasks/{}".format(task_id)
    startTime = time.time()
    while True:
        task_res = client.http.get(uri)
        if is_complete(task_res):
            break
        time.sleep(5)
        elapsedTime = time.time() - startTime
        if elapsedTime > timeout:
            break
    return res

def is_complete(res):
    if "status" in res and res["status"] == "COMPLETED":
        return True 
    if "state" in res and res["state"] == "COMPLETED":
        return True
    return False


@decorator
def trace(func, *args, **kw):

    logger.debug(f"before run: {func.__module__}:{func.__qualname__}")
    res = func(*args, **kw)
    logger.debug(f"after run: {func.__module__}:{func.__qualname__}")
    logger.debug(f"result: {res}")
    return res
    
