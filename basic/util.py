import os
import time
from basic import client
from decorator import decorator
import logging

DEBUG = os.environ.get("DEBUG", "0")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO if DEBUG == "0" else logging.DEBUG)


@decorator
def wait_task(func, callback=None, timeout=120, task_type="ML", *args, **kw):
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
            if callback is not None:
                callback_obj = args[0]
                actual_callback = getattr(callback_obj, callback.__name__, None)
                if actual_callback is not None:
                    actual_callback(task_res)
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
    

def parser(obj, keys):
    def get(obj, key):
        if isinstance(obj, list):
            ret = []
            for o in obj:
                ret.append(get(o, key))
            return ret
        else:
            if key == "*":
                return list(obj.values())
            if key not in obj:
                raise KeyError
            return obj[key]

    for key in keys:
        obj = get(obj, key)

    # flatten
    return flatten(obj)

def flatten(obj):
    if isinstance(obj, list):
        ret = []
        for i in obj:
            r = flatten(i)
            if isinstance(r, list):
                ret = ret + r
            else:
                ret.append(flatten(i))
        return ret
    else:
        return obj


