# -*- coding=utf-8
from . import _make_request
import time

TOTAL_WAIT_TIME_IN_S = 60  # 最大等待时间设置为1min
SINGLE_WAIT_TIME_IN_S = 2
SYNC_WAIT_TIME_IN_S = 5  # CAM ACL 同步时间为5s

def wait_for_acl_valid(status, bucket, key=None):
    sleep_times = 0
    while(sleep_times < TOTAL_WAIT_TIME_IN_S):
        res = _make_request('HEAD', bucket, key)
        if res.status == status:
            break
        time.sleep(SINGLE_WAIT_TIME_IN_S)
        sleep_times += SINGLE_WAIT_TIME_IN_S
    time.sleep(SYNC_WAIT_TIME_IN_S)  # 等待CAM的ACL缓存同步
    return None
