# -*- coding=utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos import CosServiceError
from qcloud_cos import CosClientError

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
def nuke():
    secret_id = os.environ['MAIN_SECRET_ID']
    secret_key =  os.environ['MAIN_SECRET_KEY']
    appid = os.environ['MAIN_APPID']
    region =  os.environ['COS_REGION']
    token = None
    config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=None)  # 获取配置对象
    client = CosS3Client(config)
    os.system("coscmd config -a %s -s %s -b %s -r %s" % (secret_id, secret_key, "test-" + appid, region))
    rt = client.list_buckets()
    print rt
    if rt['Buckets'] is not None:
        for dt in rt['Buckets']['Bucket']:
            if dt['Location'] == region:
                os.system("coscmd -b {bucket} -r {region} deletebucket -f".format(bucket=dt['Name'], region=dt['Location']))
nuke()
