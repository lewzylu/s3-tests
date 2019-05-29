#!/usr/bin/env python

import base64
import binascii
import hashlib
import hmac
import json
import os
import random
import threading
import time
import urllib
import urllib2
import urlparse

def get_auth(uri, method, headers, secret_id, secret_key):
    uri_list = uri.split("?")
    uri = uri_list[0]
    if len(uri_list) > 1:
        result = urlparse.urlparse('?'+uri_list[1])[4].split('=')
        if len(result)%2 != 0:
            result.append('')
        params = { result[idx-1]:result[idx] for idx,_ in enumerate(result) if (idx+1)%2==0 }
    else:
        params = {}

    format_str = '%s\n'%method.lower() + \
            '%s\n'%(uri) + \
            '&'.join([ urllib.urlencode({k.lower():params[k]}) for k in sorted(params.keys()) ]) + '\n' + \
            '&'.join(['%s=%s'%(k.lower(), headers[k]) for k in sorted(headers.keys()) ]) + '\n'

    start_sign_time = int(time.time())
    sign_time = '%s;%s'%(start_sign_time-50, start_sign_time+1000000)
    sha1 = hashlib.sha1()
    sha1.update(format_str)
    str_to_sign = 'sha1\n' + '%s\n'%sign_time + sha1.hexdigest() + '\n'

    hashed = hmac.new(secret_key, '%s'%sign_time, hashlib.sha1)
    sign_key = hashed.hexdigest()
    hasded1 = hmac.new(sign_key, str_to_sign, hashlib.sha1)
    sign =  hasded1.hexdigest()

    param_list = ';'.join([k.lower() for k in sorted(params.keys())])
    header_list = ';'.join([k.lower() for k in sorted(headers.keys())])
    auth_tuple = (secret_id, sign_time, sign_time, param_list, header_list, sign)
    return 'q-sign-algorithm=sha1&q-ak=%s&q-sign-time=%s&q-key-time=%s&q-url-param-list=%s&q-header-list=%s&q-signature=%s'%(auth_tuple)


if __name__ == "__main__":
    headers = {}
    headers['Host'] = 'service.cos.myqcloud.com'
    secret_id = os.environ['MAIN_SECRET_ID']
    secret_key = os.environ['MAIN_SECRET_KEY']
    print(get_auth('/', 'get', headers, secret_id, secret_key))
