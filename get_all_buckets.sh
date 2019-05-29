#!/bin/bash

sign=`./get_service_sign.py`
curl 'http://service.cos.myqcloud.com' -H "Authorization: $sign" -H 'Date:' 2>/dev/null | grep Name | grep -v DisplayName |  awk -F'>' '{print $2}' | awk  -F'<' '{print $1}'
