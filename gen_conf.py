# -*- coding=utf-8
import os
region = ""
main_secret_id = ""
main_secret_key = ""
main_uin = ""
main_appid = ""
alt_secret_id = ""
alt_secret_key = ""
alt_uin = ""
alt_appid = ""

try:
    region = os.environ["COS_REGION"]
    main_secret_id = os.environ["MAIN_SECRET_ID"]
    main_secret_key = os.environ["MAIN_SECRET_KEY"]
    main_uin = os.environ["MAIN_UIN"]
    main_appid = os.environ["MAIN_APPID"]
    alt_secret_id = os.environ["ALT_SECRET_ID"]
    alt_secret_key = os.environ["ALT_SECRET_KEY"]
    alt_uin = os.environ["ALT_UIN"]
    alt_appid = os.environ["ALT_APPID"]
except Exception as e:
    print (e)
with open("cos.conf", "w") as f:
    body = """[DEFAULT]
host=cos.{region}.myqcloud.com
s3website_domain=cos-website.{region}.myqcloud.com
is_secure=no
calling_format=subdomain

[fixtures]
bucket prefix = richardyao{{random}}-{mappid}
cos_bucket = richardyao{{random}}
cos_appid = {mappid}
default_region = {region}

[s3 main]
user_id=qcs::cam::uin/{muin}:uin/{muin}
display_name=qcs::cam::uin/{muin}:uin/{muin}
access_key={mid}
secret_key={mkey}
api_name = {region}
port = 80

[s3 alt]
user_id=qcs::cam::uin/{auin}:uin/{auin}
display_name=qcs::cam::uin/{auin}:uin/{auin}
access_key={aid}
secret_key={akey}
cos_alt_appid = {aappid}""".format(region=region,
               mappid=main_appid,
               muin=main_uin,
               mid=main_secret_id,
               mkey=main_secret_key,
               aappid=alt_appid,
               auin=alt_uin,
               aid=alt_secret_id,
               akey=alt_secret_key)
    f.write(body)
               
