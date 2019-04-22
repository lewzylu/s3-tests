# -*- coding=utf-8
from __future__ import print_function
import sys
import ConfigParser
import boto.exception
import boto.s3.connection
import bunch
import itertools
import os
import random
import string
import subprocess
from httplib import HTTPConnection, HTTPSConnection
from urlparse import urlparse

from .utils import region_sync_meta

s3 = bunch.Bunch()
config = bunch.Bunch()
targets = bunch.Bunch()

# this will be assigned by setup()
prefix = None
cos_bucket=None
cos_appid = None
cos_alt_appid = None


calling_formats = dict(
    ordinary=boto.s3.connection.OrdinaryCallingFormat(),
    subdomain=boto.s3.connection.SubdomainCallingFormat(),
    vhost=boto.s3.connection.VHostCallingFormat(),
    )

def get_all_buckets():
    out = subprocess.check_output(['/data/home/richardyao/workspace/github/s3-tests/get_all_buckets.sh'])
    buckets = out.splitlines()
    return buckets

def get_prefix():
    assert prefix is not None
    return prefix

def get_cos_bucket():
    assert cos_bucket is not None
    return cos_bucket

def get_cos_appid():
    assert cos_appid is not None
    return cos_appid

def get_cos_alt_appid():
    assert cos_alt_appid is not None
    return cos_alt_appid


def is_slow_backend():
    return slow_backend

def choose_bucket_prefix(template, max_len=30):
    """
    Choose a prefix for our test buckets, so they're easy to identify.

    Use template and feed it more and more random filler, until it's
    as long as possible but still below max_len.
    """
    print("enter choose_bucket_prefix template {template}".format(template=template))
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )

    while rand:
        s = template.format(random=rand)
        if len(s) <= max_len:
            return s
        rand = rand[:-1]

    raise RuntimeError(
        'Bucket prefix template is impossible to fulfill: {template!r}'.format(
            template=template,
            ),
        )


def nuke_prefixed_buckets_on_conn(prefix, name, conn):
    print('Cleaning buckets from connection {name} prefix {prefix!r}.'.format(
        name=name,
        prefix=prefix,
        ))

    for bucket in get_all_buckets():
        print('prefix=',prefix)
        if name == 'main':
            print('Cleaning bucket {bucket}'.format(bucket=bucket))
            try:
                bucket = conn.get_bucket(bucket)
            except boto.exception.S3ResponseError as e:
                 # 兼容一下控制台和架平底层不同步的问题
                 # 控制台存在架平不存在时,直接去删除
                 if e.status == 404:
                     subprocess.call(['/usr/local/services/s3-tests/s3-tests/nuke.sh'])
                     continue
                 else:
                     raise e
            success = False
            version_flag = False
            versioning_result = bucket.get_versioning_status()
            if 'Versioning' in versioning_result:
                version_flag = True
            for i in xrange(2):
                try:
                    for mp in bucket.get_all_multipart_uploads():
                        mp.cancel_upload()
                    if version_flag:
                        iterator = iter(bucket.list_versions())
                        # peek into iterator to issue list operation
                        try:
                            keys = itertools.chain([next(iterator)], iterator)
                        except StopIteration:
                            keys = []  # empty iterator
                    else:
                        keys = bucket.list();
                    for key in keys:
                        print('Cleaning bucket {bucket} key {key}'.format(
                            bucket=bucket,
                            key=key,
                            ))
                        # key.set_canned_acl('private')
                        if version_flag:
                            bucket.delete_key(key.name, version_id = key.version_id)
                        else:
                            bucket.delete_key(key.name)
                    bucket.delete()
                    success = True
                except boto.exception.S3ResponseError as e:
                    if e.error_code != 'AccessDenied':
                        print('GOT UNWANTED ERROR', e.error_code)
                        raise
                    # seems like we don't have permissions set appropriately, we'll
                    # modify permissions and retry
                    pass

                if success:
                    break

                bucket.set_canned_acl('private')


def nuke_prefixed_buckets(prefix):
    print("enter nuke_prefixed_buckets prefix {prefix}".format(prefix=prefix))
    # If no regions are specified, use the simple method
    if targets.main.master == None:
        for name, conn in s3.items():
            print('Deleting buckets on {name}'.format(name=name))
            nuke_prefixed_buckets_on_conn(prefix, name, conn)
    else:
		    # First, delete all buckets on the master connection
		    for name, conn in s3.items():
		        if conn == targets.main.master.connection:
		            print('Deleting buckets on {name} (master)'.format(name=name))
		            nuke_prefixed_buckets_on_conn(prefix, name, conn)

		    # Then sync to propagate deletes to secondaries
		    region_sync_meta(targets.main, targets.main.master.connection)
		    print('region-sync in nuke_prefixed_buckets')

		    # Now delete remaining buckets on any other connection
		    for name, conn in s3.items():
		        if conn != targets.main.master.connection:
		            print('Deleting buckets on {name} (non-master)'.format(name=name))
		            nuke_prefixed_buckets_on_conn(prefix, name, conn)

    print('Done with cleanup of test buckets.')

class TargetConfig:
    def __init__(self, cfg, section):
        self.port = None
        self.api_name = ''
        self.is_master = False
        self.is_secure = False
        self.sync_agent_addr = None
        self.sync_agent_port = 0
        self.sync_meta_wait = 0
        try:
            self.api_name = cfg.get(section, 'api_name')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            pass
        try:
            self.port = cfg.getint(section, 'port')
        except ConfigParser.NoOptionError:
            pass
        try:
            self.host=cfg.get(section, 'host')
        except ConfigParser.NoOptionError:
            raise RuntimeError(
                'host not specified for section {s}'.format(s=section)
                )
        try:
            self.is_master=cfg.getboolean(section, 'is_master')
        except ConfigParser.NoOptionError:
            pass

        try:
            self.is_secure=cfg.getboolean(section, 'is_secure')
        except ConfigParser.NoOptionError:
            pass

        try:
            raw_calling_format = cfg.get(section, 'calling_format')
        except ConfigParser.NoOptionError:
            raw_calling_format = 'ordinary'

        try:
            self.sync_agent_addr = cfg.get(section, 'sync_agent_addr')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            pass

        try:
            self.sync_agent_port = cfg.getint(section, 'sync_agent_port')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            pass

        try:
            self.sync_meta_wait = cfg.getint(section, 'sync_meta_wait')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            pass


        try:
            self.calling_format = calling_formats[raw_calling_format]
        except KeyError:
            raise RuntimeError(
                'calling_format unknown: %r' % raw_calling_format
                )

class TargetConnection:
    def __init__(self, conf, conn):
        self.conf = conf
        self.connection = conn



class RegionsInfo:
    def __init__(self):
        self.m = bunch.Bunch()
        self.master = None
        self.secondaries = []

    def add(self, name, region_config):
        self.m[name] = region_config
        if (region_config.is_master):
            if not self.master is None:
                raise RuntimeError(
                    'multiple regions defined as master'
                    )
            self.master = region_config
        else:
            self.secondaries.append(region_config)
    def get(self, name):
        return self.m[name]
    def get(self):
        return self.m
    def iteritems(self):
        return self.m.iteritems()

regions = RegionsInfo()


class RegionsConn:
    def __init__(self):
        self.m = bunch.Bunch()
        self.default = None
        self.master = None
        self.secondaries = []

    def iteritems(self):
        return self.m.iteritems()

    def set_default(self, conn):
        self.default = conn

    def add(self, name, conn):
        self.m[name] = conn
        if not self.default:
            self.default = conn
        if (conn.conf.is_master):
            self.master = conn
        else:
            self.secondaries.append(conn)


# nosetests --processes=N with N>1 is safe
_multiprocess_can_split_ = True

def setup():
    print("enter setup\n")

    cfg = ConfigParser.RawConfigParser()
    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable S3TEST_CONF to a config file.',
            )
    with file(path) as f:
        cfg.readfp(f)

    global prefix
    global targets
    global slow_backend
    global cos_bucket
    global cos_appid
    global cos_alt_appid

    try:
        template = cfg.get('fixtures', 'bucket prefix')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        template = 'test-{random}-'
    prefix = choose_bucket_prefix(template=template)


    try:
        template = cfg.get('fixtures', 'cos_bucket')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        template = 'test{random}'
    cos_bucket = choose_bucket_prefix(template=template)

    try:
        cos_appid = cfg.get('fixtures', 'cos_appid')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        # length of random app id is 10
        cos_appid = ''.join(
            random.choice(string.digits)
            for c in range(10)
        )

    try:
        cos_alt_appid = cfg.get('s3 alt', 'cos_alt_appid')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        # length of random app id is 10
        cos_alt_appid = ''.join(
            random.choice(string.digits)
            for c in range(10)
        )


    print("111")

    try:
        slow_backend = cfg.getboolean('fixtures', 'slow backend')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        slow_backend = False

    # pull the default_region out, if it exists
    try:
        default_region = cfg.get('fixtures', 'default_region')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        default_region = None

    s3.clear()
    config.clear()
    print("222")

    for section in cfg.sections():
        try:
            (type_, name) = section.split(None, 1)
        except ValueError:
            continue
        if type_ != 'region':
            continue
        regions.add(name, TargetConfig(cfg, section))

    for section in cfg.sections():
        try:
            (type_, name) = section.split(None, 1)
        except ValueError:
            continue
        if type_ != 's3':
            continue

        if len(regions.get()) == 0:
            regions.add("default", TargetConfig(cfg, section))

        config[name] = bunch.Bunch()
        for var in [
            'user_id',
            'display_name',
            'email',
            's3website_domain',
            'host',
            'port',
            'is_secure',
            'kms_keyid',
            ]:
            try:
                config[name][var] = cfg.get(section, var)
            except ConfigParser.NoOptionError:
                pass

        targets[name] = RegionsConn()

        for (k, conf) in regions.iteritems():
            conn = boto.s3.connection.S3Connection(
                aws_access_key_id=cfg.get(section, 'access_key'),
                aws_secret_access_key=cfg.get(section, 'secret_key'),
                is_secure=conf.is_secure,
                port=conf.port,
                host=conf.host,
                # TODO test vhost calling format
                calling_format=conf.calling_format,
                )

            temp_targetConn = TargetConnection(conf, conn)
            targets[name].add(k, temp_targetConn)

            # Explicitly test for and set the default region, if specified.
            # If it was not specified, use the 'is_master' flag to set it.
            if default_region:
                if default_region == name:
                    targets[name].set_default(temp_targetConn)
            elif conf.is_master:
                targets[name].set_default(temp_targetConn)

        s3[name] = targets[name].default.connection
    print("333")

    # WARNING! we actively delete all buckets we see with the prefix
    # we've chosen! Choose your prefix with care, and don't reuse
    # credentials!

    # We also assume nobody else is going to use buckets with that
    # prefix. This is racy but given enough randomness, should not
    # really fail.
    nuke_prefixed_buckets(prefix=prefix)


def teardown():
    # remove our buckets here also, to avoid littering
    nuke_prefixed_buckets(prefix=prefix)
    print("enter teardown")


bucket_counter = itertools.count(1)


def get_new_bucket_name():
    """
    Get a bucket name that probably does not exist.

    We make every attempt to use a unique random prefix, so if a
    bucket by this name happens to exist, it's ok if tests give
    false negatives.
    """
    print("enter get_new_bucket_name")
    name = '{num}{prefix}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    return name

def get_new_bucket_name_cos_style(cos_bucket=None, cos_appid=None, prefix='', suffix='', is_use_counter=False):
    """
    Get a bucket name that probably does not exist.

    We make every attempt to use a unique random prefix, so if a
    bucket by this name happens to exist, it's ok if tests give
    false negatives.
    """
    print("enter get_new_bucket_name_cos_style")
    if cos_bucket is None:
        cos_bucket = get_cos_bucket()

    if cos_appid is None:
        cos_appid = get_cos_appid()

    num = ''
    if is_use_counter:
        num=next(bucket_counter)
    
    name = '{prefix}{num}{bucket}{suffix}-{appid}'.format(
        bucket=cos_bucket,
        appid=cos_appid,
        prefix=prefix,
        suffix=suffix,
        num=num,
        )
    return name


def get_new_bucket(target=None, name=None, headers=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    if target is None:
        target = targets.main.default
    connection = target.connection
    if name is None:
        name = get_new_bucket_name()
    # the only way for this to fail with a pre-existing bucket is if
    # someone raced us between setup nuke_prefixed_buckets and here;
    # ignore that as astronomically unlikely
    bucket = connection.create_bucket(name, location=target.conf.api_name, headers=headers)
    return bucket

def _make_request(method, bucket, key, body=None, authenticated=False, response_headers=None, request_headers=None, expires_in=100000, path_style=False, timeout=None):
    """
    issue a request for a specified method, on a specified <bucket,key>,
    with a specified (optional) body (encrypted per the connection), and
    return the response (status, reason).

    If key is None, then this will be treated as a bucket-level request.

    If the request or response headers are None, then default values will be
    provided by later methods.
    """
    if not path_style:
        if not request_headers:
            request_headers = dict()
        conn = bucket.connection
        request_headers['Host'] = conn.calling_format.build_host(conn.server_name(), bucket.name)

    if authenticated:
        urlobj = None
        if key is not None:
            urlobj = key
        elif bucket is not None:
            urlobj = bucket
        else:
            raise RuntimeError('Unable to find bucket name')
        url = urlobj.generate_url(expires_in, method=method, response_headers=response_headers, headers=request_headers)
        o = urlparse(url)
        path = o.path + '?' + o.query
    else:
        bucketobj = None
        if key is not None:
            path = '/{obj}'.format(obj=key.name)
            bucketobj = key.bucket
        elif bucket is not None:
            path = '/'
            bucketobj = bucket
        else:
            raise RuntimeError('Unable to find bucket name')
        if path_style:
            path = '/{bucket}'.format(bucket=bucketobj.name) + path
    if not path_style:
        host = request_headers['Host']
    else:
        host = s3.main.host
    return _make_raw_request(host=host, port=s3.main.port, method=method, path=path, body=body, request_headers=request_headers, secure=s3.main.is_secure, timeout=timeout)

def _make_bucket_request(method, bucket, body=None, authenticated=False, response_headers=None, request_headers=None, expires_in=100000, path_style=False, timeout=None):
    """
    issue a request for a specified method, on a specified <bucket>,
    with a specified (optional) body (encrypted per the connection), and
    return the response (status, reason)
    """
    return _make_request(method=method, bucket=bucket, key=None, body=body, authenticated=authenticated, response_headers=response_headers, request_headers=request_headers, expires_in=expires_in, path_style=path_style, timeout=timeout)

def _make_raw_request(host, port, method, path, body=None, request_headers=None, secure=False, timeout=None):
    """
    issue a request to a specific host & port, for a specified method, on a
    specified path with a specified (optional) body (encrypted per the
    connection), and return the response (status, reason).

    This allows construction of special cases not covered by the bucket/key to
    URL mapping of _make_request/_make_bucket_request.
    """
    if secure:
        class_ = HTTPSConnection
    else:
        class_ = HTTPConnection

    if request_headers is None:
        request_headers = {}

    c = class_(host, port, strict=True, timeout=timeout)

    # TODO: We might have to modify this in future if we need to interact with
    # how httplib.request handles Accept-Encoding and Host.
    c.request(method, path, body=body, headers=request_headers)

    res = c.getresponse()
    #c.close()

    print(res.status, res.reason)
    return res


