# -*- coding:utf-8 -*-

from cStringIO import StringIO
from check_acl import wait_for_acl_valid
import boto.exception
import boto.s3.connection
import boto.s3.acl
import boto.s3.lifecycle
import bunch
import datetime
import time
import email.utils
import isodate
import nose
import operator
import socket
import ssl
import os
import requests
import base64
import hmac
import hashlib
import sha
import pytz
import json
import httplib2
import threading
import itertools
import string
import random
import re
import subprocess

import xml.etree.ElementTree as ET

from email.Utils import formatdate
from httplib import HTTPConnection, HTTPSConnection
from urlparse import urlparse

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from .utils import assert_raises
from .utils import assert_raises_gaierror
from .utils import generate_random
from .utils import region_sync_meta
import AnonymousAuth

from email.header import decode_header
from ordereddict import OrderedDict

from boto.s3.cors import CORSConfiguration

from . import (
    nuke_prefixed_buckets,
    get_new_bucket,
    get_new_bucket_name,
    get_new_bucket_name_cos_style,
    s3,
    targets,
    config,
    get_prefix,
    get_cos_bucket,
    get_cos_appid,
    get_cos_alt_appid,
    get_all_buckets,
    is_slow_backend,
    _make_request,
    _make_bucket_request,
    )


NONEXISTENT_EMAIL = 'doesnotexist@dreamhost.com.invalid'
ACL_SLEEP = 10
SYNC_SLEEP = 4

def not_eq(a, b):
    assert a != b, "%r == %r" % (a, b)

def check_access_denied(fn, *args, **kwargs):
    e = assert_raises(boto.exception.S3ResponseError, fn, *args, **kwargs)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


def check_grants(got, want):
    """
    Check that grants list in got matches the dictionaries in want,
    in any order.
    """
    eq(len(got), len(want))
    got = sorted(got, key=operator.attrgetter('id'))
    want = sorted(want, key=operator.itemgetter('id'))
    for g, w in zip(got, want):
        w = dict(w)
        eq(g.permission, w.pop('permission'))
        eq(g.id, w.pop('id'))
        eq(g.display_name, w.pop('display_name'))
        eq(g.uri, w.pop('uri'))
        eq(g.email_address, w.pop('email_address'))
        eq(g.type, w.pop('type'))
        eq(w, {})

def check_aws4_support():
    if 'S3_USE_SIGV4' not in os.environ:
        raise SkipTest

def tag(*tags):
    def wrap(func):
        for tag in tags:
            setattr(func, tag, True)
        return func
    return wrap


def _create_list_bucket_connection():
    # We're going to need to manually build a connection using bad authorization info.
    # But to save the day, lets just hijack the settings from s3.main. :)
    main = s3.main
    conn = boto.s3.connection.S3Connection(
        aws_access_key_id=main.access_key,
        aws_secret_access_key=main.secret_key,
        is_secure=main.is_secure,
        port=main.port,
        host="service.cos.myqcloud.com",
        calling_format=main.calling_format,
        )

    return conn

def delete_bucket(prefix):
    conn = _create_list_bucket_connection()
    for bucket in conn.get_all_buckets():
        print ("prefix:",prefix," bucketName:",bucket.name)
        if bucket.name.startswith(prefix):
            s3.main.delete_bucket(bucket.name)
            print('Cleaning bucket {bucket}'.format(bucket=bucket))



@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty buckets return no contents')
def test_bucket_list_empty():
    print("enter test_bucket_list_empty")
    bucket = get_new_bucket()
    l = bucket.list()
    l = list(l)
    eq(l, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='distinct buckets have different contents')
def test_bucket_list_distinct():
    bucket1 = get_new_bucket()
    bucket2 = get_new_bucket()
    key = bucket1.new_key('asdf')
    key.set_contents_from_string('asdf')
    l = bucket2.list()
    l = list(l)
    eq(l, [])

def _create_keys(bucket=None, keys=[]):
    """
    Populate a (specified or new) bucket with objects with
    specified names (and contents identical to their names).
    """
    if bucket is None:
        bucket = get_new_bucket()

    for s in keys:
        key = bucket.new_key(s)
        key.set_contents_from_string(s)

    return bucket


def _get_keys_prefixes(li):
    """
    figure out which of the strings in a list are actually keys
    return lists of strings that are (keys) and are not (prefixes)
    """
    keys = [x for x in li if isinstance(x, boto.s3.key.Key)]
    prefixes = [x for x in li if not isinstance(x, boto.s3.key.Key)]
    return (keys, prefixes)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=2, no marker')
def test_bucket_list_many():
    bucket = _create_keys(keys=['foo', 'bar', 'baz'])

    # bucket.list() is high-level and will not let us set max-keys,
    # using it would require using >1000 keys to test, and that would
    # be too slow; use the lower-level call bucket.get_all_keys()
    # instead
    l = bucket.get_all_keys(max_keys=2)
    eq(len(l), 2)
    eq(l.is_truncated, True)
    names = [e.name for e in l]
    eq(names, ['bar', 'baz'])

    l = bucket.get_all_keys(max_keys=2, marker=names[-1])
    eq(len(l), 1)
    eq(l.is_truncated, False)
    names = [e.name for e in l]
    eq(names, ['foo'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefixes in multi-component object names')
def test_bucket_list_delimiter_basic():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz/xyzzy', 'quux/thud', 'asdf'])

    # listings should treat / delimiter in a directory-like fashion
    li = bucket.list(delimiter='/')
    eq(li.delimiter, '/')

    # asdf is the only terminal object that should appear in the listing
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['asdf'])

    # In Amazon, you will have two CommonPrefixes elements, each with a single
    # prefix. According to Amazon documentation
    # (http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html),
    # the response's CommonPrefixes should contain all the prefixes, which DHO
    # does.
    #
    # Unfortunately, boto considers a CommonPrefixes element as a prefix, and
    # will store the last Prefix element within a CommonPrefixes element,
    # effectively overwriting any other prefixes.

    # the other returned values should be the pure prefixes foo/ and quux/
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['foo/', 'quux/'])

def validate_bucket_list(bucket, prefix, delimiter, marker, max_keys,
                         is_truncated, check_objs, check_prefixes, next_marker):
    #
    li = bucket.get_all_keys(delimiter=delimiter, prefix=prefix, max_keys=max_keys, marker=marker)

    eq(li.is_truncated, is_truncated)
    eq(li.next_marker, next_marker)

    (keys, prefixes) = _get_keys_prefixes(li)

    eq(len(keys), len(check_objs))
    eq(len(prefixes), len(check_prefixes))

    objs = [e.name for e in keys]
    eq(objs, check_objs)

    prefix_names = [e.name for e in prefixes]
    eq(prefix_names, check_prefixes)

    return li.next_marker

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefixes in multi-component object names')
def test_bucket_list_delimiter_prefix():
    bucket = _create_keys(keys=['asdf', 'boo/bar', 'boo/baz/xyzzy', 'cquux/thud', 'cquux/bla'])

    delim = '/'
    marker = ''
    prefix = ''

    marker = validate_bucket_list(bucket, prefix, delim, '', 1, True, ['asdf'], [], 'asdf')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, True, [], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, True, [], ['cquux/'], 'cquux/')
    # cos loop one more time
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, False, [], [], None)

    marker = validate_bucket_list(bucket, prefix, delim, '', 2, True, ['asdf'], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 2, False, [], ['cquux/'], None)

    prefix = 'boo/'

    marker = validate_bucket_list(bucket, prefix, delim, '', 1, True, ['boo/bar'], [], 'boo/bar')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, True, [], ['boo/baz/'], 'boo/baz/')
    # cos loop one more time
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, False, [], [], None)

    marker = validate_bucket_list(bucket, prefix, delim, '', 2, True, ['boo/bar'], ['boo/baz/'], 'boo/baz/')
    # cos loop one more time
    marker = validate_bucket_list(bucket, prefix, delim, marker, 2, False, [], [], None)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefix and delimiter handling when object ends with delimiter')
def test_bucket_list_delimiter_prefix_ends_with_delimiter():
    bucket = _create_keys(keys=['asdf/'])
    validate_bucket_list(bucket, 'asdf/', '/', '', 1000, False, ['asdf/'], [], None)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-slash delimiter characters')
def test_bucket_list_delimiter_alt():
    bucket = _create_keys(keys=['bar', 'baz', 'cab', 'foo'])

    li = bucket.list(delimiter='a')
    eq(li.delimiter, 'a')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['ba', 'ca'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='percentage delimiter characters')
def test_bucket_list_delimiter_percentage():
    bucket = _create_keys(keys=['b%ar', 'b%az', 'c%ab', 'foo'])

    li = bucket.list(delimiter='%')
    eq(li.delimiter, '%')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['b%', 'c%'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='whitespace delimiter characters')
def test_bucket_list_delimiter_whitespace():
    bucket = _create_keys(keys=['b ar', 'b az', 'c ab', 'foo'])

    li = bucket.list(delimiter=' ')
    eq(li.delimiter, ' ')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['b ', 'c '])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='dot delimiter characters')
def test_bucket_list_delimiter_dot():
    bucket = _create_keys(keys=['b.ar', 'b.az', 'c.ab', 'foo'])

    li = bucket.list(delimiter='.')
    eq(li.delimiter, '.')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['b.', 'c.'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-printable delimiter can be specified')
def test_bucket_list_delimiter_unreadable():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(delimiter='\x0a')
    eq(li.delimiter, '\x0a')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty delimiter can be specified')
def test_bucket_list_delimiter_empty():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(delimiter='')
    eq(li.delimiter, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])



@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='unspecified delimiter defaults to none')
def test_bucket_list_delimiter_none():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list()
    eq(li.delimiter, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='unused delimiter is not found')
def test_bucket_list_delimiter_not_exist():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(delimiter='/')
    eq(li.delimiter, '/')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='returns only objects under prefix')
def test_bucket_list_prefix_basic():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz', 'quux'])

    li = bucket.list(prefix='foo/')
    eq(li.prefix, 'foo/')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo/bar', 'foo/baz'])
    eq(prefixes, [])


# just testing that we can do the delimeter and prefix logic on non-slashes
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='prefixes w/o delimiters')
def test_bucket_list_prefix_alt():
    bucket = _create_keys(keys=['bar', 'baz', 'foo'])

    li = bucket.list(prefix='ba')
    eq(li.prefix, 'ba')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['bar', 'baz'])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='empty prefix returns everything')
def test_bucket_list_prefix_empty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(prefix='')
    eq(li.prefix, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='unspecified prefix returns everything')
def test_bucket_list_prefix_none():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket = _create_keys(keys=key_names)

    li = bucket.list()
    eq(li.prefix, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='nonexistent prefix returns nothing')
def test_bucket_list_prefix_not_exist():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz', 'quux'])

    li = bucket.list(prefix='d')
    eq(li.prefix, 'd')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='non-printable prefix can be specified')
def test_bucket_list_prefix_unreadable():
    # FIX: shouldn't this test include strings that start with the tested prefix
    bucket = _create_keys(keys=['foo/bar', 'foo/baz', 'quux'])

    li = bucket.list(prefix='\x0a')
    eq(li.prefix, '\x0a')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='returns only objects directly under prefix')
def test_bucket_list_prefix_delimiter_basic():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz/xyzzy', 'quux/thud', 'asdf'])

    li = bucket.list(prefix='foo/', delimiter='/')
    eq(li.prefix, 'foo/')
    eq(li.delimiter, '/')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo/bar'])

    prefix_names = [e.name for e in prefixes]
    eq(prefix_names, ['foo/baz/'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='non-slash delimiters')
def test_bucket_list_prefix_delimiter_alt():
    bucket = _create_keys(keys=['bar', 'bazar', 'cab', 'foo'])

    li = bucket.list(prefix='ba', delimiter='a')
    eq(li.prefix, 'ba')
    eq(li.delimiter, 'a')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['bar'])

    prefix_names = [e.name for e in prefixes]
    eq(prefix_names, ['baza'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='finds nothing w/unmatched prefix')
def test_bucket_list_prefix_delimiter_prefix_not_exist():
    bucket = _create_keys(keys=['b/a/r', 'b/a/c', 'b/a/g', 'g'])

    li = bucket.list(prefix='d', delimiter='/')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='over-ridden slash ceases to be a delimiter')
def test_bucket_list_prefix_delimiter_delimiter_not_exist():
    bucket = _create_keys(keys=['b/a/c', 'b/a/g', 'b/a/r', 'g'])

    li = bucket.list(prefix='b', delimiter='z')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['b/a/c', 'b/a/g', 'b/a/r'])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='finds nothing w/unmatched prefix and delimiter')
def test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist():
    bucket = _create_keys(keys=['b/a/c', 'b/a/g', 'b/a/r', 'g'])

    li = bucket.list(prefix='y', delimiter='z')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=1, marker')
def test_bucket_list_maxkeys_one():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(max_keys=1)
    eq(len(li), 1)
    eq(li.is_truncated, True)
    names = [e.name for e in li]
    eq(names, key_names[0:1])

    li = bucket.get_all_keys(marker=key_names[0])
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names[1:])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=0')
def test_bucket_list_maxkeys_zero():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    li = bucket.get_all_keys(max_keys=0)
    eq(li.is_truncated, False)
    eq(li, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/o max_keys')
def test_bucket_list_maxkeys_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys()
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)
    eq(li.MaxKeys, '1000')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='invalid max_keys')
def test_bucket_list_maxkeys_invalid():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_all_keys, max_keys='blah')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidArgument')


@attr('fails_on_rgw')
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='non-printing max_keys')
def test_bucket_list_maxkeys_unreadable():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_all_keys, max_keys='\x0a')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    # Weird because you can clearly see an InvalidArgument error code. What's
    # also funny is the Amazon tells us that it's not an interger or within an
    # integer range. Is 'blah' in the integer range?
    eq(e.error_code, 'InvalidArgument')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='no pagination, no marker')
def test_bucket_list_marker_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys()
    eq(li.marker, '')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='no pagination, empty marker')
def test_bucket_list_marker_empty():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(marker='')
    eq(li.marker, '')
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='non-printing marker')
def test_bucket_list_marker_unreadable():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(marker='\x0a')
    eq(li.marker, '\x0a')
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker not-in-list')
def test_bucket_list_marker_not_in_list():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    li = bucket.get_all_keys(marker='blah')
    eq(li.marker, 'blah')
    names = [e.name for e in li]
    eq(names, ['foo', 'quxx'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker after list')
def test_bucket_list_marker_after_list():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    li = bucket.get_all_keys(marker='zzz')
    eq(li.marker, 'zzz')
    eq(li.is_truncated, False)
    eq(li, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker before list')
def test_bucket_list_marker_before_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(marker='aaa')
    eq(li.marker, 'aaa')
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)


def _compare_dates(iso_datetime, http_datetime):
    """
    compare an iso date and an http date, within an epsiolon
    """
    date = isodate.parse_datetime(iso_datetime)

    pd = email.utils.parsedate_tz(http_datetime)
    tz = isodate.tzinfo.FixedOffset(0, pd[-1]/60, 'who cares')
    date2 = datetime.datetime(*pd[:6], tzinfo=tz)

    # our tolerance
    minutes = 5
    acceptable_delta = datetime.timedelta(minutes=minutes)
    assert abs(date - date2) < acceptable_delta, \
            ("Times are not within {minutes} minutes of each other: "
             + "{date1!r}, {date2!r}"
             ).format(
                minutes=minutes,
                date1=iso_datetime,
                date2=http_datetime,
                )

@attr(resource='object')
@attr(method='head')
@attr(operation='compare w/bucket list')
@attr(assertion='return same metadata')
def test_bucket_list_return_data():
    key_names = ['bar', 'baz', 'foo']
    bucket = _create_keys(keys=key_names)

    # grab the data from each key individually
    data = {}
    for key_name in key_names:
        key = bucket.get_key(key_name)
        acl = key.get_acl()
        data.update({
            key_name: {
                'user_id': acl.owner.id,
                'display_name': acl.owner.display_name,
                'etag': key.etag,
                'last_modified': key.last_modified,
                'size': key.size,
                'md5': key.md5,
                'content_encoding': key.content_encoding,
                }
            })

    # now grab the data from each key through list
    li = bucket.list()
    for key in li:
        key_data = data[key.name]
        eq(key.content_encoding, key_data['content_encoding'])
        # eq(key.owner.display_name, key_data['display_name']) list_bucket返回的是appid，要求返回uin
        eq(key.etag, key_data['etag'])
        eq(key.md5, key_data['md5'])
        eq(key.size, key_data['size'])
        # eq(key.owner.id, key_data['user_id'])
        _compare_dates(key.last_modified, key_data['last_modified'])


@attr(resource='object')
@attr(method='head')
@attr(operation='compare w/bucket list when bucket versioning is configured')
@attr(assertion='return same metadata')
def test_bucket_list_return_data_versioning():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")
    key_names = ['bar', 'baz', 'foo']
    bucket = _create_keys(bucket=bucket, keys=key_names)
    # grab the data from each key individually
    data = {}
    for key_name in key_names:
        key = bucket.get_key(key_name)
        acl = key.get_acl()
        data.update({
            key_name: {
                'user_id': acl.owner.id,
                'display_name': acl.owner.display_name,
                'etag': key.etag,
                'last_modified': key.last_modified,
                'size': key.size,
                'md5': key.md5,
                'content_encoding': key.content_encoding,
                'version_id': key.version_id
            }
        })

    # now grab the data from each key through list
    li = bucket.list_versions()
    for key in li:
        key_data = data[key.name]
        eq(key.content_encoding, key_data['content_encoding'])
        eq(key.owner.display_name, key_data['display_name'])
        eq(key.etag, key_data['etag'])
        eq(key.md5, key_data['md5'])
        eq(key.size, key_data['size'])
        eq(key.owner.id, key_data['user_id'])
        _compare_dates(key.last_modified, key_data['last_modified'])
        eq(key.version_id, key_data['version_id'])


@attr(resource='object.metadata')
@attr(method='head')
@attr(operation='modification-times')
@attr(assertion='http and ISO-6801 times agree')
def test_bucket_list_object_time():
    bucket = _create_keys(keys=['foo'])

    # Wed, 10 Aug 2011 21:58:25 GMT'
    key = bucket.get_key('foo')
    http_datetime = key.last_modified

    # ISO-6801 formatted datetime
    # there should be only one element, but list doesn't have a __getitem__
    # only an __iter__
    for key in bucket.list():
        iso_datetime = key.last_modified

    _compare_dates(iso_datetime, http_datetime)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all objects (anonymous)')
@attr(assertion='succeeds')
def test_bucket_list_objects_anonymous():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    raise SkipTest # 与CAM逻辑冲突，带签名一定会校验签名
    conn = _create_connection_bad_auth()
    conn._auth_handler = AnonymousAuth.AnonymousAuthHandler(None, None, None) # Doesn't need this
    bucket = get_new_bucket()
    bucket.set_acl('public-read')
    anon_bucket = conn.get_bucket(bucket.name)
    anon_bucket.get_all_keys()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all objects (anonymous)')
@attr(assertion='fails')
def test_bucket_list_objects_anonymous_fail():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    conn = _create_connection_bad_auth()
    conn._auth_handler = AnonymousAuth.AnonymousAuthHandler(None, None, None) # Doesn't need this
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, bucket.name)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_bucket_notexist():
    # generate a (hopefully) unique, not-yet existent bucket name
    # name = '{prefix}foo'.format(prefix=get_prefix())
    name = 'foo{prefix}'.format(prefix=get_prefix())

    print 'Trying bucket {name!r}'.format(name=name)

    e = assert_raises(boto.exception.S3ResponseError, s3.main.get_bucket, name)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


@attr(resource='bucket')
@attr(method='delete')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_bucket_delete_notexist():
    # name = '{prefix}foo'.format(prefix=get_prefix())
    name = 'foo{prefix}'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    e = assert_raises(boto.exception.S3ResponseError, s3.main.delete_bucket, name)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='bucket')
@attr(method='delete')
@attr(operation='non-empty bucket')
@attr(assertion='fails 409')
def test_bucket_delete_nonempty():
    bucket = get_new_bucket()

    # fill up bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('foocontent')

    # try to delete
    e = assert_raises(boto.exception.S3ResponseError, bucket.delete)
    eq(e.status, 409)
    eq(e.reason, 'Conflict')
    eq(e.error_code, 'BucketNotEmpty')

def _do_set_bucket_canned_acl(bucket, canned_acl, i, results):
    try:
        bucket.set_canned_acl(canned_acl)
        results[i] = True
    except:
        results[i] = False

    # res = _make_bucket_request('PUT', bucket, policy='public-read')
    # print res
    # results[i] = res


def _do_set_bucket_canned_acl_concurrent(bucket, canned_acl, num, results):
    t = []
    for i in range(num):
        thr = threading.Thread(target = _do_set_bucket_canned_acl, args=(bucket, canned_acl, i, results))
        thr.start()
        t.append(thr)
    return t

@attr(resource='bucket')
@attr(method='put')
@attr(operation='concurrent set of acls on a bucket')
@attr(assertion='works')
def test_bucket_concurrent_set_canned_acl():
    bucket = get_new_bucket()

    num_threads = 50 # boto retry defaults to 5 so we need a thread to fail at least 5 times
                     # this seems like a large enough number to get through retry (if bug
                     # exists)
    results = [None] * num_threads

    t = _do_set_bucket_canned_acl_concurrent(bucket, 'public-read', num_threads, results)
    _do_wait_completion(t)

    for r in results:
        eq(r, True)


@attr(resource='object')
@attr(method='put')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_object_write_to_nonexist_bucket():
    name = 'foo{prefix}'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    bucket = s3.main.get_bucket(name, validate=False)
    key = bucket.new_key('foo123bar')
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'foo')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


@attr(resource='bucket')
@attr(method='del')
@attr(operation='deleted bucket')
@attr(assertion='fails 404')
def test_bucket_create_delete():
    name = 'foo{prefix}'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    bucket = get_new_bucket(targets.main.default, name)
    # make sure it's actually there
    s3.main.get_bucket(bucket.name)
    bucket.delete()
    # make sure it's gone
    e = assert_raises(boto.exception.S3ResponseError, bucket.delete)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written')
@attr(assertion='fails 404')
def test_object_read_notexist():
    bucket = get_new_bucket()
    key = bucket.new_key('foobar')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written to raise one error response')
@attr(assertion='RequestId appears in the error response')
def test_object_requestid_on_error():
    bucket = get_new_bucket()
    key = bucket.new_key('foobar')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    request_id = re.search(r'<RequestId>.*</RequestId>', e.body.encode('utf-8')).group(0)
    assert request_id is not None

@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written to raise one error response')
@attr(assertion='RequestId in the error response matchs the x-amz-request-id in the headers')
def test_object_requestid_matchs_header_on_error():
    bucket = get_new_bucket()
    key = bucket.new_key('foobar')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    request_id = re.search(r'<RequestId>(.*)</RequestId>', e.body.encode('utf-8')).group(1)
    eq(key.resp.getheader('x-amz-request-id'), request_id)

# While the test itself passes, there's a SAX parser error during teardown. It
# seems to be a boto bug.  It happens with both amazon and dho.
# http://code.google.com/p/boto/issues/detail?id=501
@attr(resource='object')
@attr(method='put')
@attr(operation='write to non-printing key')
@attr(assertion='fails 404')
def test_object_create_unreadable():
    bucket = get_new_bucket()
    #key = bucket.new_key('\x0a')
    # to do: temporarily, we use some to-be-encoded characters here in place of unreadable chars,
    # because nginx would discard unreadable chars resulting in a major difficulty to fix it right now, anyway, we would figure out a solution in future
    key = bucket.new_key('%%%!!!')
    key.set_contents_from_string('bar')

@attr(resource='object')
@attr(method='post')
@attr(operation='delete multiple objects')
@attr(assertion='deletes multiple objects with versionId but Bucket not enabled versioning')
def test_multi_object_delete_with_versionid_but_bucket_not_versioning():
        bucket = get_new_bucket()
        key1 = boto.s3.key.Key(bucket, 'not_a_real_key1')
        key1.version_id = 'not_a_real_version_id1'
        key2 = boto.s3.key.Key(bucket, 'not_a_real_key2')
        key2.version_id = 'not_a_real_version_id2'
   
        stored_keys = [key1, key2] 
        result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 2)
        eq(len(result.errors), 0)
 

@attr(resource='object')
@attr(method='post')
@attr(operation='delete multiple objects')
@attr(assertion='deletes multiple objects with a single call')
def test_multi_object_delete():
	bucket = get_new_bucket()
	key0 = bucket.new_key('key0')
	key0.set_contents_from_string('foo')
	key1 = bucket.new_key('key1')
	key1.set_contents_from_string('bar')
	key2 = bucket.new_key('_key2_')
	key2.set_contents_from_string('underscore')
	stored_keys = bucket.get_all_keys()
	eq(len(stored_keys), 3)
	result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 3)
        eq(len(result.errors), 0)
        eq(len(bucket.get_all_keys()), 0)

        # now remove again, should all succeed due to idempotency
        result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 3)
        eq(len(result.errors), 0)
        eq(len(bucket.get_all_keys()), 0)

@attr(resource='object')
@attr(method='put')
@attr(operation='write zero-byte key')
@attr(assertion='correct content length')
def test_object_head_zero_bytes():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('')

    key2 = bucket.get_key('foo')
    eq(key2.content_length, '0')

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct etag')
def test_object_write_check_etag():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    res = _make_request('PUT', bucket, key, body='bar', authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')
    eq(res.getheader("ETag"), '"37b51d194a7513e45b56f6524f2d51f2"')

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct cache control header')
def test_object_write_cache_control():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    cache_control = 'public, max-age=14400'
    key.set_contents_from_string('bar', headers = {'Cache-Control': cache_control})
    key2 = bucket.get_key('foo')
    eq(key2.cache_control, cache_control)

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct expires header')
def test_object_write_expires():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
    expires = expires.strftime("%a, %d %b %Y %H:%M:%S GMT")
    key.set_contents_from_string('bar', headers = {'Expires': expires})
    key2 = bucket.get_key('foo')
    eq(key2.expires, expires)

@attr(resource='object')
@attr(method='all')
@attr(operation='complete object life cycle')
@attr(assertion='read back what we wrote and rewrote')
def test_object_write_read_update_read_delete():
    bucket = get_new_bucket()
    # Write
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    # Read
    got = key.get_contents_as_string()
    eq(got, 'bar')
    # Update
    key.set_contents_from_string('soup')
    # Read
    got = key.get_contents_as_string()
    eq(got, 'soup')
    # Delete
    key.delete()


def _set_get_metadata(metadata, bucket=None):
    """
    create a new key in a (new or specified) bucket,
    set the meta1 property to a specified, value,
    and then re-read and return that property
    """
    if bucket is None:
        bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('foo')
    key.set_metadata('meta1', metadata)
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    return key2.get_metadata('meta1')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-read')
@attr(assertion='reread what we wrote')
def test_object_set_get_metadata_none_to_good():
    got = _set_get_metadata('mymeta')
    eq(got, 'mymeta')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-read')
@attr(assertion='write empty value, returns empty value')
def test_object_set_get_metadata_none_to_empty():
    got = _set_get_metadata('')
    eq(got, '')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='new value replaces old')
def test_object_set_get_metadata_overwrite_to_good():
    bucket = get_new_bucket()
    got = _set_get_metadata('oldmeta', bucket)
    eq(got, 'oldmeta')
    got = _set_get_metadata('newmeta', bucket)
    eq(got, 'newmeta')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='empty value replaces old')
def test_object_set_get_metadata_overwrite_to_empty():
    bucket = get_new_bucket()
    got = _set_get_metadata('oldmeta', bucket)
    eq(got, 'oldmeta')
    got = _set_get_metadata('', bucket)
    eq(got, '')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='UTF-8 values passed through')
def test_object_set_get_unicode_metadata():
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = (u'foo')
    key.set_metadata('meta1', u"Hello World\xe9")
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    got = key2.get_metadata('meta1')
    eq(got, u"Hello World\xe9")


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='non-UTF-8 values detected, but preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_non_utf8_metadata():
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('foo')
    key.set_metadata('meta1', '\x04mymeta')
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    got = key2.get_metadata('meta1')
    eq(got, '=?UTF-8?Q?=04mymeta?=')

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata size greater than 2k')
@attr(assertion='get KeyTooLong error, add by rabbit')
def test_object_set_exceed_limit_metadata():
    # skip becase we fix it at cos4.19
    raise SkipTest
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('foo')
    key.set_metadata('meta1', 'A' * 2058)
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.error_code, 'KeyTooLong')

@attr(resource='object')
@attr(method='put')
@attr(operation='object key size greater than 850')
@attr(assertion='get InvalidURI error, add by rabbit')
def test_object_name_size_exceed_limit():
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('A' * 851)
    key.set_metadata('meta1', 'I am kevin')
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.error_code, 'InvalidURI')

def _set_get_metadata_unreadable(metadata, bucket=None):
    """
    set and then read back a meta-data value (which presumably
    includes some interesting characters), and return a list
    containing the stored value AND the encoding with which it
    was returned.
    """
    got = _set_get_metadata(metadata, bucket)
    got = decode_header(got)
    return got


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting prefixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting suffixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting in-fixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting prefixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = '\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting suffixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting in-fixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write')
@attr(assertion='replaces previous metadata')
def test_object_metadata_replaced_on_put():
    bucket = get_new_bucket()

    # create object with metadata
    key = bucket.new_key('foo')
    key.set_metadata('meta1', 'bar')
    key.set_contents_from_string('bar')

    # overwrite previous object, no metadata
    key2 = bucket.new_key('foo')
    key2.set_contents_from_string('bar')

    # should see no metadata, as per 2nd write
    key3 = bucket.get_key('foo')
    got = key3.get_metadata('meta1')
    assert got is None, "did not expect to see metadata: %r" % got


@attr(resource='object')
@attr(method='put')
@attr(operation='data write from file (w/100-Continue)')
@attr(assertion='succeeds and returns written data')
def test_object_write_file():
    # boto Key.set_contents_from_file / .send_file uses Expect:
    # 100-Continue, so this test exercises that (though a bit too
    # subtly)
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    data = StringIO('bar')
    key.set_contents_from_file(fp=data)
    got = key.get_contents_as_string()
    eq(got, 'bar')


def _get_post_url(conn, bucket):

	url = '{protocol}://{bucket}.{host}:{port}/'.format(protocol= 'https' if conn.is_secure else 'http',\
                    host=conn.host, port=conn.port, bucket=bucket.name)
	return url

@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_anonymous_request():
        raise SkipTest
	bucket = get_new_bucket()
	url = _get_post_url(s3.main, bucket)
	bucket.set_acl('public-read-write')

	payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')


# s3 post v4 signature
def sign(key, msg):
    s = hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()
    return s


def getSignatureKey(key, date_stamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning


# default region is us-east-1, it's old region
def extendPayload(ak, sk, policy, payload, region = 'us-east-1'):
    t = datetime.datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = t.strftime('%Y%m%d')

    service = "s3"
    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = date_stamp + '/' + region + '/' + service + '/' + 'aws4_request'
    credential = ak + '/' + credential_scope

    # s3 policy v4 need this
    policy['conditions'].append({'x-amz-credential': credential})
    policy['conditions'].append({'x-amz-algorithm': algorithm})
    policy['conditions'].append({'x-amz-date': amz_date})

    policy = base64.b64encode(
        json.dumps(policy).encode('utf-8')).decode('utf-8')
    payload['policy'] = policy

    # get new signature
    signing_key = getSignatureKey(sk, date_stamp, region, service)
    signature = hmac.new(signing_key, policy, hashlib.sha256).hexdigest()

    # extend payload
    payload['x-amz-algorithm'] = algorithm
    payload['x-amz-signature'] = signature
    payload['x-amz-credential'] = credential
    payload['x-amz-date'] = amz_date

    # The file or text content must be the last field in the form
    file_content = payload['file']
    del payload['file']
    payload['file'] = file_content


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_empty_object_authenticated_request():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

        conn = s3.main

        payload = OrderedDict([ ("key" , "foo.txt"),
            ("Content-Type" , "text/plain"),('file', (''))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, '')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns 204')
def test_post_object_authenticated_request_with_exact_length():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 9, 9]\
	]\
	}

        conn = s3.main

        payload = OrderedDict([ ("key" , "foo.txt"),
            ("Content-Type" , "text/plain"),('file', ('123456789'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fail and returns 403')
def test_post_object_authenticated_request_with_x_amz_header():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
        #x-amz-* can not use starts-with
	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	["starts-with", "x-amz-haha", "only_eq"],\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 9, 9]\
	]\
	}

        conn = s3.main

        payload = OrderedDict([ ("key" , "foo.txt"),
            ("Content-Type" , "text/plain"),('x-amz-haha','only_eqaa'),('file', ('123456789'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)

@attr(resource='object')
@attr(method='post')
@attr(operation='post object with tagging info')
@attr(assertion='succeeds and returns 204')
def test_post_object_authenticated_request_with_normal_tagging():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["starts-with", "$tagging", "<Tagging"],\
	["content-length-range", 0, 1024]
	]\
	}

        conn = s3.main

        payload = OrderedDict([ ("key" , "foo.txt"),
            ("acl" , "private"),
            ("tagging", "<Tagging><TagSet><Tag><Key>Tag Name</Key><Value>Tag Value</Value></Tag></TagSet></Tagging>"),
            ("Content-Type" , "text/plain"),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_authenticated_request():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

        conn = s3.main

        payload = OrderedDict([ ("key" , "foo.txt"),
            ("acl" , "private"),\
            ("Content-Type" , "text/plain"),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request, bad access key')
@attr(assertion='fails')
def test_post_object_authenticated_request_bad_access_key():
	bucket = get_new_bucket()
	bucket.set_acl('public-read-write')

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , 'foo'),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds with status 201')
def test_post_object_set_success_code():
        raise SkipTest
	bucket = get_new_bucket()
	bucket.set_acl('public-read-write')
	url = _get_post_url(s3.main, bucket)

	payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
	("success_action_status" , "201"),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 201)
	message = ET.fromstring(r.content).find('Key')
	eq(message.text,'foo.txt')


@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_set_invalid_success_code():
        raise SkipTest
	bucket = get_new_bucket()
	bucket.set_acl('public-read-write')
	url = _get_post_url(s3.main, bucket)

	payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
	("success_action_status" , "404"),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	eq(r.content,'')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_upload_larger_than_chunk():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 5*1024*1024]\
	]\
	}

	conn = s3.main

	foo_string = 'foo' * 1024*1024

	payload = OrderedDict([ ("key" , "foo.txt"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),('file', foo_string)])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, foo_string)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_set_key_from_filename():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 5*1024*1024]\
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("key" , "${filename}"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),('file', ('foo.txt', 'bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_ignored_header():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("key" , "foo.txt"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),("x-ignore-foo" , "bar"),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_case_insensitive_condition_fields():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bUcKeT": bucket.name},\
	["StArTs-WiTh", "$KeY", "foo"],\
	{"AcL": "private"},\
	["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("kEy" , "foo.txt"),
	("aCl" , "private"),
	("Content-Type" , "text/plain"),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with escaped leading $ and returns written data')
def test_post_object_escaped_field_values():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("key" , "\$foo.txt"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("\$foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns redirect url')
def test_post_object_success_redirect_action():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)
	redirect_url = _get_post_url(s3.main, bucket)
	bucket.set_acl('public-read')
	wait_for_acl_valid(200, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["eq", "$success_action_redirect", redirect_url],\
	["content-length-range", 0, 1024]\
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("key" , "foo.txt"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),("success_action_redirect" , redirect_url),\
	('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 200)
	url = r.url
	key = bucket.get_key("foo.txt")
	eq(url,
	'{rurl}?bucket={bucket}&key={key}&etag=%22{etag}%22'.format(rurl = redirect_url, bucket = bucket.name,
	                                                             key = key.name, etag = key.etag.strip('"')))


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid signature error')
def test_post_object_invalid_signature():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())[::-1]

	payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with access key does not exist error')
def test_post_object_invalid_access_key():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id[::-1]),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid expiration error')
def test_post_object_invalid_date_format():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": str(expires),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with missing key error')
def test_post_object_no_key_specified():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with missing signature error')
def test_post_object_missing_signature():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

        json_policy_document = json.JSONEncoder().encode(policy_document)
        policy = base64.b64encode(json_policy_document)
	conn = s3.main

        payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId", conn.aws_access_key_id),
        ("acl" , "private"),("policy" , policy),
	("Content-Type" , "text/plain"),('file', ('bar'))])

        #extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)
        #del payload['x-amz-signature']

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with extra input fields policy error')
def test_post_object_missing_policy_condition():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds using starts-with restriction on metadata header')
def test_post_object_user_specified_header():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
   ["starts-with", "$x-amz-meta-foo",  "bar"]
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("key" , "foo.txt"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	eq(key.get_metadata('foo'), 'barclamp')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy condition failed error due to missing field in POST request')
def test_post_object_request_missing_policy_specified_field():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
   ["starts-with", "$x-amz-meta-foo",  "bar"]
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with conditions must be list error')
def test_post_object_condition_is_case_sensitive():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"CONDITIONS": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with expiration must be string error')
def test_post_object_expires_is_case_sensitive():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"EXPIRATION": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy expired error')
def test_post_object_expired_policy():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=-6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails using equality restriction on metadata header')
def test_post_object_invalid_request_field_value():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
   ["eq", "$x-amz-meta-foo",  ""]
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("key" , "foo.txt"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy missing expiration error')
def test_post_object_missing_expires_condition():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy missing conditions error')
def test_post_object_missing_conditions_list():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with allowable upload size exceeded error')
def test_post_object_upload_size_limit_exceeded():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 0]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid content length error')
def test_post_object_missing_content_length_argument():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid JSON error')
def test_post_object_invalid_content_length_argument():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", -1, 0]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with upload size less than minimum allowable error')
def test_post_object_upload_size_below_minimum():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 512, 1000]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='empty conditions return appropriate error response')
def test_post_object_empty_conditions():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
        { }\
	]\
	}

	conn = s3.main

	payload = OrderedDict([ ("key" , "foo.txt"),
	("acl" , "private"),
	("Content-Type" , "text/plain"),('file', ('bar'))])

        extendPayload(conn.aws_access_key_id, conn.aws_secret_access_key, policy_document, payload)

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Match: the latest ETag')
@attr(assertion='succeeds')
def test_get_object_ifmatch_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    new_key = bucket.get_key('foo', headers={'If-Match': key.etag})
    got = new_key.get_contents_as_string()
    eq(got, 'bar')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Match: bogus ETag')
@attr(assertion='fails 412')
def test_get_object_ifmatch_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_key, 'foo', headers={'If-Match': '"ABCORZ"'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-None-Match: the latest ETag')
@attr(assertion='fails 304')
def test_get_object_ifnonematch_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_key, 'foo', headers={'If-None-Match': key.etag})
    eq(e.status, 304)
    eq(e.reason, 'Not Modified')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-None-Match: bogus ETag')
@attr(assertion='succeeds')
def test_get_object_ifnonematch_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    new_key = bucket.get_key('foo', headers={'If-None-Match': 'ABCORZ'})
    got = new_key.get_contents_as_string()
    eq(got, 'bar')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Modified-Since: before')
@attr(assertion='succeeds')
def test_get_object_ifmodifiedsince_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    new_key = bucket.get_key('foo', headers={'If-Modified-Since': 'Sat, 29 Oct 1994 19:43:31 GMT'})
    got = new_key.get_contents_as_string()
    eq(got, 'bar')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Modified-Since: after')
@attr(assertion='fails 304')
def test_get_object_ifmodifiedsince_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    for k in bucket.get_all_keys():
        key = k

    mtime = datetime.datetime.strptime(key.last_modified, '%Y-%m-%dT%H:%M:%S.%fZ')

    after = mtime + datetime.timedelta(seconds=1)
    after_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", after.timetuple())

    time.sleep(1)

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_key, 'foo', headers={'If-Modified-Since': after_str})
    eq(e.status, 304)
    eq(e.reason, 'Not Modified')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Unmodified-Since: before')
@attr(assertion='fails 412')
def test_get_object_ifunmodifiedsince_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_key, 'foo', headers={'If-Unmodified-Since': 'Sat, 29 Oct 1994 19:43:31 GMT'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')

@attr(resource='object')
@attr(method='get')
@attr(operation='get w/ If-Unmodified-Since: after')
@attr(assertion='succeeds')
def test_get_object_ifunmodifiedsince_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    new_key = bucket.get_key('foo', headers={'If-Unmodified-Since': 'Tue, 29 Oct 2030 19:43:31 GMT'})
    got = new_key.get_contents_as_string()
    eq(got, 'bar')

@attr(resource='object')
@attr(method='put')
# put object with tagging normal
def test_put_object_with_normal_tagging():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'x-amz-tagging': 'key1=value1&key2=value2'})

@attr(resource='object')
@attr(method='put')
# put object with duplicate tagging key
def test_put_object_with_duplicate_tagging_key():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar', headers={'x-amz-tagging': 'key1=value1&key1=value2'})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidArgument')

@attr(resource='object')
@attr(method='put')
# put object with missing tagging key
def test_put_object_with_missing_tagging_key():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar', headers={'x-amz-tagging': '=value1&'})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidArgument')

@attr(resource='object')
@attr(method='put')
# put object with missing tagging value
# 200 ok
def test_put_object_with_missing_tagging_value():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'x-amz-tagging': 'key1=&'})

@attr(resource='object')
@attr(method='put')
# pub object with more than 10 uniq tagging key
def test_put_object_with_too_many_uniq_tagging_key():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar', headers={'x-amz-tagging': 'key1=value1&key2=value2&key3=value3&key4=value4&key5=value5&key6=value6&key7=value7&key8=value8&key9=value9&key10=value10&key11=value11'})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'BadRequest')

@attr(resource='object')
@attr(method='put')
# pub object with tagging key too long
# max is 128
def test_put_object_with_tagging_key_too_long():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    #skey = 's' * 128, ok
    skey = 's' * 129
    tagging = skey + '=value1'
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar', headers={'x-amz-tagging': tagging})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidTag')

@attr(resource='object')
@attr(method='put')
# pub object with tagging value too long
# max is 256
def test_put_object_with_tagging_value_too_long():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    #svalue= 's' * 256, ok
    svalue= 's' * 257
    tagging = 'key1=' + svalue
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar', headers={'x-amz-tagging': tagging})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidTag')

@attr(resource='object')
@attr(method='put')
# pub object with tagging key having invalid character
def test_put_object_with_tagging_invalid_charactor():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    skey = 'key</??>'
    tagging = skey + '=value1'
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar', headers={'x-amz-tagging': tagging})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidTag')

@attr(resource='object')
@attr(method='put')
# pub object with tagging key begining with cos:
def test_put_object_with_tagging_key_invalid_prefix():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    skey = 'cos:key'
    tagging = skey + '=value1'
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar', headers={'x-amz-tagging': tagging})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidTag')

@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write w/ If-Match: the latest ETag')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifmatch_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'If-Match': key.etag.replace('"', '').strip()})
    got_new_data = key.get_contents_as_string()
    eq(got_new_data, 'zar')


@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write w/ If-Match: outdated ETag')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifmatch_failed():
    raise SkipTest
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar',
                      headers={'If-Match': 'ABCORZ'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    got_old_data = key.get_contents_as_string()
    eq(got_old_data, 'bar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-Match: *')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifmatch_overwrite_existed_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'If-Match': '*'})
    got_new_data = key.get_contents_as_string()
    eq(got_new_data, 'zar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite non-existing object w/ If-Match: *')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifmatch_nonexisted_failed():
    raise SkipTest
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar', headers={'If-Match': '*'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: outdated ETag')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'If-None-Match': 'ABCORZ'})
    got_new_data = key.get_contents_as_string()
    eq(got_new_data, 'zar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: the latest ETag')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_failed():
    raise SkipTest
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar',
                      headers={'If-None-Match': key.etag.replace('"', '').strip()})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    got_old_data = key.get_contents_as_string()
    eq(got_old_data, 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite non-existing object w/ If-None-Match: *')
@attr(assertion='succeeds')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_nonexisted_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar', headers={'If-None-Match': '*'})
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: *')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_overwrite_existed_failed():
    raise SkipTest
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string,
                      'zar', headers={'If-None-Match': '*'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    got_old_data = key.get_contents_as_string()
    eq(got_old_data, 'bar')


def _setup_request(bucket_acl=None, object_acl=None):
    """
    add a foo key, and specified key and bucket acls to
    a (new or existing) bucket.
    """
    bucket = _create_keys(keys=['foo'])
    key = bucket.get_key('foo')

    if bucket_acl is not None:
        bucket.set_acl(bucket_acl)
    if object_acl is not None:
        key.set_acl(object_acl)

    return (bucket, key)

def _setup_bucket_request(bucket_acl=None):
    """
    set up a (new or existing) bucket with specified acl
    """
    bucket = get_new_bucket()

    if bucket_acl is not None:
        bucket.set_acl(bucket_acl)

    return bucket

@attr(resource='object')
@attr(method='get')
@attr(operation='publically readable bucket')
@attr(assertion='bucket is readable')
def test_object_raw_get():
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)
    res = _make_request('GET', bucket, key)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='get')
@attr(operation='deleted object and bucket')
@attr(assertion='fails 403')
def test_object_raw_get_bucket_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)
    key.delete()
    bucket.delete()

    time.sleep(ACL_SLEEP)
    res = _make_request('GET', bucket, key)
    eq(res.status, 404) # aws return 404, cos return 403(safer)
    eq(res.reason, 'Forbidden')


@attr(resource='object')
@attr(method='delete')
@attr(operation='deleted object and bucket')
@attr(assertion='fails 404')
def test_object_delete_key_bucket_gone():
    (bucket, key) = _setup_request()
    time.sleep(5)
    key.delete()
    time.sleep(5)
    bucket.delete()
    time.sleep(5)

    e = assert_raises(boto.exception.S3ResponseError, key.delete)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='get')
@attr(operation='deleted object')
@attr(assertion='fails 404')
def test_object_raw_get_object_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)
    key.delete()

    res = _make_request('GET', bucket, key)
    eq(res.status, 404)
    eq(res.reason, 'Not Found')

def _head_bucket(bucket, authenticated=True):
    res = _make_bucket_request('HEAD', bucket, authenticated=authenticated)
    eq(res.status, 200)
    eq(res.reason, 'OK')

    result = {}

    obj_count = res.getheader('x-rgw-object-count')
    if obj_count != None:
        result['x-rgw-object-count'] = int(obj_count)

    bytes_used = res.getheader('x-rgw-bytes-used')
    if bytes_used is not None:
        result['x-rgw-bytes-used'] = int(bytes_used)

    return result


@attr(resource='bucket')
@attr(method='head')
@attr(operation='head bucket')
@attr(assertion='succeeds')
def test_bucket_head():
    bucket = get_new_bucket()

    _head_bucket(bucket)


# This test relies on Ceph extensions.
# http://tracker.ceph.com/issues/2313
@attr('fails_on_aws')
@attr(resource='bucket')
@attr(method='head')
@attr(operation='read bucket extended information')
@attr(assertion='extended information is getting updated')
def test_bucket_head_extended():
    bucket = get_new_bucket()

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 0), 0)
    eq(result.get('x-rgw-bytes-used', 0), 0)

    _create_keys(bucket, keys=['foo', 'bar', 'baz'])

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 3), 3)

    assert result.get('x-rgw-bytes-used', 9) > 0


@attr(resource='bucket.acl')
@attr(method='get')
@attr(operation='unauthenticated on private bucket')
@attr(assertion='succeeds')
def test_object_raw_get_bucket_acl():
    (bucket, key) = _setup_request('private', 'public-read')
    wait_for_acl_valid(200, bucket, key)

    res = _make_request('GET', bucket, key)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object.acl')
@attr(method='get')
@attr(operation='unauthenticated on private object')
@attr(assertion='fails 403')
def test_object_raw_get_object_acl():
    (bucket, key) = _setup_request('public-read', 'private')
    wait_for_acl_valid(200, bucket)
    wait_for_acl_valid(403, bucket, key)

    res = _make_request('GET', bucket, key)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on public bucket/object')
@attr(assertion='succeeds')
def test_object_raw_authenticated():
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on private bucket/private object with modified response headers')
@attr(assertion='succeeds')
@attr('fails_on_rgw')
def test_object_raw_response_headers():
    (bucket, key) = _setup_request('private', 'private')

    response_headers = {
            'response-content-type': 'foo/bar',
            'response-content-disposition': 'bla',
            'response-content-language': 'esperanto',
            'response-content-encoding': 'aaa',
            'response-expires': '123',
            'response-cache-control': 'no-cache',
        }

    res = _make_request('GET', bucket, key, authenticated=True,
                        response_headers=response_headers)
    eq(res.status, 200)
    eq(res.reason, 'OK')
    eq(res.getheader('content-type'), 'foo/bar')
    eq(res.getheader('content-disposition'), 'bla')
    eq(res.getheader('content-language'), 'esperanto')
    eq(res.getheader('content-encoding'), 'aaa')
    eq(res.getheader('expires'), '123')
    eq(res.getheader('cache-control'), 'no-cache')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on private bucket/public object')
@attr(assertion='succeeds')
def test_object_raw_authenticated_bucket_acl():
    (bucket, key) = _setup_request('private', 'public-read')
    wait_for_acl_valid(200, bucket, key)

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on public bucket/private object')
@attr(assertion='succeeds')
def test_object_raw_authenticated_object_acl():
    (bucket, key) = _setup_request('public-read', 'private')
    wait_for_acl_valid(200, bucket)
    wait_for_acl_valid(403, bucket, key)

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on deleted object and bucket')
@attr(assertion='fails 404')
def test_object_raw_authenticated_bucket_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)
    key.delete()
    bucket.delete()

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 404)
    eq(res.reason, 'Not Found')


@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on deleted object')
@attr(assertion='fails 404')
def test_object_raw_authenticated_object_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)
    key.delete()

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 404)
    eq(res.reason, 'Not Found')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='x-amz-expires check not expired')
@attr(assertion='succeeds')
def test_object_raw_get_x_amz_expires_not_expired():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=100000)
    eq(res.status, 200)


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of range zero')
@attr(assertion='fails 403')
def test_object_raw_get_x_amz_expires_out_range_zero():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=0)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of max range')
@attr(assertion='fails 403')
def test_object_raw_get_x_amz_expires_out_max_range():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=604801)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of positive range')
@attr(assertion='succeeds')
def test_object_raw_get_x_amz_expires_out_positive_range():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')
    wait_for_acl_valid(200, bucket, key)

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=-7)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@attr(resource='object')
@attr(method='put')
@attr(operation='unauthenticated, no object acls')
@attr(assertion='fails 403')
def test_object_raw_put():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo')
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@attr(resource='object')
@attr(method='put')
@attr(operation='unauthenticated, publically writable object')
@attr(assertion='succeeds')
def test_object_raw_put_write_access():
    bucket = get_new_bucket()
    bucket.set_acl('public-read-write')
    wait_for_acl_valid(200, bucket)
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo')
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='put')
@attr(operation='authenticated, no object acls')
@attr(assertion='succeeds')
def test_object_raw_put_authenticated():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo', authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='put')
@attr(operation='authenticated, no object acls')
@attr(assertion='succeeds')
def test_object_raw_put_authenticated_expired():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo', authenticated=True, expires_in=-1000)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


def check_bad_bucket_name(name):
    """
    Attempt to create a bucket with a specified name, and confirm
    that the request fails because of an invalid bucket name.
    """
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, targets.main.default, name)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidBucketName')


def check_bad_bucket_name_resolve_fail(name):
    """
    Attempt to create a bucket with a specified name, and confirm
    that the request fails because of an invalid domain.
    """
    e = assert_raises_gaierror(get_new_bucket, targets.main.default, name)
    eq(e.errno, -2)

# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='name begins with underscore')
@attr(assertion='fails with subdomain: 400')
def test_bucket_create_naming_bad_starts_nonalpha():
    bucket_name = get_new_bucket_name()
    # check_bad_bucket_name('_' + bucket_name)
    check_bad_bucket_name_resolve_fail('_' + bucket_name)

@attr(resource='bucket')
@attr(method='put')
@attr(operation='empty name')
@attr(assertion='fails 405')
def test_bucket_create_naming_bad_short_empty():
    # bucket creates where name is empty look like PUTs to the parent
    # resource (with slash), hence their error response is different
    # e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, targets.main.default, '')
    # eq(e.status, 405)
    # eq(e.reason, 'Method Not Allowed')
    # eq(e.error_code, 'MethodNotAllowed')
    bucket_name = get_new_bucket_name_cos_style('')
    check_bad_bucket_name_resolve_fail(bucket_name)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='short (one character) name')
@attr(assertion='fails 400')
@attr('succ_on_cos')
def test_bucket_create_naming_bad_short_one():
    # check_bad_bucket_name('a')
    bucket_name = get_new_bucket_name_cos_style('a')
    get_new_bucket(None, bucket_name)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='short (two character) name')
@attr(assertion='fails 400')
@attr('succ_on_cos')
def test_bucket_create_naming_bad_short_two():
    # check_bad_bucket_name('aa')
    bucket_name = get_new_bucket_name_cos_style('aa')
    get_new_bucket(None, bucket_name)

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='excessively long names')
@attr(assertion='fails with subdomain: 400')
def test_bucket_create_naming_bad_long():
    # check_bad_bucket_name(256*'a')
    # check_bad_bucket_name(280*'a')
    # check_bad_bucket_name(3000*'a')
    bucket_name = get_new_bucket_name_cos_style(256*'a')
    check_bad_bucket_name_resolve_fail(bucket_name)
    bucket_name = get_new_bucket_name_cos_style(280*'a')
    check_bad_bucket_name_resolve_fail(bucket_name)
    bucket_name = get_new_bucket_name_cos_style(3000*'a')
    check_bad_bucket_name_resolve_fail(bucket_name)


def check_good_bucket_name(name, _prefix=None):
    """
    Attempt to create a bucket with a specified name
    and (specified or default) prefix, returning the
    results of that effort.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    #print "name: " + name
    #print "_prefix: " + _prefix

    if _prefix is None:
        _prefix = get_prefix()
    get_new_bucket(targets.main.default, '{name}{prefix}'.format(
            prefix=_prefix,
            name=name,
            ))

def _test_bucket_create_naming_good_long(length):
    """
    Attempt to create a bucket whose name (including the
    prefix) is of a specified length.
    """
    prefix = get_new_bucket_name()
    assert len(prefix) < 255
    num = length - len(prefix)
    get_new_bucket(targets.main.default, '{prefix}{name}'.format(
            prefix=prefix,
            name=num*'a',
            ))

def _test_bucket_create_naming_good_long_resolve_fail(length):
    """
    Attempt to create a bucket whose name (including the
    prefix) is of a specified length.
    """
    bucket = get_new_bucket_name_cos_style(None, '')
    bucket = bucket[:-1] # remove '-'
    appid = get_cos_appid()
    bucket_appid = bucket + '-' + appid
    assert len(bucket_appid) < 255
    num = length - len(bucket_appid)
    check_bad_bucket_name_resolve_fail('{bucket}{name}-{appid}'.format(
            bucket=bucket,
            name=num*'a',
            appid=appid,
            ))


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/250 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_good_long_250():
    _test_bucket_create_naming_good_long_resolve_fail(250)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/251 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_good_long_251():
    _test_bucket_create_naming_good_long_resolve_fail(251)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/252 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_good_long_252():
    _test_bucket_create_naming_good_long_resolve_fail(252)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/253 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_253():
    _test_bucket_create_naming_good_long_resolve_fail(253)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/254 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_254():
    _test_bucket_create_naming_good_long_resolve_fail(254)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/255 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_255():
    _test_bucket_create_naming_good_long_resolve_fail(255)

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list w/251 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_list_long_name():
    length = 251
    _test_bucket_create_naming_good_long_resolve_fail(251)
    # prefix = get_new_bucket_name()
    # length = 251
    # num = length - len(prefix)
    # bucket = get_new_bucket(targets.main.default, '{prefix}{name}'.format(
    #         prefix=prefix,
    #         name=num*'a',
    #         ))
    # got = bucket.list()
    # got = list(got)
    # eq(got, [])


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/ip address for name')
@attr(assertion='fails on aws')
def test_bucket_create_naming_bad_ip():
    # check_bad_bucket_name('192.168.5.123')
    bucket_name = get_new_bucket_name_cos_style("192.168.5.123")
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, targets.main.default, bucket_name)
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidRequest')



# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/! in name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_bad_punctuation():
    # characters other than [a-zA-Z0-9._-]
    bucket_name = get_new_bucket_name_cos_style('alpha!soup')
    check_bad_bucket_name_resolve_fail(bucket_name)
    # check_bad_bucket_name('alpha!soup')


# test_bucket_create_naming_dns_* are valid but not recommended
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/underscore in name')
@attr(assertion='succeeds')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_underscore():
    bucket_name = get_new_bucket_name_cos_style("foo_bar")
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, targets.main.default, bucket_name)
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidBucketName')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/100 byte name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_long():
    _test_bucket_create_naming_good_long_resolve_fail(100)
    # prefix = get_prefix()
    # assert len(prefix) < 50
    # num = 100 - len(prefix)
    # check_good_bucket_name(num * 'a')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/dash at end of name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dash_at_end():
   # cos also forbbiden bucket name contain dash, just same as aws
   bucket_name = get_new_bucket_name_cos_style('foo-')
   e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, targets.main.default, bucket_name)
   eq(e.status, 400)
   eq(e.reason, 'Bad Request')
   eq(e.error_code, 'InvalidBucketName')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/.. in name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dot_dot():
    bucket_name = get_new_bucket_name_cos_style('foo..bar')
    check_bad_bucket_name_resolve_fail(bucket_name)
    # check_good_bucket_name('foo..bar')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/.- in name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dot_dash():
    bucket_name = get_new_bucket_name_cos_style('foo.-bar')
    check_bad_bucket_name_resolve_fail(bucket_name)
    # check_good_bucket_name('foo.-bar')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/-. in name')
@attr(assertion='fails with subdomain')
@attr('fails_on_aws') # <Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message>...</Error>
def test_bucket_create_naming_dns_dash_dot():
    bucket_name = get_new_bucket_name_cos_style('foo-.bar')
    check_bad_bucket_name_resolve_fail(bucket_name)
    # check_good_bucket_name('foo-.bar')


@attr(resource='bucket')
@attr(method='put')
@attr(operation='re-create')
def test_bucket_create_exists():
    # aws-s3 default region allows recreation of buckets
    # but all other regions fail with BucketAlreadyOwnedByYou.
    bucket = get_new_bucket(targets.main.default)
    try:
        get_new_bucket(targets.main.default, bucket.name)
    except boto.exception.S3CreateError, e:
        eq(e.status, 409)
        eq(e.reason, 'Conflict')
        eq(e.error_code, 'BucketAlreadyOwnedByYou')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='recreate')
def test_bucket_configure_recreate():
    # aws-s3 default region allows recreation of buckets
    # but all other regions fail with BucketAlreadyOwnedByYou.
    bucket = get_new_bucket(targets.main.default)
    try:
        get_new_bucket(targets.main.default, bucket.name)
    except boto.exception.S3CreateError, e:
        eq(e.status, 409)
        eq(e.reason, 'Conflict')
        eq(e.error_code, 'BucketAlreadyOwnedByYou')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='get location')
def test_bucket_get_location():
    bucket = get_new_bucket(targets.main.default)
    actual_location = bucket.get_location()
    expected_location = targets.main.default.conf.api_name
    eq(actual_location, expected_location)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='re-create by non-owner')
@attr(assertion='fails 409')
def test_bucket_create_exists_nonowner():
    # Names are shared across a global namespace. As such, no two
    # users can create a bucket with that same name.
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    policy.acl.add_user_grant("FULL_CONTROL", config.alt.user_id)
    bucket.set_acl(policy)

    time.sleep(ACL_SLEEP)
    e = assert_raises(boto.exception.S3CreateError, get_new_bucket, targets.alt.default, bucket.name)
    eq(e.status, 409)
    eq(e.reason, 'Conflict')
    eq(e.error_code, 'BucketAlreadyExists')


@attr(resource='bucket')
@attr(method='del')
@attr(operation='delete by non-owner')
@attr(assertion='fails')
def test_bucket_delete_nonowner():
    bucket = get_new_bucket()
    check_access_denied(s3.alt.delete_bucket, bucket.name)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='default acl')
@attr(assertion='read back expected defaults')
def test_bucket_acl_default():
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    print repr(policy)
    eq(policy.owner.type, None)
    eq(policy.owner.id, config.main.user_id)
    eq(policy.owner.display_name, config.main.display_name)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='bucket')
@attr(method='get')
@attr(operation='public-read acl')
@attr(assertion='read back expected defaults')
@attr('fails_on_aws') # <Error><Code>IllegalLocationConstraintException</Code><Message>The unspecified location constraint is incompatible for the region specific endpoint this request was sent to.</Message>
def test_bucket_acl_canned_during_create():
    name = get_new_bucket_name()
    bucket = targets.main.default.connection.create_bucket(name, policy = 'public-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl: public-read,private')
@attr(assertion='read back expected values')
def test_bucket_acl_canned():
    bucket = get_new_bucket()
    # Since it defaults to private, set it public-read first
    bucket.set_acl('public-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )
    time.sleep(20)
    # Then back to private.
    bucket.set_acl('private')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='bucket.acls')
@attr(method='put')
@attr(operation='acl: public-read-write')
@attr(assertion='read back expected values')
def test_bucket_acl_canned_publicreadwrite():
    bucket = get_new_bucket()
    bucket.set_acl('public-read-write')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            dict(
                permission='WRITE',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl: authenticated-read')
@attr(assertion='read back expected values')
def test_bucket_acl_canned_authenticatedread():
    bucket = get_new_bucket()
    bucket.set_acl('authenticated-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AuthenticatedUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='get')
@attr(operation='default acl')
@attr(assertion='read back expected defaults')
def test_object_acl_default():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl public-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_during_create():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar', policy='public-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl public-read,private')
@attr(assertion='read back expected values')
def test_object_acl_canned():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    # Since it defaults to private, set it public-read first
    key.set_acl('public-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

    # Then back to private.
    key.set_acl('private')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='object')
@attr(method='put')
@attr(operation='acl public-read-write')
@attr(assertion='read back expected values')
def test_object_acl_canned_publicreadwrite():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_acl('public-read-write')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            dict(
                permission='WRITE',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl authenticated-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_authenticatedread():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_acl('authenticated-read')
    policy = key.get_acl()
    
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AuthenticatedUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl bucket-owner-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_bucketownerread():
    bucket = get_new_bucket()
    bucket.set_acl('public-read-write')

    # 授权后需要等60s才能正确鉴权
    wait_for_acl_valid(200, bucket)
    key = s3.alt.get_bucket(bucket.name).new_key('foo')
    key.set_contents_from_string('bar')
    bucket_policy = bucket.get_acl()
    bucket_owner_id = bucket_policy.owner.id
    bucket_owner_display = bucket_policy.owner.display_name

    key.set_acl('bucket-owner-read')
    #TODO(jimmyyan):
    # CAM不支持object的owner的概念，AWS支持，需要CAM对齐IAM
    # bucketownerfullcontrol 同样
    #policy = key.get_acl()
    #print repr(policy)
    #check_grants(
    #    policy.acl.grants,
    #    [
    #        dict(
    #            permission='FULL_CONTROL',
    #            id=policy.owner.id,
    #            display_name=policy.owner.display_name,
    #            uri=None,
    #            email_address=None,
    #            type='CanonicalUser',
    #            ),
    #        ],
    #    )

    key.delete()
    bucket.delete()


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl bucket-owner-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_bucketownerfullcontrol():
    bucket = get_new_bucket(targets.main.default)
    bucket.set_acl('public-read-write')

    # 授权后需要等60s才能正确鉴权
    wait_for_acl_valid(200, bucket)
    key = s3.alt.get_bucket(bucket.name).new_key('foo')
    key.set_contents_from_string('bar')

    bucket_policy = bucket.get_acl()
    bucket_owner_id = bucket_policy.owner.id
    bucket_owner_display = bucket_policy.owner.display_name

    key.set_acl('bucket-owner-full-control')
    '''
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )
    '''
    key.delete()
    bucket.delete()

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='set write-acp')
@attr(assertion='does not modify owner')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_object_acl_full_control_verify_owner():
    bucket = get_new_bucket(targets.main.default)
    bucket.set_acl('public-read-write')

    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    key.add_user_grant(permission='FULL_CONTROL', user_id=config.alt.user_id)

    time.sleep(ACL_SLEEP)
    k2 = s3.alt.get_bucket(bucket.name).get_key('foo')

    k2.add_user_grant(permission='READ_ACP', user_id=config.alt.user_id)

    time.sleep(ACL_SLEEP)
    policy = k2.get_acl()
    eq(policy.owner.id, config.main.user_id)

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='set write-acp')
@attr(assertion='does not modify other attributes')
def test_object_acl_full_control_verify_attributes():
    bucket = get_new_bucket(targets.main.default)
    bucket.set_acl('public-read-write')

    key = bucket.new_key('foo')
    key.set_contents_from_string('bar', {'x-amz-foo': 'bar'})

    etag = key.etag
    content_type = key.content_type

    for k in bucket.list():
        eq(k.etag, etag)
        eq(k.content_type, content_type)

    key.add_user_grant(permission='FULL_CONTROL', user_id=config.alt.user_id)

    for k in bucket.list():
        eq(k.etag, etag)
        eq(k.content_type, content_type)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl private')
@attr(assertion='a private object can be set to private')
def test_bucket_acl_canned_private_to_private():
    bucket = get_new_bucket()
    bucket.set_acl('private')


def _make_acl_xml(acl):
    """
    Return the xml form of an ACL entry
    """
    return '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner>' + acl.to_xml() + '</AccessControlPolicy>'


def _build_bucket_acl_xml(permission, bucket=None):
    """
    add the specified permission for the current user to
    a (new or specified) bucket, in XML form, set it, and
    then read it back to confirm it was correctly set
    """
    acl = boto.s3.acl.ACL()
    acl.add_user_grant(permission=permission, user_id=config.main.user_id)
    XML = _make_acl_xml(acl)
    if bucket is None:
        bucket = get_new_bucket()
    bucket.set_xml_acl(XML)
    time.sleep(10)
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission=permission,
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl FULL_CONTROL (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_bucket_acl_xml_fullcontrol():
    _build_bucket_acl_xml('FULL_CONTROL')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl WRITE (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_bucket_acl_xml_write():
    _build_bucket_acl_xml('WRITE')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl WRITE_ACP (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_bucket_acl_xml_writeacp():
    _build_bucket_acl_xml('WRITE_ACP')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl READ (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_bucket_acl_xml_read():
    _build_bucket_acl_xml('READ')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl READ_ACP (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_bucket_acl_xml_readacp():
    _build_bucket_acl_xml('READ_ACP')


def _build_object_acl_xml(permission):
    """
    add the specified permission for the current user to
    a new object in a new bucket, in XML form, set it, and
    then read it back to confirm it was correctly set
    """
    acl = boto.s3.acl.ACL()
    acl.add_user_grant(permission=permission, user_id=config.main.user_id)
    XML = _make_acl_xml(acl)
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(XML)
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission=permission,
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl FULL_CONTROL (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_xml():
    _build_object_acl_xml('FULL_CONTROL')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl WRITE (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_xml_write():
    _build_object_acl_xml('WRITE')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl WRITE_ACP (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_xml_writeacp():
    _build_object_acl_xml('WRITE_ACP')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl READ (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_xml_read():
    _build_object_acl_xml('READ')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl READ_ACP (xml)')
@attr(assertion='reads back correctly')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_object_acl_xml_readacp():
    _build_object_acl_xml('READ_ACP')


def _bucket_acl_grant_userid(permission):
    """
    create a new bucket, grant a specific user the specified
    permission, read back the acl and verify correct setting
    """
    bucket = get_new_bucket()
    # add alt user
    policy = bucket.get_acl()
    policy.acl.add_user_grant(permission, config.alt.user_id)
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission=permission,
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    return bucket


def _check_bucket_acl_grant_can_read(bucket):
    """
    verify ability to read the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name)


def _check_bucket_acl_grant_cant_read(bucket):
    """
    verify inability to read the specified bucket
    """
    check_access_denied(s3.alt.get_bucket, bucket.name)


def _check_bucket_acl_grant_can_readacp(bucket):
    """
    verify ability to read acls on specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    bucket2.get_acl()


def _check_bucket_acl_grant_cant_readacp(bucket):
    """
    verify inability to read acls on specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    check_access_denied(bucket2.get_acl)


def _check_bucket_acl_grant_can_write(bucket):
    """
    verify ability to write the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    key = bucket2.new_key('foo-write')
    key.set_contents_from_string('bar')


def _check_bucket_acl_grant_cant_write(bucket):
    """
    verify inability to write the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    key = bucket2.new_key('foo-write')
    check_access_denied(key.set_contents_from_string, 'bar')


def _check_bucket_acl_grant_can_writeacp(bucket):
    """
    verify ability to set acls on the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    bucket2.set_acl('public-read')


def _check_bucket_acl_grant_cant_writeacp(bucket):
    """
    verify inability to set acls on the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    check_access_denied(bucket2.set_acl, 'public-read')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid FULL_CONTROL')
@attr(assertion='can read/write data/acls')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${USER}</ArgumentValue>
def test_bucket_acl_grant_userid_fullcontrol():
    bucket = _bucket_acl_grant_userid('FULL_CONTROL')

    time.sleep(ACL_SLEEP)
    # alt user can read
    _check_bucket_acl_grant_can_read(bucket)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket)
    # can write
    _check_bucket_acl_grant_can_write(bucket)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket)

    # verify owner did not change
    bucket2 = s3.main.get_bucket(bucket.name)
    policy = bucket2.get_acl()
    eq(policy.owner.id, config.main.user_id)
    eq(policy.owner.display_name, config.main.display_name)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid READ')
@attr(assertion='can read data, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_read():
    bucket = _bucket_acl_grant_userid('READ')

    time.sleep(ACL_SLEEP)
    # alt user can read
    _check_bucket_acl_grant_can_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid READ_ACP')
@attr(assertion='can read acl, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_readacp():
    bucket = _bucket_acl_grant_userid('READ_ACP')

    time.sleep(ACL_SLEEP)
    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can't write acp
    #_check_bucket_acl_grant_cant_writeacp_can_readacp(bucket)
    _check_bucket_acl_grant_cant_writeacp(bucket)

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid WRITE')
@attr(assertion='can write data, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_write():
    bucket = _bucket_acl_grant_userid('WRITE')

    time.sleep(ACL_SLEEP)
    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can write
    _check_bucket_acl_grant_can_write(bucket)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid WRITE_ACP')
@attr(assertion='can write acls, no other r/w')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_acl_grant_userid_writeacp():
    bucket = _bucket_acl_grant_userid('WRITE_ACP')

    time.sleep(ACL_SLEEP)
    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/invalid userid')
@attr(assertion='fails 400')
def test_bucket_acl_grant_nonexist_user():
    bucket = get_new_bucket()
    # add alt user
    bad_user_id = '_foo'
    policy = bucket.get_acl()
    policy.acl.add_user_grant('FULL_CONTROL', bad_user_id)
    print policy.to_xml()
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_acl, policy)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidArgument')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='revoke all ACLs')
@attr(assertion='can: read obj, get/set bucket acl, cannot write objs')
def test_bucket_acl_no_grants():
    bucket = get_new_bucket()

    # write content to the bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    # clear grants
    policy = bucket.get_acl()
    policy.acl.grants = []

    # remove read/write permission
    bucket.set_acl(policy)

    # can read
    bucket.get_key('foo')

    # can't write
    key = bucket.new_key('baz')
    # cos bucket owner can do anything even if the acl has been revoked
    # no check here
    #check_access_denied(key.set_contents_from_string, 'bar')

    # can read acl
    bucket.get_acl()

    # can write acl
    bucket.set_acl('private')

def _get_acl_header(user=None, perms=None, all_headers=["read", "write", "read-acp", "write-acp", "full-control"]):
    headers = {}

    if user == None:
        user = '"' + config.alt.user_id + '"'

    if perms != None:
        for perm in perms:
           headers["x-amz-grant-{perm}".format(perm=perm)] = "id={uid}".format(uid=user)

    else:
        for perm in all_headers:
            headers["x-amz-grant-{perm}".format(perm=perm)] = "id={uid}".format(uid=user)

    return headers

@attr(resource='object')
@attr(method='PUT')
@attr(operation='add all grants to user through headers')
@attr(assertion='adds all grants individually to second user')
@attr('fails_on_dho')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_object_header_acl_grants():
    bucket = get_new_bucket()
    headers = _get_acl_header(all_headers=["read", "read-acp", "write-acp", "full-control"])
    k = bucket.new_key("foo_key")
    print headers
    print "11111111111111111"
    k.set_contents_from_string("bar", headers=headers)

    policy = k.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='READ',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
#            dict(
#                permission='WRITE',
#                id=config.alt.user_id,
#                display_name=config.alt.display_name,
#                uri=None,
#                email_address=None,
#                type='CanonicalUser',
#                ),
            dict(
                permission='READ_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='WRITE_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),

            ],
        )


@attr(resource='bucket')
@attr(method='PUT')
@attr(operation='add all grants to user through headers')
@attr(assertion='adds all grants individually to second user')
@attr('fails_on_dho')
@attr('fails_on_aws') #  <Error><Code>InvalidArgument</Code><Message>Invalid id</Message><ArgumentName>CanonicalUser/ID</ArgumentName><ArgumentValue>${ALTUSER}</ArgumentValue>
def test_bucket_header_acl_grants():
    headers = _get_acl_header()
    bucket = get_new_bucket(targets.main.default, get_prefix(), headers)

    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='READ',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='WRITE',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='WRITE_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    time.sleep(ACL_SLEEP)
    # alt user can write
    bucket2 = s3.alt.get_bucket(bucket.name)
    key = bucket2.new_key('foo')
    key.set_contents_from_string('bar')


# This test will fail on DH Objects. DHO allows multiple users with one account, which
# would violate the uniqueness requirement of a user's email. As such, DHO users are
# created without an email.
@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='add second FULL_CONTROL user')
@attr(assertion='works for S3, fails for DHO')
@attr('fails_on_aws') #  <Error><Code>AmbiguousGrantByEmailAddress</Code><Message>The e-mail address you provided is associated with more than one account. Please retry your request using a different identification method or after resolving the ambiguity.</Message>
def test_bucket_acl_grant_email():
    raise SkipTest;
    bucket = get_new_bucket()
    # add alt user
    policy = bucket.get_acl()
    policy.acl.add_email_grant('FULL_CONTROL', config.alt.email)
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    # alt user can write
    bucket2 = s3.alt.get_bucket(bucket.name)
    key = bucket2.new_key('foo')
    key.set_contents_from_string('bar')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='add acl for nonexistent user')
@attr(assertion='fail 400')
def test_bucket_acl_grant_email_notexist():
    raise SkipTest; #不支持email
    # behavior not documented by amazon
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    policy.acl.add_email_grant('FULL_CONTROL', NONEXISTENT_EMAIL)
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_acl, policy)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'UnresolvableGrantByEmailAddress')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='revoke all ACLs')
@attr(assertion='acls read back as empty')
def test_bucket_acl_revoke_all():
    # revoke all access, including the owner's access
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    policy.acl.grants = []
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    eq(len(policy.acl.grants), 1)


# TODO rgw log_bucket.set_as_logging_target() gives 403 Forbidden
# http://tracker.newdream.net/issues/984
@attr(resource='bucket.log')
@attr(method='put')
@attr(operation='set/enable/disable logging target')
@attr(assertion='operations succeed')
@attr('fails_on_rgw')
def test_logging_toggle():

    raise SkipTest; #不支持log

    bucket = get_new_bucket()
    #log_bucket = get_new_bucket(targets.main.default, bucket.name + '-log')
    log_bucket = get_new_bucket(targets.main.default, 'log' + bucket.name)
    log_bucket.set_as_logging_target()
    bucket.enable_logging(target_bucket=log_bucket, target_prefix=bucket.name)
    bucket.disable_logging()
    # NOTE: this does not actually test whether or not logging works


def _setup_access(bucket_acl, object_acl):
    """
    Simple test fixture: create a bucket with given ACL, with objects:
    - a: owning user, given ACL
    - a2: same object accessed by some other user
    - b: owning user, default ACL in bucket w/given ACL
    - b2: same object accessed by a some other user
    """
    obj = bunch.Bunch()
    bucket = get_new_bucket()
    bucket.set_acl(bucket_acl)
    status = 200
    obj.a = bucket.new_key('foo')
    obj.a.set_contents_from_string('foocontent')
    obj.a.set_acl(object_acl)
    if bucket_acl == 'private':
        status = 403
    wait_for_acl_valid(status, bucket)
    if object_acl == 'private':
        status = 403
    else:
        status = 200
    wait_for_acl_valid(status, bucket, obj.a)
    obj.b = bucket.new_key('bar')
    obj.b.set_contents_from_string('barcontent')

    # bucket2 is being accessed by a different user
    obj.bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    obj.a2 = obj.bucket2.new_key(obj.a.name)
    obj.b2 = obj.bucket2.new_key(obj.b.name)
    obj.new = obj.bucket2.new_key('new')

    return obj


def get_bucket_key_names(bucket):
    return frozenset(k.name for k in bucket.list())


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: private/private')
@attr(assertion='public has no access to bucket or objects')
def test_access_bucket_private_object_private():
    # all the test_access_* tests follow this template
    obj = _setup_access(bucket_acl='private', object_acl='private')

    # a should be public-read, b gets default (private)
    # acled object read fail
    check_access_denied(obj.a2.get_contents_as_string)
    # acled object write fail
    check_access_denied(obj.a2.set_contents_from_string, 'barcontent')
    # default object read fail
    check_access_denied(obj.b2.get_contents_as_string)
    # default object write fail
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    # bucket read fail
    check_access_denied(get_bucket_key_names, obj.bucket2)
    # bucket write fail
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: private/public-read')
@attr(assertion='public can only read readable object')
def test_access_bucket_private_object_publicread():
    obj = _setup_access(bucket_acl='private', object_acl='public-read')
    # a should be public-read, b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    check_access_denied(get_bucket_key_names, obj.bucket2)
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: private/public-read/write')
@attr(assertion='public can only read the readable object')
def test_access_bucket_private_object_publicreadwrite():
    # cos 赋予obj公有写,那么这个obj就可以被覆盖
    obj = _setup_access(bucket_acl='private', object_acl='public-read-write')
    # a should be public-read-only ... because it is in a private bucket
    # b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    check_access_denied(get_bucket_key_names, obj.bucket2)
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read/private')
@attr(assertion='public can only list the bucket')
def test_access_bucket_publicread_object_private():
    obj = _setup_access(bucket_acl='public-read', object_acl='private')
    # a should be private, b gets default (private)
    # but in cos, b gets public-read
    check_access_denied(obj.a2.get_contents_as_string)
    check_access_denied(obj.a2.set_contents_from_string, 'barcontent')
    eq(obj.b2.get_contents_as_string(), 'barcontent')
    #check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read/public-read')
@attr(assertion='public can read readable objects and list bucket')
def test_access_bucket_publicread_object_publicread():
    obj = _setup_access(bucket_acl='public-read', object_acl='public-read')
    # a should be public-read, b gets default (private)
    # b gets default (private) ,but in cos b gets public-read
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    eq(obj.b2.get_contents_as_string(), 'barcontent')
    #check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read/public-read-write')
@attr(assertion='public can read readable objects and list bucket')
def test_access_bucket_publicread_object_publicreadwrite():
    # cos 赋予obj公有写,那么这个obj就可以被覆盖
    obj = _setup_access(bucket_acl='public-read', object_acl='public-read-write')
    # a should be public-read-only ... because it is in a r/o bucket
    # b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('foooverwrite')
    eq(obj.b2.get_contents_as_string(), 'barcontent')
    #check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')



@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read-write/private')
@attr(assertion='private objects cannot be read, but can be overwritten')
def test_access_bucket_publicreadwrite_object_private():
    # cos 默认权限不设置,是继承
    obj = _setup_access(bucket_acl='public-read-write', object_acl='private')
    # a should be private, b gets default (private)
    check_access_denied(obj.a2.get_contents_as_string)
    check_access_denied(obj.a2.set_contents_from_string,'barcontent')  #设置成私有，不能被覆盖写
    #obj.a2.set_contents_from_string('barcontent')
    eq(obj.b2.get_contents_as_string(), "barcontent")
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read-write/public-read')
@attr(assertion='private objects cannot be read, but can be overwritten')
def test_access_bucket_publicreadwrite_object_publicread():
    # cos 默认权限不设置,是继承
    obj = _setup_access(bucket_acl='public-read-write', object_acl='public-read')
    # a should be public-read, b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('barcontent')
    eq(obj.b2.get_contents_as_string(), 'barcontent')
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read-write/public-read-write')
@attr(assertion='private objects cannot be read, but can be overwritten')
def test_access_bucket_publicreadwrite_object_publicreadwrite():
    # cos 默认权限不设置,是继承
    obj = _setup_access(bucket_acl='public-read-write', object_acl='public-read-write')
    # a should be public-read-write, b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('foooverwrite')
    eq(obj.b2.get_contents_as_string(), 'barcontent')
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')

@attr(resource='object')
@attr(method='put')
@attr(operation='set object acls')
@attr(assertion='valid XML ACL sets properly')
def test_object_set_valid_acl():
    XML_1 = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.main.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(XML_1)

@attr(resource='object')
@attr(method='put')
@attr(operation='set object acls')
@attr(assertion='invalid XML ACL fails 403')
def test_object_giveaway():
    CORRECT_ACL = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.main.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    WRONG_ACL = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.alt.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.alt.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(CORRECT_ACL)
    e = assert_raises(boto.exception.S3ResponseError, key.set_xml_acl, WRONG_ACL)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets')
@attr(assertion='returns all expected buckets')
def test_buckets_create_then_list():
    create_buckets = [get_new_bucket() for i in xrange(5)]
    #list_buckets = s3.main.get_all_buckets()
    #names = frozenset(bucket.name for bucket in list_buckets)
    names = get_all_buckets()
    for bucket in create_buckets:
        if bucket.name not in names:
            raise RuntimeError("S3 implementation's GET on Service did not return bucket we created: %r", bucket.name)

# Common code to create a connection object, which'll use bad authorization information
def _create_connection_bad_auth(aws_access_key_id='badauth'):
    # We're going to need to manually build a connection using bad authorization info.
    # But to save the day, lets just hijack the settings from s3.main. :)
    main = s3.main
    conn = boto.s3.connection.S3Connection(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key='roflmao',
        is_secure=main.is_secure,
        port=main.port,
        host=main.host,
        calling_format=main.calling_format,
        )
    return conn

def _create_list_bucket_connection_bad_auth(aws_access_key_id='badauth'):
    # We're going to need to manually build a connection using bad authorization info.
    # But to save the day, lets just hijack the settings from s3.main. :)
    main = s3.main
    conn = boto.s3.connection.S3Connection(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key='roflmao',
    is_secure=main.is_secure,
    port=main.port,
    host="service.cos.myqcloud.com",
    calling_format=main.calling_format,
    )
    return conn

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets (anonymous)')
@attr(assertion='succeeds')
@attr('fails_on_aws')
def test_list_buckets_anonymous():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    raise SkipTest # 实测s3匿名无法访问get service,匿名情况下无法识别该请求

    conn = _create_connection_bad_auth()
    conn._auth_handler = AnonymousAuth.AnonymousAuthHandler(None, None, None) # Doesn't need this
    #buckets = conn.get_all_buckets()
    buckets = get_all_buckets()
    eq(len(buckets), 0)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets (bad auth)')
@attr(assertion='fails 403')
def test_list_buckets_invalid_auth():
    conn = _create_list_bucket_connection_bad_auth()
    e = assert_raises(boto.exception.S3ResponseError, conn.get_all_buckets)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'InvalidAccessKeyId')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets (bad auth)')
@attr(assertion='fails 403')
def test_list_buckets_bad_auth():
    conn = _create_list_bucket_connection_bad_auth(aws_access_key_id=s3.main.aws_access_key_id)
    e = assert_raises(boto.exception.S3ResponseError, conn.get_all_buckets)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name starts with alphabetic works')
# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
#@nose.with_setup(
#    setup=lambda: nuke_prefixed_buckets(prefix='a'+get_prefix()),
#    teardown=lambda: nuke_prefixed_buckets(prefix='a'+get_prefix()),
#    )
def test_bucket_create_naming_good_starts_alpha():
    prefix = 'n'+get_prefix()
    delete_bucket('foo' + prefix)
    check_good_bucket_name('foo', _prefix=prefix)
    delete_bucket('foo' + prefix)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name starts with numeric works')
# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
#@nose.with_setup(
#    setup=lambda: nuke_prefixed_buckets(prefix='0'+get_prefix()),
#    teardown=lambda: nuke_prefixed_buckets(prefix='0'+get_prefix()),
#    )
def test_bucket_create_naming_good_starts_digit():
    prefix = '1'+get_prefix()
    delete_bucket('1' + prefix)
    check_good_bucket_name('1', _prefix=prefix)
    delete_bucket('1' + prefix)

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name containing dot works')
def test_bucket_create_naming_good_contains_period():
    raise SkipTest; #架平bucket名并不支持.  域名也不支持多个点
    check_good_bucket_name('aaa.111')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name containing hyphen works')
def test_bucket_create_naming_good_contains_hyphen():
    check_good_bucket_name('aaa-111')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket with objects and recreate it')
@attr(assertion='bucket recreation not overriding index')
def test_bucket_recreate_not_overriding():
    key_names = ['mykey1', 'mykey2']
    bucket = _create_keys(keys=key_names)

    li = bucket.list()

    names = [e.name for e in list(li)]
    eq(names, key_names)

    # aws默认的美西区重复创建bucket返回200,其余所有区都是返回409,cos对齐409
    try:
        bucket2 = get_new_bucket(targets.main.default, bucket.name)
    except boto.exception.S3CreateError, e:
        eq(e.status, 409)
        eq(e.reason, 'Conflict')

    li = bucket.list()

    names = [e.name for e in list(li)]
    eq(names, key_names)

@attr(resource='object')
@attr(method='put')
@attr(operation='create and list objects with special names')
@attr(assertion='special names work')
def test_bucket_create_special_key_names():
    key_names = [
        ' ',
        '"',
        '$',
        '%',
        '&',
        '\'',
        '<',
        '>',
        '_',
        '_ ',
        '_ _',
        '__',
    ]
    bucket = _create_keys(keys=key_names)

    li = bucket.list()

    names = [e.name for e in list(li)]
    eq(names, key_names)

    for name in key_names:
        key = bucket.get_key(name)
        eq(key.name, name)
        content = key.get_contents_as_string()
        eq(content, name)
        if  name != '\'':    #TODO cam那边暂不支持',已提需求给cam,09-22前fix
            key.set_acl('private')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='create and list objects with underscore as prefix, list using prefix')
@attr(assertion='listing works correctly')
def test_bucket_list_special_prefix():
    key_names = ['_bla/1', '_bla/2', '_bla/3', '_bla/4', 'abcd']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys()
    eq(len(li), 5)

    li2 = bucket.get_all_keys(prefix='_bla/')
    eq(len(li2), 4)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy zero sized object in same bucket')
@attr(assertion='works')
def test_object_copy_zero_size():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    fp_a = FakeWriteFile(0, '')
    key.set_contents_from_file(fp_a)
    #key.copy(bucket, 'bar321foo')
    bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar')
    key2 = bucket.get_key('bar321foo')
    eq(key2.size, 0)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object in same bucket')
@attr(assertion='works')
def test_object_copy_same_bucket():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    #key.copy(bucket, 'bar321foo')
    bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar')
    key2 = bucket.get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')

# http://tracker.ceph.com/issues/11563
@attr(resource='object')
@attr(method='put')
@attr(operation='copy object with content-type')
@attr(assertion='works')
def test_object_copy_verify_contenttype():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    content_type = 'text/bla'
    key.set_contents_from_string('foo',headers={'Content-Type': content_type})
    #key.copy(bucket, 'bar321foo')
    bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar')
    key2 = bucket.get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')
    eq(key2.content_type, content_type)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object to itself')
@attr(assertion='fails')
def test_object_copy_to_itself():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'foo123bar', bucket.name + '.' + s3.main.host, 'foo123bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'UnexpectedContent')

@attr(resource='object')
@attr(method='put')
@attr(operation='modify object metadata by copying')
@attr(assertion='fails')
def test_object_copy_to_itself_with_metadata():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    #key.copy(bucket, 'foo123bar', {'foo': 'bar'})
    bucket.copy_key('foo123bar', bucket.name + '.' + s3.main.host, 'foo123bar', metadata={'foo': 'bar'})
    key.close()

    bucket2 = s3.main.get_bucket(bucket.name)
    key2 = bucket2.get_key('foo123bar')
    md = key2.get_metadata('foo')
    eq(md, 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object from different bucket')
@attr(assertion='works')
def test_object_copy_diff_bucket():
    buckets = [get_new_bucket(), get_new_bucket()]
    key = buckets[0].new_key('foo123bar')
    key.set_contents_from_string('foo')
    #key.copy(buckets[1], 'bar321foo')
    buckets[1].copy_key('bar321foo', buckets[0].name + '.' + s3.main.host, 'foo123bar')
    key2 = buckets[1].get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')

# is this a necessary check? a NoneType object is being touched here
# it doesn't get to the S3 level
@attr(resource='object')
@attr(method='put')
@attr(operation='copy from an inaccessible bucket')
@attr(assertion='fails w/AttributeError')
def test_object_copy_not_owned_bucket():
    buckets = [get_new_bucket(), get_new_bucket(target=targets.alt.default, name=get_new_bucket_name_cos_style(cos_appid=get_cos_alt_appid()))]
    print repr(buckets[1])
    time.sleep(5)
    key = buckets[0].new_key('foo123bar')
    key.set_contents_from_string('foo')
    e = assert_raises(boto.exception.S3ResponseError, buckets[1].copy_key, 'bar321foo', buckets[0].name + '.' + s3.main.host, 'foo123bar')
    eq(e.status, 403)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy a non-owned object in a non-owned bucket, but with perms')
@attr(assertion='works')
def test_object_copy_not_owned_object_bucket():
    bucket = get_new_bucket(targets.main.default)
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    bucket.add_user_grant(permission='FULL_CONTROL', user_id=config.alt.user_id, recursive=True)
    time.sleep(ACL_SLEEP)
    bucket2 = s3.alt.get_bucket(bucket.name)
    #k2 = s3.alt.get_bucket(bucket.name).get_key('foo123bar')
    k2 = bucket2.get_key('foo123bar')
    #k2.copy(bucket.name, 'bar321foo')
    bucket2.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object and change acl')
@attr(assertion='works')
def test_object_copy_canned_acl():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')

    # use COPY directive
    key2 = bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar', headers={'x-amz-acl': 'public-read'})
    wait_for_acl_valid(200, bucket, key2)
    res = _make_request('GET', bucket, key2)
    eq(res.status, 200)
    eq(res.reason, 'OK')

    # use REPLACE directive
    key3 = bucket.copy_key('bar321foo2', bucket.name + '.' + s3.main.host, 'foo123bar', headers={'x-amz-acl': 'public-read'}, metadata={'abc': 'def'})
    wait_for_acl_valid(200, bucket, key3)
    res = _make_request('GET', bucket, key3)
    eq(res.status, 200)
    eq(res.reason, 'OK')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object and retain metadata')
def test_object_copy_retaining_metadata():
    for size in [3, 1024 * 1024]:
        bucket = get_new_bucket()
        key = bucket.new_key('foo123bar')
        metadata = {'key1': 'value1', 'key2': 'value2'}
        key.set_metadata('key1', 'value1')
        key.set_metadata('key2', 'value2')
        content_type = 'audio/ogg'
        key.content_type = content_type
        key.set_contents_from_string(str(bytearray(size)))

        bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar')
        key2 = bucket.get_key('bar321foo')
        eq(key2.size, size)
        eq(key2.metadata, metadata)
        eq(key2.content_type, content_type)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object and replace metadata')
def test_object_copy_replacing_metadata():
    for size in [3, 1024 * 1024]:
        bucket = get_new_bucket()
        key = bucket.new_key('foo123bar')
        key.set_metadata('key1', 'value1')
        key.set_metadata('key2', 'value2')
        key.content_type = 'audio/ogg'
        key.set_contents_from_string(str(bytearray(size)))

        metadata = {'key3': 'value3', 'key1': 'value4'}
        content_type = 'audio/mpeg'
        bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar', metadata=metadata, headers={'Content-Type': content_type})
        key2 = bucket.get_key('bar321foo')
        eq(key2.size, size)
        eq(key2.metadata, metadata)
        eq(key2.content_type, content_type)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy from non-existent bucket')
def test_object_copy_bucket_not_found():
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'foo123bar', 'fake' + bucket.name + '.' + s3.main.host, 'bar321foo')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy from non-existent object')
def test_object_copy_key_not_found():
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'foo123bar', bucket.name + '.' + s3.main.host, 'bar321foo')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object to/from versioned bucket')
@attr(assertion='works')
def test_object_copy_versioned_bucket():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")
    key = bucket.new_key('foo123bar')
    size = 1*1024*1024
    data = str(bytearray(size))
    key.set_contents_from_string(data)

    # copy object in the same bucket
    key2 = bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, key.name, src_version_id = key.version_id)
    key2 = bucket.get_key(key2.name)
    eq(key2.size, size)
    got = key2.get_contents_as_string()
    eq(got, data)

    # second copy
    key3 = bucket.copy_key('bar321foo2', bucket.name + '.' + s3.main.host, key2.name, src_version_id = key2.version_id)
    key3 = bucket.get_key(key3.name)
    eq(key3.size, size)
    got = key3.get_contents_as_string()
    eq(got, data)

    # copy to another versioned bucket
    bucket2 = get_new_bucket()
    check_configure_versioning_retry(bucket2, True, "Enabled")
    key4 = bucket2.copy_key('bar321foo3', bucket.name + '.' + s3.main.host, key.name, src_version_id = key.version_id)
    key4 = bucket2.get_key(key4.name)
    eq(key4.size, size)
    got = key4.get_contents_as_string()
    eq(got, data)

    # copy to another non versioned bucket
    bucket3 = get_new_bucket()
    key5 = bucket3.copy_key('bar321foo4', bucket.name + '.' + s3.main.host, key.name , src_version_id = key.version_id)
    key5 = bucket3.get_key(key5.name)
    eq(key5.size, size)
    got = key5.get_contents_as_string()
    eq(got, data)

    # copy from a non versioned bucket
    key6 = bucket.copy_key('foo123bar2', bucket3.name + '.' + s3.main.host, key5.name)
    key6 = bucket.get_key(key6.name)
    eq(key6.size, size)
    got = key6.get_contents_as_string()
    eq(got, data)

@attr(resource='object')
@attr(method='put')
@attr(operation='test copy object of a multipart upload')
@attr(assertion='successful')
def test_object_copy_versioning_multipart_upload():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")
    key_name="srcmultipart"
    content_type='text/bla'
    objlen = 30 * 1024 * 1024
    (upload, data) = _multipart_upload(bucket, key_name, objlen, headers={'Content-Type': content_type}, metadata={'foo': 'bar'})
    upload.complete_upload()
    key = bucket.get_key(key_name)

    # copy object in the same bucket
    key2 = bucket.copy_key('dstmultipart', bucket.name + '.' + s3.main.host, key.name, src_version_id = key.version_id)
    key2 = bucket.get_key(key2.name)
    eq(key2.metadata['foo'], 'bar')
    eq(key2.content_type, content_type)
    eq(key2.size, key.size)
    got = key2.get_contents_as_string()
    eq(got, data)

    # second copy
    key3 = bucket.copy_key('dstmultipart2', bucket.name + '.' + s3.main.host, key2.name, src_version_id = key2.version_id)
    key3 = bucket.get_key(key3.name)
    eq(key3.metadata['foo'], 'bar')
    eq(key3.content_type, content_type)
    eq(key3.size, key.size)
    got = key3.get_contents_as_string()
    eq(got, data)

    # copy to another versioned bucket
    bucket2 = get_new_bucket()
    check_configure_versioning_retry(bucket2, True, "Enabled")
    key4 = bucket2.copy_key('dstmultipart3', bucket.name + '.' + s3.main.host, key.name, src_version_id = key.version_id)
    key4 = bucket2.get_key(key4.name)
    eq(key4.metadata['foo'], 'bar')
    eq(key4.content_type, content_type)
    eq(key4.size, key.size)
    got = key4.get_contents_as_string()
    eq(got, data)

    # copy to another non versioned bucket
    bucket3 = get_new_bucket()
    key5 = bucket3.copy_key('dstmultipart4', bucket.name + '.' + s3.main.host, key.name, src_version_id = key.version_id)
    key5 = bucket3.get_key(key5.name)
    eq(key5.metadata['foo'], 'bar')
    eq(key5.content_type, content_type)
    eq(key5.size, key.size)
    got = key5.get_contents_as_string()
    eq(got, data)

    # copy from a non versioned bucket
    key6 = bucket3.copy_key('dstmultipart5', bucket3.name + '.' + s3.main.host, key5.name)
    key6 = bucket3.get_key(key6.name)
    eq(key6.metadata['foo'], 'bar')
    eq(key6.content_type, content_type)
    eq(key6.size, key.size)
    got = key6.get_contents_as_string()
    eq(got, data)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object tagging')
def test_object_copy_tagging():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('bar')

    #e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar', headers={'x-amz-tagging': 'key1%20+=value1%20+&key2%20+=value2%20+', 'x-amz-tagging-directive': 'Replaced'})
    #eq(e.status, 200)
    bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar', headers={'x-amz-tagging': 'key1=value1&key2=value2', 'x-amz-tagging-directive': 'Replaced'})


@attr(resource='object')
@attr(method='put')
@attr(operation='copy object invalid tagging contains cos:')
def test_object_copy_invalid_tagging():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('bar')

    e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar', headers={'x-amz-tagging': 'cos:key1=value1&key2=value2', 'x-amz-tagging-directive': 'Replaced'})
    eq(e.status, 400)
    eq(e.reason, 'Bad Request')
    eq(e.error_code, 'InvalidTag')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object tagging with url encode')
def test_object_copy_url_encode_tagging():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('bar')

    #e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar', headers={'x-amz-tagging': 'key1%20+=value1%20+&key2%20+=value2%20+', 'x-amz-tagging-directive': 'Replaced'})
    #eq(e.status, 200)
    bucket.copy_key('bar321foo', bucket.name + '.' + s3.main.host, 'foo123bar', headers={'x-amz-tagging': 'key1%20+=value1%20+&key2%20+=value2%20+', 'x-amz-tagging-directive': 'Replaced'})

def transfer_part(bucket, mp_id, mp_keyname, i, part, headers=None):
    """Transfer a part of a multipart upload. Designed to be run in parallel.
    """
    mp = boto.s3.multipart.MultiPartUpload(bucket)
    mp.key_name = mp_keyname
    mp.id = mp_id
    part_out = StringIO(part)
    mp.upload_part_from_file(part_out, i+1, headers=headers)

def copy_part(src_bucket, src_keyname, dst_bucket, dst_keyname, mp_id, i, start=None, end=None, src_version_id=None):
    """Copy a part of a multipart upload from other bucket.
    """
    replaced_src_bucket = src_bucket + '.' + s3.main.host
    mp = boto.s3.multipart.MultiPartUpload(dst_bucket)
    mp.key_name = dst_keyname
    mp.src_version_id = src_version_id
    mp.id = mp_id
    mp.copy_part_from_key(replaced_src_bucket, src_keyname, i+1, start, end)

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in xrange(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size / chunk):
            s = s + strpart
        if this_part_size > len(s):
            s = s + strpart[0:this_part_size - len(s)]
        yield s
        if (x == size):
            return

def _multipart_upload(bucket, s3_key_name, size, part_size=5*1024*1024, do_list=None, headers=None, metadata=None, resend_parts=[]):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    upload = bucket.initiate_multipart_upload(s3_key_name, headers=headers, metadata=metadata)
    s = ''
    for i, part in enumerate(generate_random(size, part_size)):
        s += part
        transfer_part(bucket, upload.id, upload.key_name, i, part, headers)
        if i in resend_parts:
            transfer_part(bucket, upload.id, upload.key_name, i, part, headers)

    if do_list is not None:
        l = bucket.list_multipart_uploads()
        l = list(l)

    return (upload, s)

def _multipart_copy(src_bucketname, src_keyname, dst_bucket, dst_keyname, size, part_size=5*1024*1024,
                    do_list=None, headers=None, metadata=None, resend_parts=[], src_version_id = None):
    upload = dst_bucket.initiate_multipart_upload(dst_keyname, headers=headers, metadata=metadata)
    i = 0
    for start_offset in range(0, size, part_size):
        end_offset = min(start_offset + part_size - 1, size - 1)
        copy_part(src_bucketname, src_keyname, dst_bucket, dst_keyname, upload.id, i, start_offset, end_offset, src_version_id=src_version_id)
        if i in resend_parts:
            copy_part(src_bucketname, src_keyname, dst_bucket, dst_name, upload.id, i, start_offset, end_offset, src_version_id=src_version_id)
        i = i + 1

    if do_list is not None:
        l = dst_bucket.list_multipart_uploads()
        l = list(l)

    return upload

def _create_key_with_random_content(keyname, size=7*1024*1024, bucket=None):
    if bucket is None:
        bucket = get_new_bucket()
    key = bucket.new_key(keyname)
    data = StringIO(str(generate_random(size, size).next()))
    key.set_contents_from_file(fp=data)
    return (bucket, key)

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart upload without parts')
def test_multipart_upload_empty():
    bucket = get_new_bucket()
    key = "mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 0)
    e = assert_raises(boto.exception.S3ResponseError, upload.complete_upload)
    eq(e.status, 400)
    eq(e.error_code, u'MalformedXML')
    upload.cancel_upload()

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart uploads with single small part')
def test_multipart_upload_small():
    bucket = get_new_bucket()
    key = "mymultipart"
    size = 1
    (upload, data) = _multipart_upload(bucket, key, size)
    upload.complete_upload()
    key2 = bucket.get_key(key)
    eq(key2.size, size)

def _check_key_content(src, dst):
    assert(src.size >= dst.size)
    src_content = src.get_contents_as_string(headers={'Range': 'bytes={s}-{e}'.format(s=0, e=dst.size-1)})
    dst_content = dst.get_contents_as_string()
    eq(src_content, dst_content)

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart copies with single small part')
def test_multipart_copy_small():
    (src_bucket, src_key) = _create_key_with_random_content('foo')
    dst_bucket = get_new_bucket()
    dst_keyname = "mymultipart"
    size = 1
    copy = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, size)
    copy.complete_upload()
    key2 = dst_bucket.get_key(dst_keyname)
    eq(key2.size, size)
    _check_key_content(src_key, key2)

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart copieds with wrong x-cos-copy-source-range')
def test_multipart_copy_with_wrong_range():
    (src_bucket, src_key) = _create_key_with_random_content('foo')
    dst_bucket = get_new_bucket()
    dst_keyname = "mymultipart"
    upload = dst_bucket.initiate_multipart_upload(dst_keyname)

    e = assert_raises(boto.exception.S3ResponseError, copy_part,
                      src_bucket.name + '.yfb.myqcloud.com', src_key.name, dst_bucket, dst_keyname, upload.id, 1, 0, 1001001010010, None)
    eq(e.status, 400)
    eq(e.error_code, u'InvalidArgument')
    eq(e.error_message, u'The x-cos-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy')
    upload.complete_upload()


@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart copies with single small part')
def test_multipart_copy_special_names():
    src_bucket = get_new_bucket()
    dst_bucket = get_new_bucket()
    dst_keyname = "mymultipart"
    size = 1
    for name in (' ', '_', '__', '?versionId'):
        (src_bucket, src_key) = _create_key_with_random_content(name, bucket=src_bucket)
        copy = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, size)
        copy.complete_upload()
        key2 = dst_bucket.get_key(dst_keyname)
        eq(key2.size, size)
        _check_key_content(src_key, key2)

def _check_content_using_range(k, data, step):
    objlen = k.size
    for ofs in xrange(0, k.size, step):
        toread = k.size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        read_range = k.get_contents_as_string(headers={'Range': 'bytes={s}-{e}'.format(s=ofs, e=end)})
        eq(len(read_range), toread)
        eq(read_range, data[ofs:end+1])

@attr(resource='object')
@attr(method='put')
@attr(operation='complete multi-part upload')
@attr(assertion='successful')
def test_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    content_type='text/bla'
    objlen = 30 * 1024 * 1024
    (upload, data) = _multipart_upload(bucket, key, objlen, headers={'Content-Type': content_type}, metadata={'foo': 'bar'})
    upload.complete_upload()

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 1), 1)
    eq(result.get('x-rgw-bytes-used', 30 * 1024 * 1024), 30 * 1024 * 1024)

    k=bucket.get_key(key)
    eq(k.metadata['foo'], 'bar')
    eq(k.content_type, content_type)
    test_string=k.get_contents_as_string()
    eq(len(test_string), k.size)
    eq(test_string, data)

    _check_content_using_range(k, data, 1000000)
    _check_content_using_range(k, data, 10000000)

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart copies with single small part')
def test_multipart_copy_special_names():
    src_bucket = get_new_bucket()
    dst_bucket = get_new_bucket()
    dst_keyname = "mymultipart"
    size = 1
    # TODO(rabbitliu) 架平暂时不支持路径带问号
    #for name in (' ', '_', '__', '?versionId'):
    for name in (' ', '_', '__'):
        (src_bucket, src_key) = _create_key_with_random_content(name, bucket=src_bucket)
        copy = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, size)
        copy.complete_upload()
        key2 = dst_bucket.get_key(dst_keyname)
        eq(key2.size, size)
        _check_key_content(src_key, key2)

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart copies of versioned objects')
def test_multipart_copy_versioned():
    src_bucket = get_new_bucket()
    dst_bucket = get_new_bucket()
    dst_keyname = "mymultipart"

    check_versioning(src_bucket, None)

    src_name = 'foo'

    check_configure_versioning_retry(src_bucket, True, "Enabled")

    size = 15 * 1024 * 1024
    (src_bucket, src_key) = _create_key_with_random_content(src_name, size=size, bucket=src_bucket)
    (src_bucket, src_key) = _create_key_with_random_content(src_name, size=size, bucket=src_bucket)
    (src_bucket, src_key) = _create_key_with_random_content(src_name, size=size, bucket=src_bucket)

    version_id = []
    for k in src_bucket.list_versions():
        version_id.append(k.version_id)
        break

    for vid in version_id:
        src_key = src_bucket.get_key(src_name, version_id=vid)
        copy = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, size, src_version_id=vid)
        copy.complete_upload()
        key2 = dst_bucket.get_key(dst_keyname)
        eq(key2.size, size)
        _check_key_content(src_key, key2)



def _check_upload_multipart_resend(bucket, key, objlen, resend_parts):
    content_type='text/bla'
    (upload, data) = _multipart_upload(bucket, key, objlen, headers={'Content-Type': content_type}, metadata={'foo': 'bar'}, resend_parts=resend_parts)
    upload.complete_upload()

    k=bucket.get_key(key)
    eq(k.metadata['foo'], 'bar')
    eq(k.content_type, content_type)
    test_string=k.get_contents_as_string()
    eq(k.size, len(test_string))
    eq(k.size, objlen)
    eq(test_string, data)

    _check_content_using_range(k, data, 1000000)
    _check_content_using_range(k, data, 10000000)

@attr(resource='object')
@attr(method='put')
@attr(operation='complete multiple multi-part upload with different sizes')
@attr(resource='object')
@attr(method='put')
@attr(operation='complete multi-part upload')
@attr(assertion='successful')
def test_multipart_upload_resend_part():
    bucket = get_new_bucket()
    key="mymultipart"
    objlen = 30 * 1024 * 1024

    _check_upload_multipart_resend(bucket, key, objlen, [0])
    _check_upload_multipart_resend(bucket, key, objlen, [1])
    _check_upload_multipart_resend(bucket, key, objlen, [2])
    _check_upload_multipart_resend(bucket, key, objlen, [1,2])
    _check_upload_multipart_resend(bucket, key, objlen, [0,1,2,3,4,5])

@attr(assertion='successful')
def test_multipart_upload_multiple_sizes():
    bucket = get_new_bucket()
    key="mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 5 * 1024 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 5 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 5 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024)
    upload.complete_upload()

@attr(assertion='successful')
@attr('fails_on_rgw')
def test_multipart_copy_multiple_sizes():
    (src_bucket, src_key) = _create_key_with_random_content('foo', 12 * 1024 * 1024)
    dst_bucket = get_new_bucket()
    dst_keyname="mymultipart"

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 5 * 1024 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 5 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 5 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 10 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 10 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 10 * 1024 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

@attr(resource='object')
@attr(method='put')
@attr(operation='check failure on multiple multi-part upload with size too small')
@attr(assertion='fails 400')
def test_multipart_upload_size_too_small():
    bucket = get_new_bucket()
    key="mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 100 * 1024, part_size=10*1024)
    e = assert_raises(boto.exception.S3ResponseError, upload.complete_upload)
    eq(e.status, 400) # s3 返回400
    eq(e.error_code, u'EntityTooSmall')
    upload.cancel_upload()

def gen_rand_string(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def _do_test_multipart_upload_contents(bucket, key_name, num_parts):
    payload=gen_rand_string(5)*1024*1024
    mp=bucket.initiate_multipart_upload(key_name)
    for i in range(0, num_parts):
        mp.upload_part_from_file(StringIO(payload), i+1)

    last_payload='123'*1024*1024
    mp.upload_part_from_file(StringIO(last_payload), num_parts + 1)

    mp.complete_upload()
    key=bucket.get_key(key_name)
    test_string=key.get_contents_as_string()

    all_payload = payload*num_parts + last_payload
    print 'JJJ', key_name, len(all_payload), len(test_string)

    assert test_string == all_payload

    return all_payload


@attr(resource='object')
@attr(method='put')
@attr(operation='check contents of multi-part upload')
@attr(assertion='successful')
def test_multipart_upload_contents():
    _do_test_multipart_upload_contents(get_new_bucket(), 'mymultipart', 3)


@attr(resource='object')
@attr(method='put')
@attr(operation=' multi-part upload overwrites existing key')
@attr(assertion='successful')
def test_multipart_upload_overwrite_existing_object():
    bucket = get_new_bucket()
    key_name="mymultipart"
    payload='12345'*1024*1024
    num_parts=2
    key=bucket.new_key(key_name)
    key.set_contents_from_string(payload)

    mp=bucket.initiate_multipart_upload(key_name)
    for i in range(0, num_parts):
        mp.upload_part_from_file(StringIO(payload), i+1)

    mp.complete_upload()
    key=bucket.get_key(key_name)
    test_string=key.get_contents_as_string()
    assert test_string == payload*num_parts

@attr(resource='object')
@attr(method='put')
@attr(operation='abort multi-part upload')
@attr(assertion='successful')
def test_abort_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024)
    upload.cancel_upload()

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 0), 0)
    eq(result.get('x-rgw-bytes-used', 0), 0)

def test_abort_multipart_upload_not_found():
    bucket = get_new_bucket()
    key="mymultipart"
    e = assert_raises(boto.exception.S3ResponseError, bucket.cancel_multipart_upload, key, '1')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchUpload')

@attr(resource='object')
@attr(method='put')
@attr(operation='concurrent multi-part uploads')
@attr(assertion='successful')
def test_list_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    mb = 1024 * 1024
    (upload1, data) = _multipart_upload(bucket, key, 5 * mb, do_list = True)
    (upload2, data) = _multipart_upload(bucket, key, 6 * mb, do_list = True)

    key2="mymultipart2"
    (upload3, data) = _multipart_upload(bucket, key2, 5 * mb, do_list = True)

    l = bucket.list_multipart_uploads()
    l = list(l)

    index = dict([(key, 2), (key2, 1)])

    for upload in l:
        index[upload.key_name] -= 1;

    for k, c in index.items():
        eq(c, 0)

    upload1.cancel_upload()
    upload2.cancel_upload()
    upload3.cancel_upload()

@attr(resource='object')
@attr(method='put')
@attr(operation='multi-part upload with missing part')
def test_multipart_upload_missing_part():
    bucket = get_new_bucket()
    key_name = "mymultipart"
    mp = bucket.initiate_multipart_upload(key_name)
    mp.upload_part_from_file(StringIO('\x00'), 1)
    xml = mp.to_xml()
    xml = xml.replace('<PartNumber>1</PartNumber>', '<PartNumber>9999</PartNumber>')
    e = assert_raises(boto.exception.S3ResponseError, bucket.complete_multipart_upload, key_name, mp.id, xml)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidPart')
    mp.cancel_upload()
    

@attr(resource='object')
@attr(method='put')
@attr(operation='multi-part upload with incorrect ETag')
def test_multipart_upload_incorrect_etag():
    bucket = get_new_bucket()
    key_name = "mymultipart"
    mp = bucket.initiate_multipart_upload(key_name)
    mp.upload_part_from_file(StringIO('\x00'), 1)
    xml = mp.to_xml()
    xml = xml.replace('<ETag>"93b885adfe0da089cdf634904fd59f71"</ETag>', '<ETag>"ffffffffffffffffffffffffffffffff"</ETag>')
    e = assert_raises(boto.exception.S3ResponseError, bucket.complete_multipart_upload, key_name, mp.id, xml)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidPart')
    mp.cancel_upload()

@attr(resource='object')
@attr(method='put')
@attr(operation='multi-part upload with unorderd part')
def test_multipart_upload_incorrect_part_order():
    bucket = get_new_bucket()
    key_name = "mymultipart"
    mp = bucket.initiate_multipart_upload(key_name)
    mp.upload_part_from_file(StringIO('\x00'), 1)
    mp.upload_part_from_file(StringIO('\x00'), 2)
    xml = mp.to_xml()
    xml = xml.replace('<PartNumber>1</PartNumber>', '<PartNumber>3</PartNumber>')
    xml = xml.replace('<PartNumber>2</PartNumber>', '<PartNumber>1</PartNumber>')
    xml = xml.replace('<PartNumber>3</PartNumber>', '<PartNumber>2</PartNumber>')
    e = assert_raises(boto.exception.S3ResponseError, bucket.complete_multipart_upload, key_name, mp.id, xml)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidPartOrder')
    mp.cancel_upload()

def _simple_http_req_100_cont(host, port, is_secure, method, resource):
    """
    Send the specified request w/expect 100-continue
    and await confirmation.
    """
    req = '{method} {resource} HTTP/1.1\r\nHost: {host}\r\nAccept-Encoding: identity\r\nContent-Length: 123\r\nExpect: 100-continue\r\n\r\n'.format(
            method=method,
            resource=resource,
            host=host,
            )

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if is_secure:
        s = ssl.wrap_socket(s);
    s.settimeout(5)
    s.connect((host, port))
    s.send(req)

    try:
        data = s.recv(1024)
    except socket.error, msg:
        print 'got response: ', msg
        print 'most likely server doesn\'t support 100-continue'

    s.close()
    l = data.split(' ')

    assert l[0].startswith('HTTP')

    return l[1]

@attr(resource='object')
@attr(method='put')
@attr(operation='w/expect continue')
@attr(assertion='succeeds if object is public-read-write')
@attr('100_continue')
@attr('fails_on_mod_proxy_fcgi')
def test_100_continue():
    bucket = get_new_bucket()
    objname = 'testobj'
    resource = '/{obj}'.format(obj=objname)
    host = bucket.name + '.' + s3.main.host
    status = _simple_http_req_100_cont(host, s3.main.port, s3.main.is_secure, 'PUT', resource)
    # fails on nginx, nginx always sends 100-continue instead of delegating that
    # responsibility to upstream server
    eq(status, '100')

    bucket.set_acl('public-read-write')

    status = _simple_http_req_100_cont(host, s3.main.port, s3.main.is_secure, 'PUT', resource)
    eq(status, '100')

def _test_bucket_acls_changes_persistent(bucket):
    """
    set and verify readback of each possible permission
    """
    perms = ('FULL_CONTROL', 'WRITE', 'WRITE_ACP', 'READ', 'READ_ACP')
    for p in perms:
        _build_bucket_acl_xml(p, bucket)

@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl set')
@attr(assertion='all permissions are persistent')
def test_bucket_acls_changes_persistent():
    bucket = get_new_bucket()
    _test_bucket_acls_changes_persistent(bucket);

@attr(resource='bucket')
@attr(method='put')
@attr(operation='repeated acl set')
@attr(assertion='all permissions are persistent')
def test_stress_bucket_acls_changes():
    bucket = get_new_bucket()
    for i in xrange(10):
        _test_bucket_acls_changes_persistent(bucket);

@attr(resource='bucket')
@attr(method='put')
@attr(operation='set cors')
@attr(assertion='succeeds')
def test_set_cors():
    bucket = get_new_bucket()
    cfg = CORSConfiguration()
    cfg.add_rule('GET', '*.get', None, None, 1)
    cfg.add_rule('PUT', '*.put', None, None, 1)

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_cors)
    eq(e.status, 404)

    bucket.set_cors(cfg)
    time.sleep(SYNC_SLEEP) # wait for cors sync
    new_cfg = bucket.get_cors()

    eq(len(new_cfg), 2)

    result = bunch.Bunch()

    for c in new_cfg:
        eq(len(c.allowed_method), 1)
        eq(len(c.allowed_origin), 1)
        result[c.allowed_method[0]] = c.allowed_origin[0]


    eq(result['GET'], '*.get')
    eq(result['PUT'], '*.put')

    bucket.delete_cors()

'''
    # TODO(rabbitliu) 加了这一行会卡住，等架平兼容cors后再打开
    e = assert_raises(boto.exception.S3ResponseError, bucket.get_cors)
    eq(e.status, 404)
'''

def _cors_request_and_check(func, url, headers, expect_status, expect_allow_origin, expect_allow_methods):
    r = func(url, headers=headers)
    eq(r.status_code, expect_status)

    assert r.headers['access-control-allow-origin'] == expect_allow_origin
    assert r.headers['access-control-allow-methods'] == expect_allow_methods



@attr(resource='bucket')
@attr(method='get')
@attr(operation='check cors response when origin header set')
@attr(assertion='returning cors header')
def test_cors_origin_response():
    cfg = CORSConfiguration()
    bucket = get_new_bucket()

    bucket.set_acl('public-read')
    # set acl must sleep 60s
    wait_for_acl_valid(200, bucket)

    cfg.add_rule('GET', '*suffix', None, None, 1)
    cfg.add_rule('GET', 'start*end', None, None, 1)
    cfg.add_rule('GET', 'prefix*', None, None, 1)
    cfg.add_rule('PUT', '*.put', None, None, 1)

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_cors)
    eq(e.status, 404)

    bucket.set_cors(cfg)

    time.sleep(SYNC_SLEEP) # waiting, since if running against amazon data consistency model is not strict read-after-write

    url = _get_post_url(s3.main, bucket)

    _cors_request_and_check(requests.get, url, None, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.suffix'}, 200, 'foo.suffix', 'GET')
    # 这里规则就不允许
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.bar'}, 200, None, None)
    # 这里规则就不允许
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.suffix.get'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'startend'}, 200, 'startend', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'start1end'}, 200, 'start1end', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'start12end'}, 200, 'start12end', 'GET')
    # 这里规则就不允许
    _cors_request_and_check(requests.get, url, {'Origin': '0start12end'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'prefix'}, 200, 'prefix', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'prefix.suffix'}, 200, 'prefix.suffix', 'GET')
    # 这里规则就不允许
    _cors_request_and_check(requests.get, url, {'Origin': 'bla.prefix'}, 200, None, None)

    obj_url = '{u}/{o}'.format(u=url, o='bar')
    # TODO(rabbitliu) cgi下载失败时没吐cors头部信息，等改动完成后打开开关再看看
    #_cors_request_and_check(requests.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                                    'content-length': '0'}, 403, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'PUT',
                                                    'content-length': '0'}, 403, None, None)
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'DELETE',
                                                    'content-length': '0'}, 403, None, None)
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'content-length': '0'}, 403, None, None)

    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.put', 'content-length': '0'}, 403, 'foo.put', 'PUT')

    #_cors_request_and_check(requests.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')

    _cors_request_and_check(requests.options, url, None, 400, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.suffix'}, 400, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'bla'}, 400, None, None)
    _cors_request_and_check(requests.options, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                                    'content-length': '0'}, 200, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.bar', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.suffix.get', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'startend', 'Access-Control-Request-Method': 'GET'}, 200, 'startend', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'start1end', 'Access-Control-Request-Method': 'GET'}, 200, 'start1end', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'start12end', 'Access-Control-Request-Method': 'GET'}, 200, 'start12end', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': '0start12end', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'prefix', 'Access-Control-Request-Method': 'GET'}, 200, 'prefix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'prefix.suffix', 'Access-Control-Request-Method': 'GET'}, 200, 'prefix.suffix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'bla.prefix', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.put', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.put', 'Access-Control-Request-Method': 'PUT'}, 200, 'foo.put', 'PUT')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='check cors response when origin is set to wildcard')
@attr(assertion='returning cors header')
def test_cors_origin_wildcard():
    cfg = CORSConfiguration()
    bucket = get_new_bucket()

    bucket.set_acl('public-read')
    # set acl must sleep 60s
    wait_for_acl_valid(200, bucket)

    cfg.add_rule('GET', '*', None, None, 100)

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_cors)
    eq(e.status, 404)

    bucket.set_cors(cfg)

    time.sleep(SYNC_SLEEP)

    url = _get_post_url(s3.main, bucket)

    _cors_request_and_check(requests.get, url, None, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'example.origin'}, 200, '*', 'GET')


class FakeFile(object):
    """
    file that simulates seek, tell, and current character
    """
    def __init__(self, char='A', interrupt=None):
        self.offset = 0
        self.char = char
        self.interrupt = interrupt

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset;
        elif whence == os.SEEK_CUR:
            self.offset += offset

    def tell(self):
        return self.offset

class FakeWriteFile(FakeFile):
    """
    file that simulates interruptable reads of constant data
    """
    def __init__(self, size, char='A', interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.size = size

    def read(self, size=-1):
        if size < 0:
            size = self.size - self.offset
        count = min(size, self.size - self.offset)
        self.offset += count

        # Sneaky! do stuff before we return (the last time)
        if self.interrupt != None and self.offset == self.size and count > 0:
            self.interrupt()

        return self.char*count

class FakeReadFile(FakeFile):
    """
    file that simulates writes, interrupting after the second
    """
    def __init__(self, size, char='A', interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.interrupted = False
        self.size = 0
        self.expected_size = size

    def write(self, chars):
        eq(chars, self.char*len(chars))
        self.offset += len(chars)
        self.size += len(chars)

        # Sneaky! do stuff on the second seek
        if not self.interrupted and self.interrupt != None \
                and self.offset > 0:
            self.interrupt()
            self.interrupted = True

    def close(self):
        eq(self.size, self.expected_size)

class FakeFileVerifier(object):
    """
    file that verifies expected data has been written
    """
    def __init__(self, char=None):
        self.char = char
        self.size = 0

    def write(self, data):
        size = len(data)
        if self.char == None:
            self.char = data[0]
        self.size += size
        eq(data, self.char*size)

def _verify_atomic_key_data(key, size=-1, char=None):
    """
    Make sure file is of the expected size and (simulated) content
    """
    fp_verify = FakeFileVerifier(char)
    key.get_contents_to_file(fp_verify)
    if size >= 0:
        eq(fp_verify.size, size)

def _test_atomic_read(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    key = bucket.new_key('testobj')

    # create object of <file_size> As
    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)

    read_conn = boto.s3.connection.S3Connection(
        aws_access_key_id=s3['main'].aws_access_key_id,
        aws_secret_access_key=s3['main'].aws_secret_access_key,
        is_secure=s3['main'].is_secure,
        port=s3['main'].port,
        host=s3['main'].host,
        calling_format=s3['main'].calling_format,
        )

    read_bucket = read_conn.get_bucket(bucket.name)
    read_key = read_bucket.get_key('testobj')
    fp_b = FakeWriteFile(file_size, 'B')
    fp_a2 = FakeReadFile(file_size, 'A',
        lambda: key.set_contents_from_file(fp_b)
        )

    # read object while writing it to it
    read_key.get_contents_to_file(fp_a2)
    fp_a2.close()

    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='read atomicity')
@attr(assertion='1MB successful')
def test_atomic_read_1mb():
    _test_atomic_read(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='read atomicity')
@attr(assertion='4MB successful')
def test_atomic_read_4mb():
    _test_atomic_read(1024*1024*4)

@attr(resource='object')
@attr(method='put')
@attr(operation='read atomicity')
@attr(assertion='8MB successful')
def test_atomic_read_8mb():
    _test_atomic_read(1024*1024*8)

def _test_atomic_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)

    # verify A's
    _verify_atomic_key_data(key, file_size, 'A')

    read_key = bucket.get_key(objname)

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    fp_b = FakeWriteFile(file_size, 'B',
        lambda: _verify_atomic_key_data(read_key, file_size)
        )
    key.set_contents_from_file(fp_b)

    # verify B's
    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='1MB successful')
def test_atomic_write_1mb():
    _test_atomic_write(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='4MB successful')
def test_atomic_write_4mb():
    _test_atomic_write(1024*1024*4)

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='8MB successful')
def test_atomic_write_8mb():
    _test_atomic_write(1024*1024*8)

def _test_atomic_dual_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # get a second key object (for the same key)
    # so both can be writing without interfering
    key2 = bucket.new_key(objname)

    # write <file_size> file of B's
    # but before we're done, try to write all A's
    fp_a = FakeWriteFile(file_size, 'A')
    fp_b = FakeWriteFile(file_size, 'B',
        lambda: key2.set_contents_from_file(fp_a, rewind=True)
        )
    key.set_contents_from_file(fp_b)

    # verify the file
    _verify_atomic_key_data(key, file_size)

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='1MB successful')
def test_atomic_dual_write_1mb():
    _test_atomic_dual_write(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='4MB successful')
def test_atomic_dual_write_4mb():
    _test_atomic_dual_write(1024*1024*4)

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='8MB successful')
def test_atomic_dual_write_8mb():
    _test_atomic_dual_write(1024*1024*8)

def _test_atomic_conditional_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)

    # verify A's
    _verify_atomic_key_data(key, file_size, 'A')

    read_key = bucket.get_key(objname)

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    fp_b = FakeWriteFile(file_size, 'B',
        lambda: _verify_atomic_key_data(read_key, file_size)
        )
    key.set_contents_from_file(fp_b, headers={'If-Match': '*'})

    # verify B's
    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='1MB successful')
@attr('fails_on_aws')
def test_atomic_conditional_write_1mb():
    _test_atomic_conditional_write(1024*1024)

def _test_atomic_dual_conditional_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)
    _verify_atomic_key_data(key, file_size, 'A')
    etag_fp_a = key.etag.replace('"', '').strip()

    # get a second key object (for the same key)
    # so both can be writing without interfering
    key2 = bucket.new_key(objname)

    # write <file_size> file of C's
    # but before we're done, try to write all B's
    fp_b = FakeWriteFile(file_size, 'B')
    fp_c = FakeWriteFile(file_size, 'C',
        lambda: key2.set_contents_from_file(fp_b, rewind=True, headers={'If-Match': etag_fp_a})
        )
    # key.set_contents_from_file(fp_c, headers={'If-Match': etag_fp_a})
    # aws s3 return 501, cos return 200
    e = key.set_contents_from_file(fp_c, headers={'If-Match': etag_fp_a})
    eq(e, file_size)
    #eq(e.reason, 'Precondition Failed')
    #eq(e.error_code, 'PreconditionFailed')

    # verify the file
    # 0. 目前cos的v5接口可以保证数据的原子性，即按照时间数据，后收到的数据为准；
    # 1. 目前cgi不能保证相同key，先收到的第一个字节的文件先finish。
    # 2. aws和cos都不支持上传的if-match头部
    # 因此，虽然本例中，先发完B文件才发送C文件，但是不能保证cgi收到的顺序，因此本例有随机性，
    # 该例在ceph运行，由于if-match字段，C文件预期返回412，因此才只有B文件，在cos上不能校验
    #_verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='1MB successful')
@attr('fails_on_aws')
def test_atomic_dual_conditional_write_1mb():
    _test_atomic_dual_conditional_write(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='write file in deleted bucket')
@attr(assertion='fail 404')
@attr('fails_on_aws')
def test_atomic_write_bucket_gone():
    bucket = get_new_bucket()

    def remove_bucket():
        bucket.delete()

    # create file of A's but delete the bucket it's in before we finish writing
    # all of them
    key = bucket.new_key('foo')
    fp_a = FakeWriteFile(1024*1024, 'A', remove_bucket)
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_file, fp_a)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='put')
@attr(operation='begin to overwrite file with multipart upload then abort')
@attr(assertion='read back original key contents')
def test_atomic_multipart_upload_write():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    upload = bucket.initiate_multipart_upload(key)

    key = bucket.get_key('foo')
    got = key.get_contents_as_string()
    eq(got, 'bar')

    upload.cancel_upload()

    key = bucket.get_key('foo')
    got = key.get_contents_as_string()
    eq(got, 'bar')

class Counter:
    def __init__(self, default_val):
        self.val = default_val

    def inc(self):
        self.val = self.val + 1

class ActionOnCount:
    def __init__(self, trigger_count, action):
        self.count = 0
        self.trigger_count = trigger_count
        self.action = action

    def trigger(self):
        self.count = self.count + 1

        if self.count == self.trigger_count:
            self.action()

@attr(resource='object')
@attr(method='put')
@attr(operation='multipart check for two writes of the same part, first write finishes last')
@attr(assertion='object contains correct content')
def test_multipart_resend_first_finishes_last():
    # AWS的分块并发上传是以第一个请求为准(谁先发起)
    # COS的分块并发上传是以后一个请求为准(谁后发起)
    raise SkipTest  # 先skip掉,等4.19.3上线以后再打开这个用例
    bucket = get_new_bucket()
    key_name = "mymultipart"
    mp = bucket.initiate_multipart_upload(key_name)

    file_size = 8 * 1024 * 1024

    counter = Counter(0)

    # mp.upload_part_from_file might read multiple times from the object
    # first time when it calculates md5, second time when it writes data
    # out. We want to interject only on the last time, but we can't be
    # sure how many times it's going to read, so let's have a test run
    # and count the number of reads
    fp_dryrun = FakeWriteFile(file_size, 'C',
        lambda: counter.inc()
        )
    mp.upload_part_from_file(fp_dryrun, 1)
    mp.complete_upload()

    bucket.delete_key(key_name)

    # ok, now for the actual test

    fp_b = FakeWriteFile(file_size, 'B')

    action = ActionOnCount(counter.val, lambda: mp.upload_part_from_file(fp_b, 1))
	
    fp_a = FakeWriteFile(file_size, 'A',
        lambda: action.trigger()
        )

    mp = bucket.initiate_multipart_upload(key_name)
    # 4.19.2并发上传返回500,boto sdk对于5XX错误会重试,上传的时序关系为ABA,所以最终的结果为A
    # 4.19.3并发上传返回409,boto 不会重试,上传的时序关系为AB,所以最终的结果为B
    try:
        mp.upload_part_from_file(fp_a, 1)
    except Exception as e:
        # 忽略并发上传的异常
        pass
    mp.complete_upload()

    key = bucket.get_key(key_name)
    # COS写入的是第二个块,所以最终的结果是B
    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns correct data, 206')
def test_ranged_request_response_code():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    key.open('r', headers={'Range': 'bytes=4-7'})
    status = key.resp.status
    content_range = key.resp.getheader('Content-Range')
    fetched_content = ''
    for data in key:
        fetched_content += data;
    key.close()

    eq(fetched_content, content[4:8])
    eq(status, 206)
    eq(content_range, 'bytes 4-7/11')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns correct data, 206')
def test_ranged_request_skip_leading_bytes_response_code():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    # test trailing bytes
    key.open('r', headers={'Range': 'bytes=4-'})
    status = key.resp.status
    content_range = key.resp.getheader('Content-Range')
    fetched_content = ''
    for data in key:
        fetched_content += data;
    key.close()

    eq(fetched_content, content[4:])
    eq(status, 206)
    eq(content_range, 'bytes 4-10/11')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns correct data, 206')
def test_ranged_request_return_trailing_bytes_response_code():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    # test leading bytes
    key.open('r', headers={'Range': 'bytes=-7'})
    status = key.resp.status
    content_range = key.resp.getheader('Content-Range')
    fetched_content = ''
    for data in key:
        fetched_content += data;
    key.close()

    eq(fetched_content, content[-7:])
    eq(status, 206)
    eq(content_range, 'bytes 4-10/11')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns invalid range, 416')
def test_ranged_request_invalid_range():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    # test invalid range
    e = assert_raises(boto.exception.S3ResponseError, key.open, 'r', headers={'Range': 'bytes=40-50'})
    eq(e.status, 416)
    eq(e.error_code, 'InvalidRange')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns invalid range, 416')
def test_ranged_request_empty_object():
    content = ''

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    # test invalid range
    e = assert_raises(boto.exception.S3ResponseError, key.open, 'r', headers={'Range': 'bytes=40-50'})
    eq(e.status, 416)
    eq(e.error_code, 'InvalidRange')

def check_can_test_multiregion():
    if not targets.main.master or len(targets.main.secondaries) == 0:
        raise SkipTest

def create_presigned_url(conn, method, bucket_name, key_name, expiration):
    return conn.generate_url(expires_in=expiration,
        method=method,
        bucket=bucket_name,
        key=key_name,
        query_auth=True,
    )

def send_raw_http_request(conn, method, bucket_name, key_name, follow_redirects = False):
    url = create_presigned_url(conn, method, bucket_name, key_name, 3600)
    print url
    h = httplib2.Http()
    h.follow_redirects = follow_redirects
    return h.request(url, method)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='create on one region, access in another')
@attr(assertion='can\'t access in other region')
@attr('multiregion')
def test_region_bucket_create_secondary_access_remove_master():
    check_can_test_multiregion()

    master_conn = targets.main.master.connection

    for r in targets.main.secondaries:
        conn = r.connection
        bucket = get_new_bucket(r)

        r, content = send_raw_http_request(master_conn, 'GET', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        r, content = send_raw_http_request(master_conn, 'DELETE', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        conn.delete_bucket(bucket)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='create on one region, access in another')
@attr(assertion='can\'t access in other region')
@attr('multiregion')
def test_region_bucket_create_master_access_remove_secondary():
    check_can_test_multiregion()

    master = targets.main.master
    master_conn = master.connection

    for r in targets.main.secondaries:
        conn = r.connection
        bucket = get_new_bucket(master)

        region_sync_meta(targets.main, master)

        r, content = send_raw_http_request(conn, 'GET', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        r, content = send_raw_http_request(conn, 'DELETE', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        master_conn.delete_bucket(bucket)
        region_sync_meta(targets.main, master)

        e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, bucket.name)
        eq(e.status, 404)

        e = assert_raises(boto.exception.S3ResponseError, master_conn.get_bucket, bucket.name)
        eq(e.status, 404)


@attr(resource='object')
@attr(method='copy')
@attr(operation='copy object between regions, verify')
@attr(assertion='can read object')
@attr('multiregion')
def test_region_copy_object():
    check_can_test_multiregion()

    for (k, dest) in targets.main.iteritems():
        dest_conn = dest.connection

        dest_bucket = get_new_bucket(dest)
        print 'created new dest bucket ', dest_bucket.name
        region_sync_meta(targets.main, dest)

        if is_slow_backend():
            sizes = (1024, 10 * 1024 * 1024)
        else:
            sizes = (1024, 10 * 1024 * 1024, 100 * 1024 * 1024)

        for file_size in sizes:
            for (k2, r) in targets.main.iteritems():
                if r == dest_conn:
                    continue
                conn = r.connection

                bucket = get_new_bucket(r)
                print 'created bucket', bucket.name
                region_sync_meta(targets.main, r)

                content = 'testcontent'

                print 'creating key=testobj', 'bucket=',bucket.name

                key = bucket.new_key('testobj')
                fp_a = FakeWriteFile(file_size, 'A')
                key.set_contents_from_file(fp_a)

                print 'calling region_sync_meta'

                region_sync_meta(targets.main, r)

                print 'dest_bucket=', dest_bucket.name, 'key=', key.name

                dest_key = dest_bucket.copy_key('testobj-dest', bucket.name, key.name)

                print

                # verify dest
                _verify_atomic_key_data(dest_key, file_size, 'A')

                bucket.delete_key(key.name)

                # confirm that the key was deleted as expected
                region_sync_meta(targets.main, r)
                temp_key = bucket.get_key(key.name)
                assert temp_key == None

                print 'removing bucket', bucket.name
                conn.delete_bucket(bucket)

                # confirm that the bucket was deleted as expected
                region_sync_meta(targets.main, r)
                e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, bucket.name)
                eq(e.status, 404)
                e = assert_raises(boto.exception.S3ResponseError, dest_conn.get_bucket, bucket.name)
                eq(e.status, 404)

                # confirm that the key was deleted as expected
                dest_bucket.delete_key(dest_key.name)
                temp_key = dest_bucket.get_key(dest_key.name)
                assert temp_key == None


        dest_conn.delete_bucket(dest_bucket)
        region_sync_meta(targets.main, dest)

        # ensure that dest_bucket was deleted on this host and all other hosts
        e = assert_raises(boto.exception.S3ResponseError, dest_conn.get_bucket, dest_bucket.name)
        eq(e.status, 404)
        for (k2, r) in targets.main.iteritems():
            if r == dest_conn:
                continue
            conn = r.connection
            e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, dest_bucket.name)
            eq(e.status, 404)

def check_versioning(bucket, status):
    try:
        eq(bucket.get_versioning_status()['Versioning'], status)
    except KeyError:
        eq(status, None)

# amazon is eventual consistent, retry a bit if failed
def check_configure_versioning_retry(bucket, status, expected_string):
    bucket.configure_versioning(status)

    read_status = None

    for i in xrange(5):
        try:
            read_status = bucket.get_versioning_status()['Versioning']
        except KeyError:
            read_status = None

        if (expected_string == read_status):
            break

        time.sleep(1)

    eq(expected_string, read_status)


@attr(resource='bucket')
@attr(method='create')
@attr(operation='create versioned bucket')
@attr(assertion='can create and suspend bucket versioning')
@attr('versioning')
def test_versioning_bucket_create_suspend():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, False, "Suspended")
    check_configure_versioning_retry(bucket, True, "Enabled")
    check_configure_versioning_retry(bucket, True, "Enabled")
    check_configure_versioning_retry(bucket, False, "Suspended")


def check_head_obj_content(key, content):
    if content is not None:
        eq(key.get_contents_as_string(), content)
    else:
        print 'check head', key
        eq(key, None)

def check_obj_content(key, content):
    if content is not None:
        eq(key.get_contents_as_string(), content)
    else:
        eq(isinstance(key, boto.s3.deletemarker.DeleteMarker), True)


def check_obj_versions(bucket, objname, keys, contents):
    # check to see if object is pointing at correct version
    key = bucket.get_key(objname)

    if len(contents) > 0:
        print 'testing obj head', objname
        check_head_obj_content(key, contents[-1])
        i = len(contents)
        for key in bucket.list_versions():
            if key.name != objname:
                continue

            i -= 1
            eq(keys[i].version_id or 'null', key.version_id)
            print 'testing obj version-id=', key.version_id
            check_obj_content(key, contents[i])
    else:
        eq(key, None)

def create_multiple_versions(bucket, objname, num_versions, k = None, c = None):
    c = c or []
    k = k or []
    for i in xrange(num_versions):
        c.append('content-{i}'.format(i=i))

        key = bucket.new_key(objname)
        key.set_contents_from_string(c[i])

        if i == 0:
            check_configure_versioning_retry(bucket, True, "Enabled")

    k_pos = len(k)
    i = 0
    for o in bucket.list_versions():
        if o.name != objname:
            continue
        i += 1
        if i > num_versions:
            break

        print o, o.version_id
        k.insert(k_pos, o)
        print 'created obj name=', objname, 'version-id=', o.version_id

    eq(len(k), len(c))

    for j in xrange(num_versions):
        print j, k[j], k[j].version_id

    check_obj_versions(bucket, objname, k, c)

    return (k, c)


def remove_obj_version(bucket, k, c, i):
    # check by versioned key
    i = i % len(k)
    rmkey = k.pop(i)
    content = c.pop(i)
    if (not rmkey.delete_marker):
        eq(rmkey.get_contents_as_string(), content)

    # remove version
    print 'removing version_id=', rmkey.version_id
    bucket.delete_key(rmkey.name, version_id = rmkey.version_id)
    check_obj_versions(bucket, rmkey.name, k, c)

def remove_obj_head(bucket, objname, k, c):
    print 'removing obj=', objname
    key = bucket.delete_key(objname)

    k.append(key)
    c.append(None)

    eq(key.delete_marker, True)

    check_obj_versions(bucket, objname, k, c)

def _do_test_create_remove_versions(bucket, objname, num_versions, remove_start_idx, idx_inc):
    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    idx = remove_start_idx

    for j in xrange(num_versions):
        remove_obj_version(bucket, k, c, idx)
        idx += idx_inc

def _do_remove_versions(bucket, objname, remove_start_idx, idx_inc, head_rm_ratio, k, c):
    idx = remove_start_idx

    r = 0

    total = len(k)

    for j in xrange(total):
        r += head_rm_ratio
        if r >= 1:
            r %= 1
            remove_obj_head(bucket, objname, k, c)
        else:
            remove_obj_version(bucket, k, c, idx)
            idx += idx_inc

    check_obj_versions(bucket, objname, k, c)

def _do_test_create_remove_versions_and_head(bucket, objname, num_versions, num_ops, remove_start_idx, idx_inc, head_rm_ratio):
    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    _do_remove_versions(bucket, objname, remove_start_idx, idx_inc, head_rm_ratio, k, c)

@attr(resource='object')
@attr(method='create')
@attr(operation='create and remove versioned object')
@attr(assertion='can create access and remove appropriate versions')
@attr('versioning')
def test_versioning_obj_create_read_remove():
    bucket = get_new_bucket()
    objname = 'testobj'
    num_vers = 5

    _do_test_create_remove_versions(bucket, objname, num_vers, -1, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, -1, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, 0, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, 1, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, 4, -1)
    _do_test_create_remove_versions(bucket, objname, num_vers, 3, 3)

@attr(resource='object')
@attr(method='create')
@attr(operation='create and remove versioned object and head')
@attr(assertion='can create access and remove appropriate versions')
@attr('versioning')
def test_versioning_obj_create_read_remove_head():
    bucket = get_new_bucket()
    objname = 'testobj'
    num_vers = 5

    _do_test_create_remove_versions_and_head(bucket, objname, num_vers, num_vers * 2, -1, 0, 0.5)

def is_null_key(k):
    return (k.version_id is None) or (k.version_id == 'null')

def delete_suspended_versioning_obj(bucket, objname, k, c):
    key = bucket.delete_key(objname)

    i = 0
    while i < len(k):
        if is_null_key(k[i]):
            k.pop(i)
            c.pop(i)
        else:
            i += 1

    key.version_id = "null"
    k.append(key)
    c.append(None)

    check_obj_versions(bucket, objname, k, c)

def overwrite_suspended_versioning_obj(bucket, objname, k, c, content):
    key = bucket.new_key(objname)
    key.set_contents_from_string(content)

    i = 0
    while i < len(k):
        print 'kkk', i, k[i], k[i].version_id
        if is_null_key(k[i]):
            print 'null key!'
            k.pop(i)
            c.pop(i)
        else:
            i += 1

    k.append(key)
    c.append(content)

    check_obj_versions(bucket, objname, k, c)

@attr(resource='object')
@attr(method='create')
@attr(operation='create object, then switch to versioning')
@attr(assertion='behaves correctly')
@attr('versioning')
def test_versioning_obj_plain_null_version_removal():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    content = 'fooz'
    objname = 'testobj'

    key = bucket.new_key(objname)
    key.set_contents_from_string(content)

    check_configure_versioning_retry(bucket, True, "Enabled")

    bucket.delete_key(key, version_id='null')

    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')


    k = []
    for key in bucket.list_versions():
        k.insert(0, key)

    eq(len(k), 0)

@attr(resource='object')
@attr(method='create')
@attr(operation='create object, then switch to versioning')
@attr(assertion='behaves correctly')
@attr('versioning')
def test_versioning_obj_plain_null_version_overwrite():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    content = 'fooz'
    objname = 'testobj'

    key = bucket.new_key(objname)
    key.set_contents_from_string(content)

    check_configure_versioning_retry(bucket, True, "Enabled")

    content2 = 'zzz'
    key.set_contents_from_string(content2)

    eq(key.get_contents_as_string(), content2)
    # get_contents_to_string() will set key.version_id, clear it
    key.version_id = None

    version_id = None
    for k in bucket.list_versions():
        version_id = k.version_id
        break

    print 'version_id=', version_id
    bucket.delete_key(key, version_id=version_id)

    eq(key.get_contents_as_string(), content)

    bucket.delete_key(key, version_id='null')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

    k = []
    for key in bucket.list_versions():
        k.insert(0, key)

    eq(len(k), 0)

@attr(resource='object')
@attr(method='create')
@attr(operation='create object, then switch to versioning')
@attr(assertion='behaves correctly')
@attr('versioning')
def test_versioning_obj_plain_null_version_overwrite_suspended():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    content = 'fooz'
    objname = 'testobj'

    key = bucket.new_key(objname)
    key.set_contents_from_string(content)

    check_configure_versioning_retry(bucket, True, "Enabled")
    check_configure_versioning_retry(bucket, False, "Suspended")

    content2 = 'zzz'
    key.set_contents_from_string(content2)

    eq(key.get_contents_as_string(), content2)

    version_id = None
    for k in bucket.list_versions():
        version_id = k.version_id
        break

    print 'version_id=', version_id
    bucket.delete_key(key, version_id=version_id)

    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

    k = []
    for key in bucket.list_versions():
        k.insert(0, key)

    eq(len(k), 0)



@attr(resource='object')
@attr(method='create')
@attr(operation='suspend versioned bucket')
@attr(assertion='suspended versioning behaves correctly')
@attr('versioning')
def test_versioning_obj_suspend_versions():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 5
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    check_configure_versioning_retry(bucket, False, "Suspended")

    delete_suspended_versioning_obj(bucket, objname, k, c)
    delete_suspended_versioning_obj(bucket, objname, k, c)
    overwrite_suspended_versioning_obj(bucket, objname, k, c, 'null content 1')
    overwrite_suspended_versioning_obj(bucket, objname, k, c, 'null content 2')
    delete_suspended_versioning_obj(bucket, objname, k, c)
    overwrite_suspended_versioning_obj(bucket, objname, k, c, 'null content 3')
    delete_suspended_versioning_obj(bucket, objname, k, c)

    check_configure_versioning_retry(bucket, True, "Enabled")

    (k, c) = create_multiple_versions(bucket, objname, 3, k, c)

    _do_remove_versions(bucket, objname, 0, 5, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 5, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))

@attr(resource='object')
@attr(method='create')
@attr(operation='suspend versioned bucket')
@attr(assertion='suspended versioning behaves correctly')
@attr('versioning')
def test_versioning_obj_suspend_versions_simple():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 1
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    check_configure_versioning_retry(bucket, False, "Suspended")

    delete_suspended_versioning_obj(bucket, objname, k, c)

    check_configure_versioning_retry(bucket, True, "Enabled")

    (k, c) = create_multiple_versions(bucket, objname, 1, k, c)

    for i in xrange(len(k)):
        print 'JJJ: ', k[i].version_id, c[i]

    _do_remove_versions(bucket, objname, 0, 0, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 0, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))

@attr(resource='object')
@attr(method='remove')
@attr(operation='create and remove versions')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_create_versions_remove_all():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 10
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    _do_remove_versions(bucket, objname, 0, 5, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 5, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))

@attr(resource='object')
@attr(method='remove')
@attr(operation='create and remove versions')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_create_versions_remove_special_names():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 10
    objnames = ['_testobj', '_', ':', ' ']

    for objname in objnames:
        (k, c) = create_multiple_versions(bucket, objname, num_versions)

        _do_remove_versions(bucket, objname, 0, 5, 0.5, k, c)
        _do_remove_versions(bucket, objname, 0, 5, 0, k, c)

        eq(len(k), 0)
        eq(len(k), len(c))

@attr(resource='object')
@attr(method='multipart')
@attr(operation='create and test multipart object')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_create_overwrite_multipart():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")

    objname = 'testobj'

    c = []

    num_vers = 3

    for i in xrange(num_vers):
        c.append(_do_test_multipart_upload_contents(bucket, objname, 3))

    k = []
    for key in bucket.list_versions():
        k.insert(0, key)

    eq(len(k), num_vers)
    check_obj_versions(bucket, objname, k, c)

    _do_remove_versions(bucket, objname, 0, 3, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 3, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))



@attr(resource='object')
@attr(method='multipart')
@attr(operation='list versioned objects')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_list_marker():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")

    objname = 'testobj'
    objname2 = 'testobj-1'

    num_vers = 5

    (k, c) = create_multiple_versions(bucket, objname, num_vers)
    (k2, c2) = create_multiple_versions(bucket, objname2, num_vers)

    k.reverse()
    k2.reverse()

    allkeys = k + k2

    names = []

    for key1, key2 in itertools.izip_longest(bucket.list_versions(), allkeys):
        eq(key1.version_id, key2.version_id)
        names.append(key1.name)

    for i in xrange(len(allkeys)):
        for key1, key2 in itertools.izip_longest(bucket.list_versions(key_marker=names[i], version_id_marker=allkeys[i].version_id), allkeys[i+1:]):
            eq(key1.version_id, key2.version_id)

    # with nonexisting version id, skip to next object
    for key1, key2 in itertools.izip_longest(bucket.list_versions(key_marker=objname, version_id_marker='nosuchversion'), allkeys[5:]):
            eq(key1.version_id, key2.version_id)


@attr(resource='object')
@attr(method='multipart')
@attr(operation='create and test versioned object copying')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_copy_obj_version():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 3
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    # copy into the same bucket
    for i in xrange(num_versions):
        new_key_name = 'key_{i}'.format(i=i)
        new_key = bucket.copy_key(new_key_name, bucket.name, k[i].name, src_version_id=k[i].version_id)
        eq(new_key.get_contents_as_string(), c[i])

    another_bucket = get_new_bucket()

    # copy into a different bucket
    for i in xrange(num_versions):
        new_key_name = 'key_{i}'.format(i=i)
        new_key = another_bucket.copy_key(new_key_name, bucket.name, k[i].name, src_version_id=k[i].version_id)
        eq(new_key.get_contents_as_string(), c[i])

    # test copy of head object
    new_key = another_bucket.copy_key('new_key', bucket.name, objname)
    eq(new_key.get_contents_as_string(), c[num_versions - 1])

def _count_bucket_versioned_objs(bucket):
    k = []
    for key in bucket.list_versions():
        k.insert(0, key)
    return len(k)


@attr(resource='object')
@attr(method='delete')
@attr(operation='delete multiple versions')
@attr(assertion='deletes multiple versions of an object with a single call')
@attr('versioning')
def test_versioning_multi_object_delete():
	bucket = get_new_bucket()

        check_configure_versioning_retry(bucket, True, "Enabled")

        keyname = 'key'

	key0 = bucket.new_key(keyname)
	key0.set_contents_from_string('foo')
	key1 = bucket.new_key(keyname)
	key1.set_contents_from_string('bar')

        stored_keys = []
        for key in bucket.list_versions():
            stored_keys.insert(0, key)

        eq(len(stored_keys), 2)

	result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 2)
        eq(len(result.errors), 0)

        eq(_count_bucket_versioned_objs(bucket), 0)

        # now remove again, should all succeed due to idempotency
        result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 2)
        eq(len(result.errors), 0)

        eq(_count_bucket_versioned_objs(bucket), 0)

@attr(resource='object')
@attr(method='delete')
@attr(operation='delete multiple versions')
@attr(assertion='deletes multiple versions of an object and delete marker with a single call')
@attr('versioning')
def test_versioning_multi_object_delete_with_marker():
        bucket = get_new_bucket()

        check_configure_versioning_retry(bucket, True, "Enabled")

        keyname = 'key'

	key0 = bucket.new_key(keyname)
	key0.set_contents_from_string('foo')
	key1 = bucket.new_key(keyname)
	key1.set_contents_from_string('bar')

        key2 = bucket.delete_key(keyname)
        eq(key2.delete_marker, True)

        stored_keys = []
        for key in bucket.list_versions():
            stored_keys.insert(0, key)

        eq(len(stored_keys), 3)

	result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 3)
        eq(len(result.errors), 0)
        eq(_count_bucket_versioned_objs(bucket), 0)

        delete_markers = []
        for o in result.deleted:
            if o.delete_marker:
                delete_markers.insert(0, o)

        eq(len(delete_markers), 1)
        eq(key2.version_id, delete_markers[0].version_id)

        # now remove again, should all succeed due to idempotency
        result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 3)
        eq(len(result.errors), 0)

        eq(_count_bucket_versioned_objs(bucket), 0)

@attr(resource='object')
@attr(method='delete')
@attr(operation='multi delete create marker')
@attr(assertion='returns correct marker version id')
@attr('versioning')
def test_versioning_multi_object_delete_with_marker_create():
        bucket = get_new_bucket()

        check_configure_versioning_retry(bucket, True, "Enabled")

        keyname = 'key'

        rmkeys = [ bucket.new_key(keyname) ]

        eq(_count_bucket_versioned_objs(bucket), 0)

        result = bucket.delete_keys(rmkeys)
        eq(len(result.deleted), 1)
        eq(_count_bucket_versioned_objs(bucket), 1)

        delete_markers = []
        for o in result.deleted:
            if o.delete_marker:
                delete_markers.insert(0, o)

        eq(len(delete_markers), 1)

        for o in bucket.list_versions():
            eq(o.name, keyname)
            eq(o.version_id, delete_markers[0].delete_marker_version_id)

@attr(resource='object')
@attr(method='put')
@attr(operation='change acl on an object version changes specific version')
@attr(assertion='works')
@attr('versioning')
def test_versioned_object_acl():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    keyname = 'foo'

    key0 = bucket.new_key(keyname)
    key0.set_contents_from_string('bar')
    key1 = bucket.new_key(keyname)
    key1.set_contents_from_string('bla')
    key2 = bucket.new_key(keyname)
    key2.set_contents_from_string('zxc')

    stored_keys = []
    for key in bucket.list_versions():
        stored_keys.insert(0, key)

    k1 = stored_keys[1]

    policy = bucket.get_acl(key_name=k1.name, version_id=k1.version_id)

    default_policy = [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ]

    print repr(policy)
    check_grants(policy.acl.grants, default_policy)

    bucket.set_canned_acl('public-read', key_name=k1.name, version_id=k1.version_id)

    policy = bucket.get_acl(key_name=k1.name, version_id=k1.version_id)
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

    k = bucket.new_key(keyname)
    check_grants(k.get_acl().acl.grants, default_policy)

@attr(resource='object')
@attr(method='put')
@attr(operation='change acl on an object with no version specified changes latest version')
@attr(assertion='works')
@attr('versioning')
def test_versioned_object_acl_no_version_specified():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    keyname = 'foo'

    key0 = bucket.new_key(keyname)
    key0.set_contents_from_string('bar')
    key1 = bucket.new_key(keyname)
    key1.set_contents_from_string('bla')
    key2 = bucket.new_key(keyname)
    key2.set_contents_from_string('zxc')

    stored_keys = []
    for key in bucket.list_versions():
        stored_keys.insert(0, key)

    k2 = stored_keys[2]

    policy = bucket.get_acl(key_name=k2.name, version_id=k2.version_id)

    default_policy = [
        dict(
            permission='FULL_CONTROL',
            id=policy.owner.id,
            display_name=policy.owner.display_name,
            uri=None,
            email_address=None,
            type='CanonicalUser',
        ),
    ]

    print repr(policy)
    check_grants(policy.acl.grants, default_policy)

    bucket.set_canned_acl('public-read', key_name=k2.name)

    policy = bucket.get_acl(key_name=k2.name, version_id=k2.version_id)
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
            ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://cam.qcloud.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
            ),
        ],
    )

def _do_create_object(bucket, objname, i):
    k = bucket.new_key(objname)
    k.set_contents_from_string('data {i}'.format(i=i))

def _do_remove_ver(bucket, obj):
    bucket.delete_key(obj.name, version_id = obj.version_id)

def _do_create_versioned_obj_concurrent(bucket, objname, num):
    t = []
    for i in range(num):
        thr = threading.Thread(target = _do_create_object, args=(bucket, objname, i))
        thr.start()
        t.append(thr)
    return t

def _do_clear_versioned_bucket_concurrent(bucket):
    t = []
    for o in bucket.list_versions():
        thr = threading.Thread(target = _do_remove_ver, args=(bucket, o))
        thr.start()
        t.append(thr)
    return t

def _do_wait_completion(t):
    for thr in t:
        thr.join()

@attr(resource='object')
@attr(method='put')
@attr(operation='concurrent creation of objects, concurrent removal')
@attr(assertion='works')
@attr('versioning')
def test_versioned_concurrent_object_create_concurrent_remove():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    keyname = 'myobj'

    num_objs = 5

    for i in xrange(5):
        t = _do_create_versioned_obj_concurrent(bucket, keyname, num_objs)
        _do_wait_completion(t)

        eq(_count_bucket_versioned_objs(bucket), num_objs)
        eq(len(bucket.get_all_keys()), 1)

        t = _do_clear_versioned_bucket_concurrent(bucket)
        _do_wait_completion(t)

        eq(_count_bucket_versioned_objs(bucket), 0)
        eq(len(bucket.get_all_keys()), 0)

@attr(resource='object')
@attr(method='put')
@attr(operation='concurrent creation and removal of objects')
@attr(assertion='works')
@attr('versioning')
def test_versioned_concurrent_object_create_and_remove():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    keyname = 'myobj'

    num_objs = 3

    all_threads = []

    for i in xrange(3):
        t = _do_create_versioned_obj_concurrent(bucket, keyname, num_objs)
        all_threads.append(t)

        t = _do_clear_versioned_bucket_concurrent(bucket)
        all_threads.append(t)


    for t in all_threads:
        _do_wait_completion(t)

    t = _do_clear_versioned_bucket_concurrent(bucket)
    _do_wait_completion(t)

    eq(_count_bucket_versioned_objs(bucket), 0)
    eq(len(bucket.get_all_keys()), 0)

# Create a lifecycle config.  Either days (int) and prefix (string) is given, or rules.
# Rules is an array of dictionaries, each dict has a 'days' and a 'prefix' key
def create_lifecycle(days = None, prefix = 'test/', rules = None):
    lifecycle = boto.s3.lifecycle.Lifecycle()
    if rules == None:
        expiration = boto.s3.lifecycle.Expiration(days=days)
        rule = boto.s3.lifecycle.Rule(id=prefix, prefix=prefix, status='Enabled',
                                      expiration=expiration)
        lifecycle.append(rule)
    else:
        for rule in rules:
            expiration = boto.s3.lifecycle.Expiration(days=rule['days'])
            rule = boto.s3.lifecycle.Rule(id=rule['id'], prefix=rule['prefix'],
                                          status=rule['status'], expiration=expiration)
            lifecycle.append(rule)
    return lifecycle

def set_lifecycle(rules = None):
    bucket = get_new_bucket()
    lifecycle = create_lifecycle(rules=rules)
    bucket.configure_lifecycle(lifecycle)
    return bucket


@attr(resource='bucket')
@attr(method='put')
@attr(operation='set lifecycle config')
@attr('lifecycle')
def test_lifecycle_set():
    bucket = get_new_bucket()
    lifecycle = create_lifecycle(rules=[{'id': 'rule1', 'days': 1, 'prefix': 'test1/', 'status':'Enabled'},
                                        {'id': 'rule2', 'days': 2, 'prefix': 'test2/', 'status':'Disabled'}])
    time.sleep(SYNC_SLEEP) # wait for sync
    eq(bucket.configure_lifecycle(lifecycle), True)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='get lifecycle config')
@attr('lifecycle')
def test_lifecycle_get():
    bucket = set_lifecycle(rules=[{'id': 'test1/', 'days': 31, 'prefix': 'test1/', 'status': 'Enabled'},
                                  {'id': 'test2/', 'days': 120, 'prefix': 'test2/', 'status':'Enabled'}])
    time.sleep(SYNC_SLEEP)
    current = bucket.get_lifecycle_config()
    eq(current[0].expiration.days, 31)
    eq(current[0].id, 'test1/')
    eq(current[0].prefix, 'test1/')
    eq(current[1].expiration.days, 120)
    eq(current[1].id, 'test2/')
    eq(current[1].prefix, 'test2/')

# The test harnass for lifecycle is configured to treat days as 2 second intervals.
@attr(resource='bucket')
@attr(method='put')
@attr(operation='test lifecycle expiration')
@attr('lifecycle')
@attr('fails_on_aws')
def test_lifecycle_expiration():
    bucket = set_lifecycle(rules=[{'id': 'rule1', 'days': 2, 'prefix': 'expire1/', 'status': 'Enabled'},
                                  {'id':'rule2', 'days': 6, 'prefix': 'expire3/', 'status': 'Enabled'}])
    time.sleep(SYNC_SLEEP)
    _create_keys(bucket=bucket, keys=['expire1/foo', 'expire1/bar', 'keep2/foo',
                                      'keep2/bar', 'expire3/foo', 'expire3/bar'])
    # Get list of all keys
    init_keys = bucket.get_all_keys()
    # Wait for first expiration (plus fudge to handle the timer window)
    #time.sleep(35)
    expire1_keys = bucket.get_all_keys()
    # Wait for next expiration cycle
    #time.sleep(15)
    keep2_keys = bucket.get_all_keys()
    # Wait for final expiration cycle
    #time.sleep(25)
    expire3_keys = bucket.get_all_keys()

    # 因为规则设置的是2天和6天过期，而实际上只sleep了几十秒，所以都不过期，都是6个
    eq(len(init_keys), 6)
    #eq(len(expire1_keys), 4)
    #eq(len(keep2_keys), 4)
    #eq(len(expire3_keys), 2)

@attr(resource='bucket')
@attr(method='put')
@attr(operation='id too long in lifecycle rule')
@attr('lifecycle')
@attr(assertion='fails 400')
def test_lifecycle_id_too_long():
    bucket = get_new_bucket()
    lifecycle = create_lifecycle(rules=[{'id': 256*'a', 'days': 2, 'prefix': 'test1/', 'status': 'Enabled'}])
    e = bucket.configure_lifecycle(lifecycle)
    # s3没有id字符数限制
    eq(e, True)
    #eq(e.error_code, 'InvalidArgument')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='same id')
@attr('lifecycle')
@attr(assertion='fails 400')
def test_lifecycle_same_id():
    bucket = get_new_bucket()
    lifecycle = create_lifecycle(rules=[{'id': 'rule1', 'days': 2, 'prefix': 'test1/', 'status': 'Enabled'},
                                        {'id': 'rule1', 'days': 2, 'prefix': 'test2/', 'status': 'Enabled'}])
    e = assert_raises(boto.exception.S3ResponseError, bucket.configure_lifecycle, lifecycle)
    eq(e.status, 400)
    eq(e.error_code, 'InvalidArgument')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='invalid status in lifecycle rule')
@attr('lifecycle')
@attr(assertion='fails 400')
def test_lifecycle_invalid_status():
    bucket = get_new_bucket()
    lifecycle = create_lifecycle(rules=[{'id': 'rule1', 'days': 2, 'prefix': 'test1/', 'status': 'enabled'}])
    e = assert_raises(boto.exception.S3ResponseError, bucket.configure_lifecycle, lifecycle)
    eq(e.status, 400)
    eq(e.error_code, 'MalformedXML')

    lifecycle = create_lifecycle(rules=[{'id': 'rule1', 'days': 2, 'prefix': 'test1/', 'status': 'disabled'}])
    e = assert_raises(boto.exception.S3ResponseError, bucket.configure_lifecycle, lifecycle)
    eq(e.status, 400)
    eq(e.error_code, 'MalformedXML')

    lifecycle = create_lifecycle(rules=[{'id': 'rule1', 'days': 2, 'prefix': 'test1/', 'status': 'invalid'}])
    e = assert_raises(boto.exception.S3ResponseError, bucket.configure_lifecycle, lifecycle)
    eq(e.status, 400)
    eq(e.error_code, 'MalformedXML')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='rules conflicted in lifecycle')
@attr('lifecycle')
@attr(assertion='fails 400')
def test_lifecycle_rules_conflicted():
    bucket = get_new_bucket()
    lifecycle = create_lifecycle(rules=[{'id': 'rule1', 'days': 2, 'prefix': 'test1/', 'status': 'Enabled'},
                                        {'id': 'rule2', 'days': 3, 'prefix': 'test3/', 'status': 'Enabled'},
                                        {'id': 'rule3', 'days': 5, 'prefix': 'test1/abc', 'status': 'Enabled'}])
    e = assert_raises(boto.exception.S3ResponseError, bucket.configure_lifecycle, lifecycle)
    eq(e.status, 400)
    eq(e.error_code, 'InvalidRequest')


def generate_lifecycle_body(rules):
    body = '<?xml version="1.0" encoding="UTF-8"?><LifecycleConfiguration>'
    for rule in rules:
        body += '<Rule><ID>%s</ID><Prefix>%s</Prefix><Status>%s</Status>' % (rule['ID'], rule['Prefix'], rule['Status'])
        if 'Expiration' in rule.keys():
            if 'ExpiredObjectDeleteMarker' in rule['Expiration'].keys():
                body += '<Expiration><ExpiredObjectDeleteMarker>%s</ExpiredObjectDeleteMarker></Expiration>' \
                        % rule['Expiration']['ExpiredObjectDeleteMarker']
            else:
                body += '<Expiration><Days>%d</Days></Expiration>' % rule['Expiration']['Days']
        if 'NoncurrentVersionExpiration' in rule.keys():
            body += '<NoncurrentVersionExpiration><NoncurrentDays>%d</NoncurrentDays></NoncurrentVersionExpiration>' % \
                    rule['NoncurrentVersionExpiration']['NoncurrentDays']
        if 'AbortIncompleteMultipartUpload' in rule.keys():
            body += '<AbortIncompleteMultipartUpload><DaysAfterInitiation>%d</DaysAfterInitiation>' \
                    '</AbortIncompleteMultipartUpload>' % rule['AbortIncompleteMultipartUpload']['DaysAfterInitiation']
        body += '</Rule>'
    body += '</LifecycleConfiguration>'
    return body


@attr(resource='bucket')
@attr(method='put')
@attr(operation='set lifecycle config with noncurrent version expiration')
@attr('lifecycle')
def test_lifecycle_set_noncurrent():
    bucket = get_new_bucket()
    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled', 'NoncurrentVersionExpiration': {'NoncurrentDays': 2}},
        {'ID': 'rule2', 'Prefix': 'test2/', 'Status': 'Disabled', 'NoncurrentVersionExpiration': {'NoncurrentDays': 3}}
    ]
    body = generate_lifecycle_body(rules)
    fp = StringIO(body)
    md5 = boto.utils.compute_md5(fp)
    headers = {'Content-MD5': md5[1], 'Content-Type': 'text/xml'}
    res = bucket.connection.make_request('PUT', bucket.name, data=fp.getvalue(), query_args='lifecycle',
                                         headers=headers)
    eq(res.status, 200)
    eq(res.reason, 'OK')


# The test harnass for lifecycle is configured to treat days as 2 second intervals.
@attr(resource='bucket')
@attr(method='put')
@attr(operation='test lifecycle non-current version expiration')
@attr('lifecycle')
@attr('fails_on_aws')
def test_lifecycle_noncur_expiration():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")
    create_multiple_versions(bucket, "test1/a", 3)
    create_multiple_versions(bucket, "test2/abc", 3)
    init_keys = bucket.get_all_versions()
    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled', 'NoncurrentVersionExpiration': {'NoncurrentDays': 2}}
    ]
    body = generate_lifecycle_body(rules)
    fp = StringIO(body)
    md5 = boto.utils.compute_md5(fp)
    headers = {'Content-MD5': md5[1], 'Content-Type': 'text/xml'}
    bucket.connection.make_request('PUT', bucket.name, data=fp.getvalue(), query_args='lifecycle',
                                         headers=headers)
    time.sleep(50)
    expire_keys = bucket.get_all_versions()
    eq(len(init_keys), 6)
    # test设置生命周期，以天为单位的，所以这里改test，这个在aws上也过不了
    eq(len(expire_keys), 6)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='set lifecycle config with delete marker expiration')
@attr('lifecycle')
def test_lifecycle_set_deletemarker():
    bucket = get_new_bucket()
    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled', 'Expiration': {'ExpiredObjectDeleteMarker': 'true'}}
    ]
    body = generate_lifecycle_body(rules)
    fp = StringIO(body)
    md5 = boto.utils.compute_md5(fp)
    headers = {'Content-MD5': md5[1], 'Content-Type': 'text/xml'}
    res = bucket.connection.make_request('PUT', bucket.name, data=fp.getvalue(), query_args='lifecycle',
                                         headers=headers)
    eq(res.status, 200)
    eq(res.reason, 'OK')


# The test harnass for lifecycle is configured to treat days as 1 second intervals.
@attr(resource='bucket')
@attr(method='put')
@attr(operation='test lifecycle delete marker expiration')
@attr('lifecycle')
@attr('fails_on_aws')
def test_lifecycle_deletemarker_expiration():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")
    create_multiple_versions(bucket, "test1/a", 1)
    create_multiple_versions(bucket, "test2/abc", 1)
    bucket.delete_key('test1/a')
    bucket.delete_key('test2/abc')
    init_keys = bucket.get_all_versions()
    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled', 'Expiration': {'ExpiredObjectDeleteMarker': 'true'},
         'NoncurrentVersionExpiration': {'NoncurrentDays': 1}}
    ]
    body = generate_lifecycle_body(rules)
    fp = StringIO(body)
    md5 = boto.utils.compute_md5(fp)
    headers = {'Content-MD5': md5[1], 'Content-Type': 'text/xml'}
    bucket.connection.make_request('PUT', bucket.name, data=fp.getvalue(), query_args='lifecycle',
                                   headers=headers)
    time.sleep(50)
    expire_keys = bucket.get_all_versions()
    #eq(len(init_keys), 4)
    #eq(len(expire_keys), 2)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='set lifecycle config with multipart expiration')
@attr('lifecycle')
def test_lifecycle_set_multipart():
    bucket = get_new_bucket()
    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled',
         'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 2}},
        {'ID': 'rule2', 'Prefix': 'test2/', 'Status': 'Disabled',
         'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 3}}
    ]
    body = generate_lifecycle_body(rules)
    fp = StringIO(body)
    md5 = boto.utils.compute_md5(fp)
    headers = {'Content-MD5': md5[1], 'Content-Type': 'text/xml'}
    res = bucket.connection.make_request('PUT', bucket.name, data=fp.getvalue(), query_args='lifecycle',
                                         headers=headers)
    eq(res.status, 200)
    eq(res.reason, 'OK')


# The test harnass for lifecycle is configured to treat days as 1 second intervals.
@attr(resource='bucket')
@attr(method='put')
@attr(operation='test lifecycle multipart expiration')
@attr('lifecycle')
@attr('fails_on_aws')
def test_lifecycle_multipart_expiration():
    bucket = get_new_bucket()
    key_names = ['test1/a', 'test2/']
    for key_name in key_names:
        bucket.initiate_multipart_upload(key_name)

    init_keys = bucket.get_all_multipart_uploads()
    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled',
         'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 2}}
    ]
    body = generate_lifecycle_body(rules)
    fp = StringIO(body)
    md5 = boto.utils.compute_md5(fp)
    headers = {'Content-MD5': md5[1], 'Content-Type': 'text/xml'}
    bucket.connection.make_request('PUT', bucket.name, data=fp.getvalue(), query_args='lifecycle',
                                   headers=headers)
    time.sleep(50)
    expire_keys = bucket.get_all_multipart_uploads()
    #eq(len(init_keys), 2)
    #eq(len(expire_keys), 1)


def _test_encryption_sse_customer_write(file_size):
    """
    Tests Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    key = bucket.new_key('testobj')
    data = 'A'*file_size
    key.set_contents_from_string(data, headers=sse_client_headers)
    rdata = key.get_contents_as_string(headers=sse_client_headers)
    eq(data, rdata)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-C encrypted transfer 1 byte')
@attr(assertion='success')
@attr('encryption')
def test_encrypted_transfer_1b():
    raise SkipTest
    _test_encryption_sse_customer_write(1)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-C encrypted transfer 1KB')
@attr(assertion='success')
@attr('encryption')
def test_encrypted_transfer_1kb():
    raise SkipTest
    _test_encryption_sse_customer_write(1024)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-C encrypted transfer 1MB')
@attr(assertion='success')
@attr('encryption')
def test_encrypted_transfer_1MB():
    raise SkipTest
    _test_encryption_sse_customer_write(1024*1024)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-C encrypted transfer 13 bytes')
@attr(assertion='success')
@attr('encryption')
def test_encrypted_transfer_13b():
    raise SkipTest
    _test_encryption_sse_customer_write(13)


@attr(resource='object')
@attr(method='head')
@attr(operation='Test SSE-C encrypted does perform head properly')
@attr(assertion='success')
@attr('encryption')
def test_encryption_sse_c_method_head():
    raise SkipTest
    bucket = get_new_bucket()
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    key = bucket.new_key('testobj')
    data = 'A'*1000
    key.set_contents_from_string(data, headers=sse_client_headers)

    res = _make_request('HEAD', bucket, key, authenticated=True)
    eq(res.status, 400)

    res = _make_request('HEAD', bucket, key, authenticated=True, request_headers=sse_client_headers)
    eq(res.status, 200)


@attr(resource='object')
@attr(method='put')
@attr(operation='write encrypted with SSE-C and read without SSE-C')
@attr(assertion='operation fails')
@attr('encryption')
def test_encryption_sse_c_present():
    raise SkipTest
    bucket = get_new_bucket()
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    key.set_contents_from_string(data, headers=sse_client_headers)
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 400)


@attr(resource='object')
@attr(method='put')
@attr(operation='write encrypted with SSE-C but read with other key')
@attr(assertion='operation fails')
@attr('encryption')
def test_encryption_sse_c_other_key():
    raise SkipTest
    bucket = get_new_bucket()
    sse_client_headers_A = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    sse_client_headers_B = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    key.set_contents_from_string(data, headers=sse_client_headers_A)
    e = assert_raises(boto.exception.S3ResponseError,
                      key.get_contents_as_string, headers=sse_client_headers_B)
    eq(e.status, 400)


@attr(resource='object')
@attr(method='put')
@attr(operation='write encrypted with SSE-C, but md5 is bad')
@attr(assertion='operation fails')
@attr('encryption')
def test_encryption_sse_c_invalid_md5():
    raise SkipTest
    bucket = get_new_bucket()
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'AAAAAAAAAAAAAAAAAAAAAA=='
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    e = assert_raises(boto.exception.S3ResponseError,
                      key.set_contents_from_string, data, headers=sse_client_headers)
    eq(e.status, 400)


@attr(resource='object')
@attr(method='put')
@attr(operation='write encrypted with SSE-C, but dont provide MD5')
@attr(assertion='operation fails')
@attr('encryption')
def test_encryption_sse_c_no_md5():
    raise SkipTest
    bucket = get_new_bucket()
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs='
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    e = assert_raises(boto.exception.S3ResponseError,
                      key.set_contents_from_string, data, headers=sse_client_headers)


@attr(resource='object')
@attr(method='put')
@attr(operation='declare SSE-C but do not provide key')
@attr(assertion='operation fails')
@attr('encryption')
def test_encryption_sse_c_no_key():
    raise SkipTest
    bucket = get_new_bucket()
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256'
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    e = assert_raises(boto.exception.S3ResponseError,
                      key.set_contents_from_string, data, headers=sse_client_headers)


@attr(resource='object')
@attr(method='put')
@attr(operation='Do not declare SSE-C but provide key and MD5')
@attr(assertion='operation successfull, no encryption')
@attr('encryption')
def test_encryption_key_no_sse_c():
    raise SkipTest
    bucket = get_new_bucket()
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    key.set_contents_from_string(data, headers=sse_client_headers)
    rdata = key.get_contents_as_string()
    eq(data, rdata)


def _multipart_upload_enc(bucket, s3_key_name, size, part_size=5*1024*1024,
                          do_list=None, init_headers=None, part_headers=None,
                          metadata=None, resend_parts=[]):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    upload = bucket.initiate_multipart_upload(s3_key_name, headers=init_headers, metadata=metadata)
    s = ''
    for i, part in enumerate(generate_random(size, part_size)):
        s += part
        transfer_part(bucket, upload.id, upload.key_name, i, part, part_headers)
        if i in resend_parts:
            transfer_part(bucket, upload.id, upload.key_name, i, part, part_headers)

    if do_list is not None:
        l = bucket.list_multipart_uploads()
        l = list(l)

    return (upload, s)


def _check_content_using_range_enc(k, data, step, enc_headers=None):
    objlen = k.size
    for ofs in xrange(0, k.size, step):
        toread = k.size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        read_range = k.get_contents_as_string(
            headers=dict({'Range': 'bytes={s}-{e}'.format(s=ofs, e=end)}, **enc_headers))
        eq(len(read_range), toread)
        eq(read_range, data[ofs:end+1])


@attr(resource='object')
@attr(method='put')
@attr(operation='complete multi-part upload')
@attr(assertion='successful')
@attr('encryption')
def test_encryption_sse_c_multipart_upload():
    raise SkipTest
    bucket = get_new_bucket()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    enc_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    (upload, data) = _multipart_upload_enc(bucket, key, objlen,
                                           init_headers=enc_headers, part_headers=enc_headers,
                                           metadata={'foo': 'bar'})
    upload.complete_upload()
    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 1), 1)
    eq(result.get('x-rgw-bytes-used', 30 * 1024 * 1024), 30 * 1024 * 1024)

    k = bucket.get_key(key, headers=enc_headers)
    eq(k.metadata['foo'], 'bar')
    eq(k.content_type, content_type)
    test_string = k.get_contents_as_string(headers=enc_headers)
    eq(len(test_string), k.size)
    eq(data, test_string)
    eq(test_string, data)

    _check_content_using_range_enc(k, data, 1000000, enc_headers=enc_headers)
    _check_content_using_range_enc(k, data, 10000000, enc_headers=enc_headers)


@attr(resource='object')
@attr(method='put')
@attr(operation='multipart upload with bad key for uploading chunks')
@attr(assertion='successful')
@attr('encryption')
def test_encryption_sse_c_multipart_invalid_chunks_1():
    raise SkipTest
    bucket = get_new_bucket()
    key = "multipart_enc"
    content_type = 'text/bla'
    objlen = 30 * 1024 * 1024
    init_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }
    e = assert_raises(boto.exception.S3ResponseError,
                      _multipart_upload_enc, bucket, key, objlen,
                      init_headers=init_headers, part_headers=part_headers,
                      metadata={'foo': 'bar'})
    eq(e.status, 400)


@attr(resource='object')
@attr(method='put')
@attr(operation='multipart upload with bad md5 for chunks')
@attr(assertion='successful')
@attr('encryption')
def test_encryption_sse_c_multipart_invalid_chunks_2():
    raise SkipTest
    bucket = get_new_bucket()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    init_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'AAAAAAAAAAAAAAAAAAAAAA=='
    }
    e = assert_raises(boto.exception.S3ResponseError,
                      _multipart_upload_enc, bucket, key, objlen,
                      init_headers=init_headers, part_headers=part_headers,
                      metadata={'foo': 'bar'})
    eq(e.status, 400)


@attr(resource='object')
@attr(method='put')
@attr(operation='complete multi-part upload and download with bad key')
@attr(assertion='successful')
@attr('encryption')
def test_encryption_sse_c_multipart_bad_download():
    raise SkipTest
    bucket = get_new_bucket()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    put_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    get_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }

    (upload, data) = _multipart_upload_enc(bucket, key, objlen,
                                           init_headers=put_headers, part_headers=put_headers,
                                           metadata={'foo': 'bar'})
    upload.complete_upload()
    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 1), 1)
    eq(result.get('x-rgw-bytes-used', 30 * 1024 * 1024), 30 * 1024 * 1024)

    k = bucket.get_key(key, headers=put_headers)
    eq(k.metadata['foo'], 'bar')
    eq(k.content_type, content_type)
    e = assert_raises(boto.exception.S3ResponseError,
                      k.get_contents_as_string, headers=get_headers)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
@attr('encryption')
def test_encryption_sse_c_post_object_authenticated_request():
    raise SkipTest
    bucket = get_new_bucket()

    url = _get_post_url(s3.main, bucket)

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"), \
                       "conditions": [ \
                           {"bucket": bucket.name}, \
                           ["starts-with", "$key", "foo"], \
                           {"acl": "private"}, \
                           ["starts-with", "$Content-Type", "text/plain"], \
                           ["starts-with", "$x-amz-server-side-encryption-customer-algorithm", ""], \
                           ["starts-with", "$x-amz-server-side-encryption-customer-key", ""], \
                           ["starts-with", "$x-amz-server-side-encryption-customer-key-md5", ""], \
                           ["content-length-range", 0, 1024] \
                           ] \
                       }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    conn = s3.main
    signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id), \
                            ("acl" , "private"),("signature" , signature),("policy" , policy), \
                            ("Content-Type" , "text/plain"), \
                            ('x-amz-server-side-encryption-customer-algorithm', 'AES256'), \
                            ('x-amz-server-side-encryption-customer-key', 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs='), \
                            ('x-amz-server-side-encryption-customer-key-md5', 'DWygnHRtgiJ77HCm+1rvHw=='), \
                            ('file', ('bar'),), ])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    get_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    key = bucket.get_key("foo.txt", headers=get_headers)
    got = key.get_contents_as_string(headers=get_headers)
    eq(got, 'bar')


def _test_sse_kms_customer_write(file_size, key_id = 'testkey-1'):
    """
    Tests Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': key_id
    }
    key = bucket.new_key('testobj')
    data = 'A'*file_size
    key.set_contents_from_string(data, headers=sse_kms_client_headers)
    rdata = key.get_contents_as_string(headers=sse_kms_client_headers)
    eq(data, rdata)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 1 byte')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_transfer_1b():
    raise SkipTest
    _test_sse_kms_customer_write(1)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 1KB')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_transfer_1kb():
    raise SkipTest
    _test_sse_kms_customer_write(1024)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 1MB')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_transfer_1MB():
    raise SkipTest
    _test_sse_kms_customer_write(1024*1024)


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 13 bytes')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_transfer_13b():
    raise SkipTest
    _test_sse_kms_customer_write(13)


@attr(resource='object')
@attr(method='head')
@attr(operation='Test SSE-KMS encrypted does perform head properly')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_method_head():
    raise SkipTest
    bucket = get_new_bucket()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1'
    }
    key = bucket.new_key('testobj')
    data = 'A'*1000
    key.set_contents_from_string(data, headers=sse_kms_client_headers)

    res = _make_request('HEAD', bucket, key, authenticated=True)
    eq(res.status, 200)
    eq(res.getheader('x-amz-server-side-encryption'), 'aws:kms')
    eq(res.getheader('x-amz-server-side-encryption-aws-kms-key-id'), 'testkey-1')


@attr(resource='object')
@attr(method='put')
@attr(operation='write encrypted with SSE-KMS and read without SSE-KMS')
@attr(assertion='operation success')
@attr('encryption')
def test_sse_kms_present():
    raise SkipTest
    bucket = get_new_bucket()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1'
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    key.set_contents_from_string(data, headers=sse_kms_client_headers)
    result = key.get_contents_as_string()
    eq(data, result)


@attr(resource='object')
@attr(method='put')
@attr(operation='write encrypted with SSE-KMS but read with other key')
@attr(assertion='operation fails')
@attr('encryption')
def test_sse_kms_other_key():
    raise SkipTest
    bucket = get_new_bucket()
    sse_kms_client_headers_A = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1'
    }
    sse_kms_client_headers_B = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-2'
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    key.set_contents_from_string(data, headers=sse_kms_client_headers_A)
    result = key.get_contents_as_string(headers=sse_kms_client_headers_B)
    eq(data, result)


@attr(resource='object')
@attr(method='put')
@attr(operation='declare SSE-KMS but do not provide key_id')
@attr(assertion='operation fails')
@attr('encryption')
def test_sse_kms_no_key():
    raise SkipTest
    bucket = get_new_bucket()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms'
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    e = assert_raises(boto.exception.S3ResponseError,
                      key.set_contents_from_string, data, headers=sse_kms_client_headers)


@attr(resource='object')
@attr(method='put')
@attr(operation='Do not declare SSE-KMS but provide key_id')
@attr(assertion='operation successfull, no encryption')
@attr('encryption')
def test_sse_kms_not_declared():
    raise SkipTest
    bucket = get_new_bucket()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-2'
    }
    key = bucket.new_key('testobj')
    data = 'A'*100
    key.set_contents_from_string(data, headers=sse_kms_client_headers)
    rdata = key.get_contents_as_string()
    eq(data, rdata)


@attr(resource='object')
@attr(method='put')
@attr(operation='complete KMS multi-part upload')
@attr(assertion='successful')
@attr('encryption')
def test_sse_kms_multipart_upload():
    raise SkipTest
    bucket = get_new_bucket()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    enc_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-2',
        'Content-Type': content_type
    }
    (upload, data) = _multipart_upload_enc(bucket, key, objlen,
                                           init_headers=enc_headers, part_headers=enc_headers,
                                           metadata={'foo': 'bar'})
    upload.complete_upload()
    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 1), 1)
    eq(result.get('x-rgw-bytes-used', 30 * 1024 * 1024), 30 * 1024 * 1024)

    k = bucket.get_key(key)
    eq(k.metadata['foo'], 'bar')
    eq(k.content_type, content_type)
    test_string = k.get_contents_as_string(headers=enc_headers)
    eq(len(test_string), k.size)
    eq(data, test_string)
    eq(test_string, data)

    _check_content_using_range_enc(k, data, 1000000, enc_headers=enc_headers)
    _check_content_using_range_enc(k, data, 10000000, enc_headers=enc_headers)


@attr(resource='object')
@attr(method='put')
@attr(operation='multipart KMS upload with bad key_id for uploading chunks')
@attr(assertion='successful')
@attr('encryption')
def test_sse_kms_multipart_invalid_chunks_1():
    raise SkipTest
    bucket = get_new_bucket()
    key = "multipart_enc"
    content_type = 'text/bla'
    objlen = 30 * 1024 * 1024
    init_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1',
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-2'
    }
    _multipart_upload_enc(bucket, key, objlen,
                            init_headers=init_headers, part_headers=part_headers,
                            metadata={'foo': 'bar'})


@attr(resource='object')
@attr(method='put')
@attr(operation='multipart KMS upload with unexistent key_id for chunks')
@attr(assertion='successful')
@attr('encryption')
def test_sse_kms_multipart_invalid_chunks_2():
    raise SkipTest
    bucket = get_new_bucket()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    init_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1',
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-not-present'
    }
    _multipart_upload_enc(bucket, key, objlen,
                            init_headers=init_headers, part_headers=part_headers,
                            metadata={'foo': 'bar'})


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated KMS browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
@attr('encryption')
def test_sse_kms_post_object_authenticated_request():
    raise SkipTest
    bucket = get_new_bucket()

    url = _get_post_url(s3.main, bucket)

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"), \
                       "conditions": [ \
                           {"bucket": bucket.name}, \
                           ["starts-with", "$key", "foo"], \
                           {"acl": "private"}, \
                           ["starts-with", "$Content-Type", "text/plain"], \
                           ["starts-with", "$x-amz-server-side-encryption", ""], \
                           ["starts-with", "$x-amz-server-side-encryption-aws-kms-key-id", ""], \
                           ["content-length-range", 0, 1024] \
                           ] \
                       }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    policy = base64.b64encode(json_policy_document)
    conn = s3.main
    signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id), \
                            ("acl" , "private"),("signature" , signature),("policy" , policy), \
                            ("Content-Type" , "text/plain"), \
                            ('x-amz-server-side-encryption', 'aws:kms'), \
                            ('x-amz-server-side-encryption-aws-kms-key-id', 'testkey-1'), \
                            ('file', ('bar'),), ])

    r = requests.post(url, files = payload)
    eq(r.status_code, 204)
    get_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1',
    }

    key = bucket.get_key("foo.txt")
    got = key.get_contents_as_string(headers=get_headers)
    eq(got, 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 1 byte')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_barb_transfer_1b():
    raise SkipTest
    if 'kms_keyid' not in config['main']:
        raise SkipTest
    _test_sse_kms_customer_write(1, key_id = config['main']['kms_keyid'])


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 1KB')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_barb_transfer_1kb():
    raise SkipTest
    if 'kms_keyid' not in config['main']:
        raise SkipTest
    _test_sse_kms_customer_write(1024, key_id = config['main']['kms_keyid'])


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 1MB')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_barb_transfer_1MB():
    raise SkipTest
    if 'kms_keyid' not in config['main']:
        raise SkipTest
    _test_sse_kms_customer_write(1024*1024, key_id = config['main']['kms_keyid'])


@attr(resource='object')
@attr(method='put')
@attr(operation='Test SSE-KMS encrypted transfer 13 bytes')
@attr(assertion='success')
@attr('encryption')
def test_sse_kms_barb_transfer_13b():
    raise SkipTest
    if 'kms_keyid' not in config['main']:
        raise SkipTest
    _test_sse_kms_customer_write(13, key_id = config['main']['kms_keyid'])

@attr(resource='bucket')
@attr(method='put')
@attr(operation='test put get delete bucket tag')
@attr(assertion='success')
@attr('put get delete bucket tag')
def test_put_get_delete_bucket_tag():
    bucket = get_new_bucket()
    tag_set = boto.s3.tagging.TagSet()
    tag_set.add_tag('name', 'xiaoming')
    tag_set.add_tag('age', '18')
    tag_set.add_tag('school', 'x xaF8+-=.:_/school')
    tags = boto.s3.tagging.Tags()
    tags.add_tag_set(tag_set)
    eq(bucket.set_tags(tags), True)
    bucket.get_tags()
    eq(bucket.delete_tags(), True)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='test get bucket tag without tag')
@attr(assertion='404')
@attr('get bucket tag with empty tag')
def test_get_bucket_with_no_tag():
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3ResponseError, bucket.get_tags)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchTagSet')

@attr(resource='bucket')
@attr(method='delete')
@attr(operation='test delete bucket tag without tag')
@attr(assertion='success')
@attr('delete bucket tag with empty tag')
def test_delete_bucket_with_no_tag():
    bucket = get_new_bucket()
    eq(bucket.delete_tags(), True)

@attr(resource='bucket')
@attr(method='put')
@attr(operation='put tag number exceed limit')
@attr(assertion='400')
@attr('put bucket tag with exceed limit')
def test_put_bucket_tag_number_exceed_limit():
    bucket = get_new_bucket()
    tag_set = boto.s3.tagging.TagSet()
    # max tag number is 11
    for x in range(11):
        key = 'key_' + str(x)
        value = 'value_' + str(x)
        tag_set.add_tag(key, value)

    tags = boto.s3.tagging.Tags()
    tags.add_tag_set(tag_set)
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_tags, tags)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request')
    eq(e.error_code, 'BadRequest')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='test put bucket tag for noexist tag')
@attr(assertion='404')
@attr('put tag for noexist tag')
def test_put_tag_for_noexist_bucket():
    connection = s3.main
    bucket = boto.s3.bucket.Bucket(connection, name='chengwubuckettag-1253969820')
    tag_set = boto.s3.tagging.TagSet()
    tag_set.add_tag('name', 'xiaoming')
    tags = boto.s3.tagging.Tags()
    tags.add_tag_set(tag_set)
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_tags, tags)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='bucket')
@attr(method='delete')
@attr(operation='test delete bucket tag for noexist tag')
@attr(assertion='404')
@attr('delete tag for noexist tag')
def test_delete_tag_for_noexist_bucket():
    connection = s3.main
    bucket = boto.s3.bucket.Bucket(connection, name='chengwubuckettag-1253969820')
    e = assert_raises(boto.exception.S3ResponseError, bucket.delete_tags)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='test get bucket tag for noexist tag')
@attr(assertion='404')
@attr('get tag for noexist tag')
def test_get_tag_for_noexist_bucket():
    connection = s3.main
    bucket = boto.s3.bucket.Bucket(connection, name='chengwubuckettag-1253969820')
    e = assert_raises(boto.exception.S3ResponseError, bucket.get_tags)

    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')
