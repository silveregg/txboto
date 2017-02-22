# Copyright (c) 2006-2012 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2012 Amazon.com, Inc. or its affiliates.
# Copyright (c) 2010 Google
# Copyright (c) 2008 rPath, Inc.
# Copyright (c) 2009 The Echo Nest Corporation
# Copyright (c) 2010, Eucalyptus Systems, Inc.
# Copyright (c) 2011, Nexenta Systems Inc.
# Copyright (c) 2015 Silver Egg Technology, Co., Ltd.
# Copyright (c) 2015 Michael Franke
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Parts of this code were copied or derived from sample code supplied by AWS.
# The following notice applies to that code.
#
#  This software code is made available "AS IS" without warranties of any
#  kind.  You may copy, display, modify and redistribute the software
#  code either by itself or as incorporated into your code; provided that
#  you do not remove any proprietary notices.  Your use of this software
#  code is at your own risk and you waive any claim against Amazon
#  Digital Services, Inc. or its affiliates with respect to your use of
#  this software code. (c) 2006 Amazon Digital Services, Inc. or its
#  affiliates.

"""
Handles connections to AWS
"""
from __future__ import absolute_import

import os
import random
import re
import sys
import time
import xml.sax

from twisted.internet import defer

from datetime import datetime

import txboto

from txboto import auth
from txboto import config
from txboto.base import AWSBaseConnection, HTTPRequest, log
from txboto.compat import six
from txboto.exception import BotoClientError
from txboto.exception import BotoServerError
from txboto.exception import PleaseRetryException
from txboto.httpcodes import code2status
from txboto.provider import Provider
from txboto.resultset import ResultSet

ON_APP_ENGINE = all(key in os.environ for key in (
    'USER_IS_ADMIN', 'CURRENT_VERSION_ID', 'APPLICATION_ID'))


class AWSAuthConnection(AWSBaseConnection):
    def __init__(self, host, aws_access_key_id=None,
                 aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, path='/',
                 provider='aws', security_token=None,
                 suppress_consec_slashes=True,
                 validate_certs=True, profile_name=None,
                 **kwargs):
        """
        :type host: str
        :param host: The host to make the connection to

        :keyword str aws_access_key_id: Your AWS Access Key ID (provided by
            Amazon). If none is specified, the value in your
            ``AWS_ACCESS_KEY_ID`` environmental variable is used.
        :keyword str aws_secret_access_key: Your AWS Secret Access Key
            (provided by Amazon). If none is specified, the value in your
            ``AWS_SECRET_ACCESS_KEY`` environmental variable is used.
        :keyword str security_token: The security token associated with
            temporary credentials issued by STS.  Optional unless using
            temporary credentials.  If none is specified, the environment
            variable ``AWS_SECURITY_TOKEN`` is used if defined.

        :type is_secure: boolean
        :param is_secure: Whether the connection is over SSL

        :type https_connection_factory: list or tuple
        :param https_connection_factory: A pair of an HTTP connection
            factory and the exceptions to catch.  The factory should have
            a similar interface to L{http_client.HTTPSConnection}.

        :param str proxy: Address/hostname for a proxy server

        :type proxy_port: int
        :param proxy_port: The port to use when connecting over a proxy

        :type proxy_user: str
        :param proxy_user: The username to connect with on the proxy

        :type proxy_pass: str
        :param proxy_pass: The password to use when connection over a proxy.

        :type port: int
        :param port: The port to use to connect

        :type suppress_consec_slashes: bool
        :param suppress_consec_slashes: If provided, controls whether
            consecutive slashes will be suppressed in key paths.

        :type validate_certs: bool
        :param validate_certs: Controls whether SSL certificates
            will be validated or not.  Defaults to True.

        :type profile_name: str
        :param profile_name: Override usual Credentials section in config
            file to use a named set of keys instead.
        """
        kw = dict(kwargs)
        kw.update({
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'is_secure': is_secure,
            'host': host,
            'port': port,
            'proxy': proxy,
            'proxy_port': proxy_port,
            'proxy_user': proxy_user,
            'proxy_pass': proxy_pass
        })
        super(AWSAuthConnection, self).__init__(**kw)

        self.suppress_consec_slashes = suppress_consec_slashes
        self.num_retries = 6

        self.path = path

        # if the value passed in for debug
        if not isinstance(debug, six.integer_types):
            debug = 0
        self.debug = config.getint('TxBoto', 'debug', debug)

        if (sys.version_info[0], sys.version_info[1]) >= (2, 6):
            # If timeout isn't defined in txboto config file, use 70 second
            # default as recommended by
            # http://docs.aws.amazon.com/amazonswf/latest/apireference/API_PollForActivityTask.html
            self.timeout = config.getint('TxBoto', 'http_socket_timeout', 70)

        if isinstance(provider, Provider):
            # Allow overriding Provider
            self.provider = provider
        else:
            self._provider_type = provider
            self.provider = Provider(self._provider_type,
                                     aws_access_key_id,
                                     aws_secret_access_key,
                                     security_token,
                                     profile_name)

        # Allow config file to override default host, port, and host header.
        if self.provider.host:
            self.host = self.provider.host
        if self.provider.port:
            self.port = self.provider.port
        if self.provider.host_header:
            self.host_header = self.provider.host_header

        self._last_rs = None
        self._auth_handler = auth.get_auth_handler(
            host, config, self.provider, self._required_auth_capability())
        if getattr(self, 'AuthServiceName', None) is not None:
            self.auth_service_name = self.AuthServiceName
        self.request_hook = None

    def __repr__(self):
        return '%s:%s' % (self.__class__.__name__, self.host)

    def _required_auth_capability(self):
        return []

    def _get_auth_service_name(self):
        return getattr(self._auth_handler, 'service_name')

    # For Sigv4, the auth_service_name/auth_region_name properties allow
    # the service_name/region_name to be explicitly set instead of being
    # derived from the endpoint url.
    def _set_auth_service_name(self, value):
        self._auth_handler.service_name = value
    auth_service_name = property(_get_auth_service_name, _set_auth_service_name)

    def _get_auth_region_name(self):
        return getattr(self._auth_handler, 'region_name')

    def _set_auth_region_name(self, value):
        self._auth_handler.region_name = value
    auth_region_name = property(_get_auth_region_name, _set_auth_region_name)

    def aws_access_key_id(self):
        return self.provider.access_key
    aws_access_key_id = property(aws_access_key_id)
    gs_access_key_id = aws_access_key_id
    access_key = aws_access_key_id

    def aws_secret_access_key(self):
        return self.provider.secret_key
    aws_secret_access_key = property(aws_secret_access_key)
    gs_secret_access_key = aws_secret_access_key
    secret_key = aws_secret_access_key

    def profile_name(self):
        return self.provider.profile_name
    profile_name = property(profile_name)

    def get_path(self, path='/'):
        # The default behavior is to suppress consecutive slashes for reasons
        # discussed at
        # https://groups.google.com/forum/#!topic/boto-dev/-ft0XPUy0y8
        # You can override that behavior with the suppress_consec_slashes param.
        if not self.suppress_consec_slashes:
            return self.path + re.sub('^(/*)/', "\\1", path)
        pos = path.find('?')
        if pos >= 0:
            params = path[pos:]
            path = path[:pos]
        else:
            params = None
        if path[-1] == '/':
            need_trailing = True
        else:
            need_trailing = False
        path_elements = self.path.split('/')
        path_elements.extend(path.split('/'))
        path_elements = [p for p in path_elements if p]
        path = '/' + '/'.join(path_elements)
        if path[-1] != '/' and need_trailing:
            path += '/'
        if params:
            path = path + params
        return path

    def server_name(self, port=None):
        if not port:
            port = self.port
        if port == 80:
            signature_host = self.host
        else:
            # This unfortunate little hack can be attributed to
            # a difference in the 2.6 version of http_client.  In old
            # versions, it would append ":443" to the hostname sent
            # in the Host header and so we needed to make sure we
            # did the same when calculating the V2 signature.  In 2.6
            # (and higher!)
            # it no longer does that.  Hence, this kludge.
            if ((ON_APP_ENGINE and sys.version[:3] == '2.5') or
                    sys.version[:3] in ('2.6', '2.7')) and port == 443:
                signature_host = self.host
            else:
                signature_host = '%s:%d' % (self.host, port)
        return signature_host

    def set_host_header(self, request):
        try:
            request.headers['Host'] = \
                self._auth_handler.host_header(self.host, request)
        except AttributeError:
            request.headers['Host'] = self.host.split(':', 1)[0]

    def set_request_hook(self, hook):
        self.request_hook = hook

    def build_base_http_request__(self, method, path, auth_path,
                                  params=None, headers=None, data='',
                                  host=None):
        path = self.get_path(path)
        if auth_path is not None:
            auth_path = self.get_path(auth_path)
        if params is None:
            params = {}
        else:
            params = params.copy()
        if headers is None:
            headers = {}
        else:
            headers = headers.copy()
        if self.host_header and not txboto.utils.find_matching_headers('host',
                                                                       headers):
            headers['host'] = self.host_header
        host = host or self.host

        # auth_path not sopported yet!!!!
        return self.prepare_request(method=method, host=host, path=path,
                                    params=params, headers=headers, data=data)

    def build_base_http_request(self, method, path, auth_path,
                                params=None, headers=None, body='', host=None):
        path = self.get_path(path)
        if auth_path is not None:
            auth_path = self.get_path(auth_path)
        if params is None:
            params = {}
        else:
            params = params.copy()
        if headers is None:
            headers = {}
        else:
            headers = headers.copy()
        if self.host_header and not txboto.utils.find_matching_headers('host',
                                                                       headers):
            headers['host'] = self.host_header
        host = host or self.host

        return HTTPRequest(method, self.protocol, host, self.port,
                           path, auth_path, params, headers, body)

    @defer.inlineCallbacks
    def _mexe(self, request, override_num_retries=1,
              retry_handler=None):
        """
        mexe - Multi-execute inside a loop, retrying multiple times to handle
               transient Internet errors by simply trying again.
               Also handles redirects.

        This code was inspired by the S3Utils classes posted to the txboto-users
        Google group by Larry Bates.  Thanks!

        """
        log.debug('Method: %s' % request.method)
        log.debug('Url: %s' % request.url)
        log.debug('Data: %s' % request.body)
        log.debug('Headers: %s' % request.headers)
        returnValue = None
        response = None
        body = None
        ex = None
        if override_num_retries is None:
            num_retries = config.getint('TxBoto', 'num_retries', self.num_retries)
        else:
            num_retries = override_num_retries
        i = 0
        while i <= num_retries:
            # Use binary exponential backoff to desynchronize client requests.
            next_sleep = min(random.random() * (2 ** i),
                             config.get('TxBoto', 'max_retry_delay', 60))
            try:
                request.authorize(connection=self)
                log.debug('Final headers: %s' % request.headers)
                request.start_time = datetime.now()

                response = yield self.send_request(request)
                response_body = yield response.content()
                response.reason = code2status(response.code, 'N/A')
                log.debug('Response headers: %s' % response.headers)
                location = response.headers.getRawHeaders('location')
                if location:
                    location = location[0]
                if callable(retry_handler):
                    status = yield defer.maybeDeferred(retry_handler, response,
                                                       response_body, i,
                                                       next_sleep)
                    if status:
                        msg, i, next_sleep = status
                        if msg:
                            log.debug(msg)
                        time.sleep(next_sleep)
                        continue
                if response.code in [500, 502, 503, 504]:
                    msg = 'Received %d response.  ' % response.code
                    msg += 'Retrying in %3.1f seconds' % next_sleep
                    log.debug(msg)
                    body = response_body
                    if isinstance(body, bytes):
                        body = body.decode('utf-8')
                elif response.code < 300 or response.code >= 400 or \
                        not location:
                    # don't return connection to the pool if response contains
                    # Connection:close header, because the connection has been
                    # closed and default reconnect behavior may do something
                    # different than new_http_connection. Also, it's probably
                    # less efficient to try to reuse a closed connection.
                    if self.request_hook is not None:
                        yield defer.maybeDeferred(
                            self.request_hook.handle_request_data,
                            request, response)
                    returnValue = (response, response_body,)
                    break
            except PleaseRetryException as e:
                log.debug('encountered a retry exception: {}'.foramt(e))
                response = e.response
                ex = e
            except self.http_exceptions as e:
                if isinstance(e, self.http_unretryable_exceptions):
                    log.debug('encountered unretryable {} exception, re-raising'
                              .format(e.__class__.__name__))
                    raise
                log.debug('encountered {} exception, reconnecting'
                          .format(e.__class__.__name__))
                ex = e
            time.sleep(next_sleep)
            i += 1

        if isinstance(returnValue, tuple):
            defer.returnValue(returnValue)
        # If we made it here, it's because we have exhausted our retries
        # and stil haven't succeeded.  So, if we have a response object,
        # use it to raise an exception.
        # Otherwise, raise the exception that must have already happened.
        if self.request_hook is not None:
            yield defer.maybeDeferred(self.request_hook.handle_request_data,
                                      request, response, error=True)
        if response:
            raise BotoServerError(response.status, response.reason, body)
        elif ex:
            raise ex
        else:
            msg = 'Please report this exception as a TxBoto Issue!'
            raise BotoClientError(msg)

    def make_request(self, method, path, headers=None, body='', host=None,
                     auth_path=None, sender=None, override_num_retries=None,
                     params=None, retry_handler=None):
        """Makes a request to the server, with stock multiple-retry logic."""
        if params is None:
            params = {}
        http_request = self.build_base_http_request(method, path, auth_path,
                                                    params, headers, body, host)
        return self._mexe(http_request, sender, override_num_retries,
                          retry_handler=retry_handler)

    def close(self):
        pass


class AWSQueryConnection(AWSAuthConnection):

    APIVersion = ''
    ResponseError = BotoServerError

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, host=None, debug=0,
                 https_connection_factory=None, path='/', security_token=None,
                 validate_certs=True, profile_name=None, provider='aws',
                 **kwargs):
        super(AWSQueryConnection, self).__init__(
            host, aws_access_key_id,
            aws_secret_access_key,
            is_secure, port, proxy,
            proxy_port, proxy_user, proxy_pass,
            debug, https_connection_factory, path,
            security_token=security_token,
            validate_certs=validate_certs,
            profile_name=profile_name,
            provider=provider,
            **kwargs)

    def _required_auth_capability(self):
        return []

    def get_utf8_value(self, value):
        return txboto.utils.get_utf8_value(value)

    def make_request(self, action, params=None, path='/', verb='GET'):
        http_request = self.build_base_http_request(verb, path, None,
                                                    params, {}, '',
                                                    self.host)
        if action:
            http_request.params['Action'] = action
        if self.APIVersion:
            http_request.params['Version'] = self.APIVersion
        return self._mexe(http_request)

    def build_list_params(self, params, items, label):
        if isinstance(items, six.string_types):
            items = [items]
        for i in range(1, len(items) + 1):
            params['%s.%d' % (label, i)] = items[i - 1]

    def build_complex_list_params(self, params, items, label, names):
        """Serialize a list of structures.

        For example::

            items = [('foo', 'bar', 'baz'), ('foo2', 'bar2', 'baz2')]
            label = 'ParamName.member'
            names = ('One', 'Two', 'Three')
            self.build_complex_list_params(params, items, label, names)

        would result in the params dict being updated with these params::

            ParamName.member.1.One = foo
            ParamName.member.1.Two = bar
            ParamName.member.1.Three = baz

            ParamName.member.2.One = foo2
            ParamName.member.2.Two = bar2
            ParamName.member.2.Three = baz2

        :type params: dict
        :param params: The params dict.  The complex list params
            will be added to this dict.

        :type items: list of tuples
        :param items: The list to serialize.

        :type label: string
        :param label: The prefix to apply to the parameter.

        :type names: tuple of strings
        :param names: The names associated with each tuple element.

        """
        for i, item in enumerate(items, 1):
            current_prefix = '%s.%s' % (label, i)
            for key, value in zip(names, item):
                full_key = '%s.%s' % (current_prefix, key)
                params[full_key] = value

    # generics

    def get_list(self, action, params, markers, path='/',
                 parent=None, verb='GET'):
        if not parent:
            parent = self
        response = self.make_request(action, params, path, verb)
        body = response.read()
        txboto.log.debug(body)
        if not body:
            txboto.log.error('Null body %s' % body)
            raise self.ResponseError(response.status, response.reason, body)
        elif response.status == 200:
            rs = ResultSet(markers)
            h = txboto.handler.XmlHandler(rs, parent)
            if isinstance(body, six.text_type):
                body = body.encode('utf-8')
            xml.sax.parseString(body, h)
            return rs
        else:
            txboto.log.error('%s %s' % (response.status, response.reason))
            txboto.log.error('%s' % body)
            raise self.ResponseError(response.status, response.reason, body)

    def get_object(self, action, params, cls, path='/',
                   parent=None, verb='GET'):
        if not parent:
            parent = self
        response = self.make_request(action, params, path, verb)
        body = response.read()
        txboto.log.debug(body)
        if not body:
            txboto.log.error('Null body %s' % body)
            raise self.ResponseError(response.status, response.reason, body)
        elif response.status == 200:
            obj = cls(parent)
            h = txboto.handler.XmlHandler(obj, parent)
            if isinstance(body, six.text_type):
                body = body.encode('utf-8')
            xml.sax.parseString(body, h)
            return obj
        else:
            txboto.log.error('%s %s' % (response.status, response.reason))
            txboto.log.error('%s' % body)
            raise self.ResponseError(response.status, response.reason, body)

    def get_status(self, action, params, path='/', parent=None, verb='GET'):
        if not parent:
            parent = self
        response = self.make_request(action, params, path, verb)
        body = response.read()
        txboto.log.debug(body)
        if not body:
            txboto.log.error('Null body %s' % body)
            raise self.ResponseError(response.status, response.reason, body)
        elif response.status == 200:
            rs = ResultSet()
            h = txboto.handler.XmlHandler(rs, parent)
            xml.sax.parseString(body, h)
            return rs.status
        else:
            txboto.log.error('%s %s' % (response.status, response.reason))
            txboto.log.error('%s' % body)
            raise self.ResponseError(response.status, response.reason, body)
