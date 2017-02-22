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

"""
Handles connections to AWS
"""
from __future__ import absolute_import

from treq.client import HTTPClient

from twisted.internet import reactor, error
from twisted.web import error as web_error
from twisted.web.client import Agent, ProxyAgent, HTTPConnectionPool, \
    ResponseFailed, RequestTransmissionFailed, ResponseNeverReceived

from txboto import UserAgent, config, log
from txboto.compat import six, quote

from OpenSSL import SSL


def httpclient(*args, **kwargs):

    pool = HTTPConnectionPool(reactor, kwargs.get('persistent', True))

    if 'proxy' in kwargs and 'proxy_port' in kwargs:
        endpoint = '{}:{}'.format(kwargs['proxy'], kwargs['proxy_port'])
        agent = ProxyAgent(endpoint, reactor=reactor, pool=pool)
    else:
        agent = Agent(reactor=reactor, pool=pool)
    return HTTPClient(agent)


class HTTPRequest(object):

    def __init__(self, method, protocol, host, port, path, auth_path,
                 params, headers, body):
        """Represents an HTTP request.

        :type method: string
        :param method: The HTTP method name, 'GET', 'POST', 'PUT' etc.

        :type protocol: string
        :param protocol: The http protocol used, 'http' or 'https'.

        :type host: string
        :param host: Host to which the request is addressed. eg. abc.com

        :type port: int
        :param port: port on which the request is being sent. Zero means unset,
            in which case default port will be chosen.

        :type path: string
        :param path: URL path that is being accessed.

        :type auth_path: string
        :param path: The part of the URL path used when creating the
            authentication string.

        :type params: dict
        :param params: HTTP url query parameters, with key as name of
            the param, and value as value of param.

        :type headers: dict
        :param headers: HTTP headers, with key as name of the header and value
            as value of header.

        :type body: string
        :param body: Body of the HTTP request. If not present, will be None or
            empty string ('').
        """
        self.method = method
        self.protocol = protocol
        self.host = host
        self.port = port
        self.path = path
        if auth_path is None:
            auth_path = path
        self.auth_path = auth_path
        self.params = params
        # chunked Transfer-Encoding should act only on PUT request.
        if headers and 'Transfer-Encoding' in headers and \
                headers['Transfer-Encoding'] == 'chunked' and \
                self.method != 'PUT':
            self.headers = headers.copy()
            del self.headers['Transfer-Encoding']
        else:
            self.headers = headers
        self.body = body
        self.url = '{}://{}:{}{}'.format(self.protocol, self.host, self.port,
                                         self.path)

    def __str__(self):
        return (('method:(%s) protocol:(%s) host(%s) port(%s) path(%s) '
                 'params(%s) headers(%s) body(%s)') % (self.method,
                self.protocol, self.host, self.port, self.path, self.params,
                self.headers, self.body))

    def authorize(self, connection, **kwargs):
        if not getattr(self, '_headers_quoted', False):
            for key in self.headers:
                val = self.headers[key]
                if isinstance(val, six.text_type):
                    safe = '!"#$%&\'()*+,/:;<=>?@[\\]^`{|}~'
                    self.headers[key] = quote(val.encode('utf-8'), safe)
            setattr(self, '_headers_quoted', True)

        self.headers['User-Agent'] = UserAgent

        connection._auth_handler.add_auth(self, **kwargs)

        # I'm not sure if this is still needed, now that add_auth is
        # setting the content-length for POST requests.
        if 'Content-Length' not in self.headers:
            if 'Transfer-Encoding' not in self.headers or \
                    self.headers['Transfer-Encoding'] != 'chunked':
                self.headers['Content-Length'] = str(len(self.body))


class AWSBaseConnection(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key,
                 host, port=None, is_secure=True,
                 proxy=None, proxy_port=None,
                 http_pool=False, **kwargs):

        # Override passed-in is_secure setting if value was defined in config.
        if config.has_option('TxBoto', 'is_secure'):
            is_secure = config.getboolean('TxBoto', 'is_secure')

        self.is_secure = is_secure
        self.protocol = "https" if is_secure else 'http'
        self.proxy = proxy
        self.proxy_port = proxy_port

        self.host_header = None

        if port:
            self.port = port
        else:
            self.port = 443 if is_secure else 80
        self.host = host

        kw = {}
        kw['persistent'] = True if kwargs.get('persistent', True) else False

        if proxy and proxy_port:
            kw['proxy'] = proxy
            kw['proxy_port'] = proxy_port

        self.client = httpclient(**kw)

        self.http_exceptions = (error.ConnectError, web_error.Error,
                                error.ConnectionDone, error.ConnectionLost,
                                error.ConnectionRefusedError,
                                error.ConnectingCancelledError,
                                error.TimeoutError,
                                ResponseFailed, RequestTransmissionFailed,
                                ResponseNeverReceived)

        # define subclasses of the above that are not retryable.
        self.http_unretryable_exceptions = (SSL.Error,
                                            error.CertificateError,
                                            error.VerifyError)
        self.request_hook = None
        self.timeout = 60

    def send_request(self, http_request):

        headers = {}
        for k, v in http_request.headers.items():
            if six.PY3:
                headers[k] = v.encode("utf-8") if isinstance(v, str) else v
            else:
                headers[k] = v.encode("utf-8") if isinstance(v, unicode) else v

        if 'Content-Length' in http_request.headers:
            # This is most annoying bug in treq
            # It will add it's own Content-Length Header
            headers = dict(headers)
            del headers['Content-Length']

        return self.client.request(method=http_request.method,
                                   url=http_request.url,
                                   headers=headers,
                                   data=http_request.body,
                                   allow_redirects=False,
                                   timeout=self.timeout)
