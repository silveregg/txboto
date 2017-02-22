# Copyright (c) 2012 Amazon.com, Inc. or its affiliates.  All Rights Reserved
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
# flake8: noqa

import os

# This allows txboto modules to say "from txboto.compat import json".  This is
# preferred so that all modules don't have to repeat this idiom.
try:
    import simplejson as json
except ImportError:
    import json


# Switch to use encodebytes, which deprecates encodestring in Python 3
try:
    from base64 import encodebytes
except ImportError:
    from base64 import encodestring as encodebytes  # noqa


# If running in Google App Engine there is no "user" and
# os.path.expanduser() will fail. Attempt to detect this case and use a
# no-op expanduser function in this case.
try:
    os.path.expanduser('~')
    expanduser = os.path.expanduser
except (AttributeError, ImportError):
    # This is probably running on App Engine.
    expanduser = (lambda x: x)

import six

from six import BytesIO, StringIO
from six.moves import (filter, http_client, map, _thread,
                       urllib, zip)
from six.moves.queue import Queue
from six.moves.urllib.parse import (parse_qs, quote, unquote,
                                    urlparse, urlsplit, quote_plus,
                                    urlencode)
from six.moves.urllib.request import urlopen

if six.PY3:
    # StandardError was removed, so use the base exception type instead
    StandardError = Exception
    long_type = int
    from configparser import ConfigParser
else:
    StandardError = StandardError
    long_type = long
    from ConfigParser import SafeConfigParser as ConfigParser


def to_str(s, encoding='utf-8'):
    if six.PY3 and isinstance(s, bytes):
        return s.decode(encoding)
    return s
