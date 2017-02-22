# Copyright (c) 2006-2012 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2010-2011, Eucalyptus Systems, Inc.
# Copyright (c) 2011, Nexenta Systems Inc.
# Copyright (c) 2012 Amazon.com, Inc. or its affiliates.
# Copyright (c) 2010, Google, Inc.
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


from txboto.pyami.config import Config
import txboto.plugin
import datetime
import os
import platform
import re
import logging
import logging.config

from twisted.python import log as twisted_log

__version__ = "0.1.2"

__title__ = "txboto"
__description__ = "AWS implementation for Twisted, based on Boto"
__uri__ = "https://www.github.com/silveregg/txboto"

__author__ = "Michael Franke"
__email__ = "michael@silveregg.co.jp"

__license__ = "BSD-3"

# http://bugs.python.org/issue7980
datetime.datetime.strptime('', '')

UserAgent = 'TxBoto/%s Python/%s %s/%s' % (
    __version__,
    platform.python_version(),
    platform.system(),
    platform.release()
)
config = Config()

# Regex to disallow buckets violating charset or not [3..255] chars total.
BUCKET_NAME_RE = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9\._-]{1,253}[a-zA-Z0-9]$')
# Regex to disallow buckets with individual DNS labels longer than 63.
TOO_LONG_DNS_NAME_COMP = re.compile(r'[-_a-z0-9]{64}')
GENERATION_RE = re.compile(r'(?P<versionless_uri_str>.+)'
                           r'#(?P<generation>[0-9]+)$')
VERSION_RE = re.compile('(?P<versionless_uri_str>.+)#(?P<version_id>.+)$')
ENDPOINTS_PATH = os.path.join(os.path.dirname(__file__), 'endpoints.json')


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


class TwistedHandler(logging.Handler):

    def handle(self, record):
        self.emit(record)

    def emit(self, record):
        msg = repr('[{}] {}'.format(record.name, record.msg))
        twisted_log.msg(msg, logLevel=record.levelno)


log = logging.getLogger('TxBoto')
perflog = logging.getLogger('TxBoto.perf')
log.addHandler(TwistedHandler())
perflog.addHandler(TwistedHandler())


# convenience function to set logging to a particular file


def set_file_logger(name, filepath, level=logging.INFO, format_string=None):
    global log
    if not format_string:
        format_string = "%(asctime)s %(name)s [%(levelname)s]:%(message)s"
    logger = logging.getLogger(name)
    logger.setLevel(level)
    fh = logging.FileHandler(filepath)
    fh.setLevel(level)
    formatter = logging.Formatter(format_string)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    log = logger


def set_stream_logger(name, level=logging.DEBUG, format_string=None):
    global log
    if not format_string:
        format_string = "%(asctime)s %(name)s [%(levelname)s]:%(message)s"
    logger = logging.getLogger(name)
    logger.setLevel(level)
    fh = logging.StreamHandler()
    fh.setLevel(level)
    formatter = logging.Formatter(format_string)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    log = logger


def connect_dynamodb(aws_access_key_id=None,
                     aws_secret_access_key=None,
                     **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`txboto.dynamodb.layer2.Layer2`
    :return: A connection to the Layer2 interface for DynamoDB.
    """
    from txboto.dynamodb.layer1 import Layer1
    return Layer1(aws_access_key_id, aws_secret_access_key, **kwargs)


def connect_dynamodb2(aws_access_key_id=None,
                      aws_secret_access_key=None,
                      **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`txboto.dynamodb.layer2.Layer2`
    :return: A connection to the Layer2 interface for DynamoDB.
    """
    from txboto.dynamodb2.layer1 import DynamoDBConnection
    return DynamoDBConnection(aws_access_key_id, aws_secret_access_key, **kwargs)


def connect_kinesis(aws_access_key_id=None,
                    aws_secret_access_key=None,
                    **kwargs):
    """
    Connect to Amazon Kinesis

    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    rtype: :class:`txboto.kinesis.layer1.KinesisConnection`
    :return: A connection to the Amazon Kinesis service
    """
    from txboto.kinesis.layer1 import KinesisConnection
    return KinesisConnection(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        **kwargs
    )

txboto.plugin.load_plugins(config)
