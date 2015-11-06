# Copyright (c) 2011 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2011 Amazon.com, Inc. or its affiliates.  All Rights Reserved
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
Exceptions that are specific to the dynamodb module.
"""
from txboto.exception import BotoServerError, BotoClientError
from txboto.exception import DynamoDBResponseError


class DynamoDBExpiredTokenError(BotoServerError):
    """
    Raised when a DynamoDB security token expires. This is generally txboto's
    (or the user's) notice to renew their DynamoDB security tokens.
    """
    pass


class DynamoDBKeyNotFoundError(BotoClientError):
    """
    Raised when attempting to retrieve or interact with an item whose key
    can't be found.
    """
    pass


class DynamoDBItemError(BotoClientError):
    """
    Raised when invalid parameters are passed when creating a
    new Item in DynamoDB.
    """
    pass


class DynamoDBNumberError(BotoClientError):
    """
    Raised in the event of incompatible numeric type casting.
    """
    pass


class DynamoDBConditionalCheckFailedError(DynamoDBResponseError):
    """
    Raised when a ConditionalCheckFailedException response is received.
    This happens when a conditional check, expressed via the expected_value
    paramenter, fails.
    """
    pass


class DynamoDBValidationError(DynamoDBResponseError):
    """
    Raised when a ValidationException response is received. This happens
    when one or more required parameter values are missing, or if the item
    has exceeded the 64Kb size limit.
    """
    pass


class DynamoDBThroughputExceededError(DynamoDBResponseError):
    """
    Raised when the provisioned throughput has been exceeded.
    Normally, when provisioned throughput is exceeded the operation
    is retried.  If the retries are exhausted then this exception
    will be raised.
    """
    pass
