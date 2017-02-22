# Copyright (c) 2012 Mitch Garnaat http://garnaat.org/
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

import time
from binascii import crc32

import txboto
from txboto.connection import AWSAuthConnection
from txboto.exception import DynamoDBResponseError
from txboto.provider import Provider
from txboto.dynamodb import exceptions as dynamodb_exceptions
from txboto.compat import json, to_str

from twisted.internet import defer


class Layer1(AWSAuthConnection):
    """
    This is the lowest-level interface to DynamoDB.  Methods at this
    layer map directly to API requests and parameters to the methods
    are either simple, scalar values or they are the Python equivalent
    of the JSON input as defined in the DynamoDB Developer's Guide.
    All responses are direct decoding of the JSON response bodies to
    Python data structures via the json or simplejson modules.

    :ivar throughput_exceeded_events: An integer variable that
        keeps a running total of the number of ThroughputExceeded
        responses this connection has received from Amazon DynamoDB.
    """

    DefaultRegionName = 'us-east-1'
    """The default region name for DynamoDB API."""

    ServiceName = 'DynamoDB'
    """The name of the Service"""

    Version = '20111205'
    """DynamoDB API version."""

    ThruputError = "ProvisionedThroughputExceededException"
    """The error response returned when provisioned throughput is exceeded"""

    SessionExpiredError = 'com.amazon.coral.service#ExpiredTokenException'
    """The error response returned when session token has expired"""

    ConditionalCheckFailedError = 'ConditionalCheckFailedException'
    """The error response returned when a conditional check fails"""

    ValidationError = 'ValidationException'
    """The error response returned when an item is invalid in some way"""

    ResponseError = DynamoDBResponseError

    NumberRetries = 10
    """The number of times an error is retried."""

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 debug=0, security_token=None, region=None,
                 validate_certs=True, validate_checksums=True, profile_name=None):
        if not region:
            region_name = txboto.config.get('DynamoDB', 'region',
                                            self.DefaultRegionName)
            for reg in txboto.dynamodb.regions():
                if reg.name == region_name:
                    region = reg
                    break

        self.region = region
        super(Layer1, self).__init__(self.region.endpoint,
                                     aws_access_key_id,
                                     aws_secret_access_key,
                                     is_secure, port, proxy, proxy_port,
                                     debug=debug, security_token=security_token,
                                     validate_certs=validate_certs,
                                     profile_name=profile_name)
        self.throughput_exceeded_events = 0
        self._validate_checksums = txboto.config.getbool(
            'DynamoDB', 'validate_checksums', validate_checksums)

    def _get_session_token(self):
        self.provider = Provider(self._provider_type)
        self._auth_handler.update_provider(self.provider)

    def _required_auth_capability(self):
        return ['hmac-v4']

    @defer.inlineCallbacks
    def make_request(self, action, body='', object_hook=None):
        """
        :raises: ``DynamoDBExpiredTokenError`` if the security token expires.
        """
        headers = {'X-Amz-Target': '%s_%s.%s' % (self.ServiceName,
                                                 self.Version, action),
                   'Host': self.region.endpoint,
                   'Content-Type': 'application/x-amz-json-1.0',
                   'Content-Length': str(len(body))}
        http_request = self.build_base_http_request('POST', '/', '/',
                                                    {}, headers, body, None)
        start = time.time()
        response, response_body = yield self._mexe(
            http_request, sender=None, override_num_retries=self.NumberRetries,
            retry_handler=self._retry_handler)
        elapsed = (time.time() - start) * 1000
        request_id = response.getheader('x-amzn-RequestId')
        txboto.log.debug('RequestId: %s' % request_id)
        txboto.perflog.debug('%s: id=%s time=%sms',
                             headers['X-Amz-Target'], request_id, int(elapsed))
        txboto.log.debug(response_body)
        defer.returnValue(json.loads(response_body, object_hook=object_hook))

    def _retry_handler(self, response, i, next_sleep):
        status = None
        if response.status == 400:
            response_body = response.read().decode('utf-8')
            txboto.log.debug(response_body)
            data = json.loads(to_str(response_body))
            if self.ThruputError in data.get('__type'):
                self.throughput_exceeded_events += 1
                msg = "%s, retry attempt %s" % (self.ThruputError, i)
                next_sleep = self._exponential_time(i)
                i += 1
                status = (msg, i, next_sleep)
                if i == self.NumberRetries:
                    # If this was our last retry attempt, raise
                    # a specific error saying that the throughput
                    # was exceeded.
                    raise dynamodb_exceptions.DynamoDBThroughputExceededError(
                        response.status, response.reason, data)
            elif self.SessionExpiredError in data.get('__type'):
                msg = 'Renewing Session Token'
                self._get_session_token()
                status = (msg, i + self.num_retries - 1, 0)
            elif self.ConditionalCheckFailedError in data.get('__type'):
                raise dynamodb_exceptions.DynamoDBConditionalCheckFailedError(
                    response.status, response.reason, data)
            elif self.ValidationError in data.get('__type'):
                raise dynamodb_exceptions.DynamoDBValidationError(
                    response.status, response.reason, data)
            else:
                raise self.ResponseError(response.status, response.reason,
                                         data)
        expected_crc32 = response.getheader('x-amz-crc32')
        if self._validate_checksums and expected_crc32 is not None:
            txboto.log.debug('Validating crc32 checksum for body: %s',
                             response.read().decode('utf-8'))
            actual_crc32 = crc32(response.read()) & 0xffffffff
            expected_crc32 = int(expected_crc32)
            if actual_crc32 != expected_crc32:
                msg = ("The calculated checksum %s did not match the expected "
                       "checksum %s" % (actual_crc32, expected_crc32))
                status = (msg, i + 1, self._exponential_time(i))
        return status

    def _exponential_time(self, i):
        if i == 0:
            next_sleep = 0
        else:
            next_sleep = min(0.05 * (2 ** i),
                             txboto.config.get('TxBoto', 'max_retry_delay', 60))
        return next_sleep

    @defer.inlineCallbacks
    def list_tables(self, limit=None, start_table=None):
        """
        Returns a dictionary of results.  The dictionary contains
        a **TableNames** key whose value is a list of the table names.
        The dictionary could also contain a **LastEvaluatedTableName**
        key whose value would be the last table name returned if
        the complete list of table names was not returned.  This
        value would then be passed as the ``start_table`` parameter on
        a subsequent call to this method.

        :type limit: int
        :param limit: The maximum number of tables to return.

        :type start_table: str
        :param start_table: The name of the table that starts the
            list.  If you ran a previous list_tables and not
            all results were returned, the response dict would
            include a LastEvaluatedTableName attribute.  Use
            that value here to continue the listing.
        """
        data = {}
        if limit:
            data['Limit'] = limit
        if start_table:
            data['ExclusiveStartTableName'] = start_table
        json_input = json.dumps(data)
        result = yield self.make_request('ListTables', json_input)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def describe_table(self, table_name):
        """
        Returns information about the table including current
        state of the table, primary key schema and when the
        table was created.

        :type table_name: str
        :param table_name: The name of the table to describe.
        """
        data = {'TableName': table_name}
        json_input = json.dumps(data)
        result = yield self.make_request('DescribeTable', json_input)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def create_table(self, table_name, schema, provisioned_throughput):
        """
        Add a new table to your account.  The table name must be unique
        among those associated with the account issuing the request.
        This request triggers an asynchronous workflow to begin creating
        the table.  When the workflow is complete, the state of the
        table will be ACTIVE.

        :type table_name: str
        :param table_name: The name of the table to create.

        :type schema: dict
        :param schema: A Python version of the KeySchema data structure
            as defined by DynamoDB

        :type provisioned_throughput: dict
        :param provisioned_throughput: A Python version of the
            ProvisionedThroughput data structure defined by
            DynamoDB.
        """
        data = {'TableName': table_name,
                'KeySchema': schema,
                'ProvisionedThroughput': provisioned_throughput}
        json_input = json.dumps(data)
        result = yield self.make_request('CreateTable', json_input)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def update_table(self, table_name, provisioned_throughput):
        """
        Updates the provisioned throughput for a given table.

        :type table_name: str
        :param table_name: The name of the table to update.

        :type provisioned_throughput: dict
        :param provisioned_throughput: A Python version of the
            ProvisionedThroughput data structure defined by
            DynamoDB.
        """
        data = {'TableName': table_name,
                'ProvisionedThroughput': provisioned_throughput}
        json_input = json.dumps(data)
        result = yield self.make_request('UpdateTable', json_input)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def delete_table(self, table_name):
        """
        Deletes the table and all of it's data.  After this request
        the table will be in the DELETING state until DynamoDB
        completes the delete operation.

        :type table_name: str
        :param table_name: The name of the table to delete.
        """
        data = {'TableName': table_name}
        json_input = json.dumps(data)
        result = yield self.make_request('DeleteTable', json_input)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_item(self, table_name, key, attributes_to_get=None,
                 consistent_read=False, object_hook=None):
        """
        Return a set of attributes for an item that matches
        the supplied key.

        :type table_name: str
        :param table_name: The name of the table containing the item.

        :type key: dict
        :param key: A Python version of the Key data structure
            defined by DynamoDB.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.

        :type consistent_read: bool
        :param consistent_read: If True, a consistent read
            request is issued.  Otherwise, an eventually consistent
            request is issued.
        """
        data = {'TableName': table_name,
                'Key': key}
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if consistent_read:
            data['ConsistentRead'] = True
        json_input = json.dumps(data)
        result = yield self.make_request('GetItem', json_input,
                                         object_hook=object_hook)
        if 'Item' not in result:
            raise dynamodb_exceptions.DynamoDBKeyNotFoundError(
                "Key does not exist."
            )
        defer.returnValue(result)

    @defer.inlineCallbacks
    def batch_get_item(self, request_items, object_hook=None):
        """
        Return a set of attributes for a multiple items in
        multiple tables using their primary keys.

        :type request_items: dict
        :param request_items: A Python version of the RequestItems
            data structure defined by DynamoDB.
        """
        # If the list is empty, return empty response
        if not request_items:
            return {}
        data = {'RequestItems': request_items}
        json_input = json.dumps(data)
        result = yield self.make_request('BatchGetItem', json_input,
                                         object_hook=object_hook)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def batch_write_item(self, request_items, object_hook=None):
        """
        This operation enables you to put or delete several items
        across multiple tables in a single API call.

        :type request_items: dict
        :param request_items: A Python version of the RequestItems
            data structure defined by DynamoDB.
        """
        data = {'RequestItems': request_items}
        json_input = json.dumps(data)
        result = yield self.make_request('BatchWriteItem', json_input,
                                         object_hook=object_hook)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def put_item(self, table_name, item,
                 expected=None, return_values=None,
                 object_hook=None):
        """
        Create a new item or replace an old item with a new
        item (including all attributes).  If an item already
        exists in the specified table with the same primary
        key, the new item will completely replace the old item.
        You can perform a conditional put by specifying an
        expected rule.

        :type table_name: str
        :param table_name: The name of the table in which to put the item.

        :type item: dict
        :param item: A Python version of the Item data structure
            defined by DynamoDB.

        :type expected: dict
        :param expected: A Python version of the Expected
            data structure defined by DynamoDB.

        :type return_values: str
        :param return_values: Controls the return of attribute
            name-value pairs before then were changed.  Possible
            values are: None or 'ALL_OLD'. If 'ALL_OLD' is
            specified and the item is overwritten, the content
            of the old item is returned.
        """
        data = {'TableName': table_name,
                'Item': item}
        if expected:
            data['Expected'] = expected
        if return_values:
            data['ReturnValues'] = return_values
        json_input = json.dumps(data)
        result = yield self.make_request('PutItem', json_input,
                                         object_hook=object_hook)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def update_item(self, table_name, key, attribute_updates,
                    expected=None, return_values=None,
                    object_hook=None):
        """
        Edits an existing item's attributes. You can perform a conditional
        update (insert a new attribute name-value pair if it doesn't exist,
        or replace an existing name-value pair if it has certain expected
        attribute values).

        :type table_name: str
        :param table_name: The name of the table.

        :type key: dict
        :param key: A Python version of the Key data structure
            defined by DynamoDB which identifies the item to be updated.

        :type attribute_updates: dict
        :param attribute_updates: A Python version of the AttributeUpdates
            data structure defined by DynamoDB.

        :type expected: dict
        :param expected: A Python version of the Expected
            data structure defined by DynamoDB.

        :type return_values: str
        :param return_values: Controls the return of attribute
            name-value pairs before then were changed.  Possible
            values are: None or 'ALL_OLD'. If 'ALL_OLD' is
            specified and the item is overwritten, the content
            of the old item is returned.
        """
        data = {'TableName': table_name,
                'Key': key,
                'AttributeUpdates': attribute_updates}
        if expected:
            data['Expected'] = expected
        if return_values:
            data['ReturnValues'] = return_values
        json_input = json.dumps(data)
        result = yield self.make_request('UpdateItem', json_input,
                                         object_hook=object_hook)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def delete_item(self, table_name, key,
                    expected=None, return_values=None,
                    object_hook=None):
        """
        Delete an item and all of it's attributes by primary key.
        You can perform a conditional delete by specifying an
        expected rule.

        :type table_name: str
        :param table_name: The name of the table containing the item.

        :type key: dict
        :param key: A Python version of the Key data structure
            defined by DynamoDB.

        :type expected: dict
        :param expected: A Python version of the Expected
            data structure defined by DynamoDB.

        :type return_values: str
        :param return_values: Controls the return of attribute
            name-value pairs before then were changed.  Possible
            values are: None or 'ALL_OLD'. If 'ALL_OLD' is
            specified and the item is overwritten, the content
            of the old item is returned.
        """
        data = {'TableName': table_name,
                'Key': key}
        if expected:
            data['Expected'] = expected
        if return_values:
            data['ReturnValues'] = return_values
        json_input = json.dumps(data)
        result = yield self.make_request('DeleteItem', json_input,
                                         object_hook=object_hook)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def query(self, table_name, hash_key_value, range_key_conditions=None,
              attributes_to_get=None, limit=None, consistent_read=False,
              scan_index_forward=True, exclusive_start_key=None,
              object_hook=None, count=False):
        """
        Perform a query of DynamoDB.  This version is currently punting
        and expecting you to provide a full and correct JSON body
        which is passed as is to DynamoDB.

        :type table_name: str
        :param table_name: The name of the table to query.

        :type hash_key_value: dict
        :param key: A DynamoDB-style HashKeyValue.

        :type range_key_conditions: dict
        :param range_key_conditions: A Python version of the
            RangeKeyConditions data structure.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.

        :type limit: int
        :param limit: The maximum number of items to return.

        :type count: bool
        :param count: If True, Amazon DynamoDB returns a total
            number of items for the Query operation, even if the
            operation has no matching items for the assigned filter.

        :type consistent_read: bool
        :param consistent_read: If True, a consistent read
            request is issued.  Otherwise, an eventually consistent
            request is issued.

        :type scan_index_forward: bool
        :param scan_index_forward: Specified forward or backward
            traversal of the index.  Default is forward (True).

        :type exclusive_start_key: list or tuple
        :param exclusive_start_key: Primary key of the item from
            which to continue an earlier query.  This would be
            provided as the LastEvaluatedKey in that query.
        """
        data = {'TableName': table_name,
                'HashKeyValue': hash_key_value}
        if range_key_conditions:
            data['RangeKeyCondition'] = range_key_conditions
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if limit:
            data['Limit'] = limit
        if count:
            data['Count'] = True
        if consistent_read:
            data['ConsistentRead'] = True
        if scan_index_forward:
            data['ScanIndexForward'] = True
        else:
            data['ScanIndexForward'] = False
        if exclusive_start_key:
            data['ExclusiveStartKey'] = exclusive_start_key
        json_input = json.dumps(data)
        result = yield self.make_request('Query', json_input,
                                         object_hook=object_hook)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def scan(self, table_name, scan_filter=None,
             attributes_to_get=None, limit=None,
             exclusive_start_key=None, object_hook=None, count=False):
        """
        Perform a scan of DynamoDB.  This version is currently punting
        and expecting you to provide a full and correct JSON body
        which is passed as is to DynamoDB.

        :type table_name: str
        :param table_name: The name of the table to scan.

        :type scan_filter: dict
        :param scan_filter: A Python version of the
            ScanFilter data structure.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.

        :type limit: int
        :param limit: The maximum number of items to evaluate.

        :type count: bool
        :param count: If True, Amazon DynamoDB returns a total
            number of items for the Scan operation, even if the
            operation has no matching items for the assigned filter.

        :type exclusive_start_key: list or tuple
        :param exclusive_start_key: Primary key of the item from
            which to continue an earlier query.  This would be
            provided as the LastEvaluatedKey in that query.
        """
        data = {'TableName': table_name}
        if scan_filter:
            data['ScanFilter'] = scan_filter
        if attributes_to_get:
            data['AttributesToGet'] = attributes_to_get
        if limit:
            data['Limit'] = limit
        if count:
            data['Count'] = True
        if exclusive_start_key:
            data['ExclusiveStartKey'] = exclusive_start_key
        json_input = json.dumps(data)
        result = yield self.make_request('Scan', json_input,
                                         object_hook=object_hook)
        defer.returnValue(result)
