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


import six

from twisted.internet import defer


class Batch(object):
    """
    Used to construct a BatchGet request.

    :ivar table: The Table object from which the item is retrieved.

    :ivar keys: A list of scalar or tuple values.  Each element in the
        list represents one Item to retrieve.  If the schema for the
        table has both a HashKey and a RangeKey, each element in the
        list should be a tuple consisting of (hash_key, range_key).  If
        the schema for the table contains only a HashKey, each element
        in the list should be a scalar value of the appropriate type
        for the table schema. NOTE: The maximum number of items that
        can be retrieved for a single operation is 100. Also, the
        number of items retrieved is constrained by a 1 MB size limit.

    :ivar attributes_to_get: A list of attribute names.
        If supplied, only the specified attribute names will
        be returned.  Otherwise, all attributes will be returned.

    :ivar consistent_read: Specify whether or not to use a
        consistent read. Defaults to False.

    """

    def __init__(self, table, keys, attributes_to_get=None,
                 consistent_read=False):
        self.table = table
        self.keys = keys
        self.attributes_to_get = attributes_to_get
        self.consistent_read = consistent_read

    def to_dict(self):
        """
        Convert the Batch object into the format required for Layer1.
        """
        batch_dict = {}
        key_list = []
        for key in self.keys:
            if isinstance(key, tuple):
                hash_key, range_key = key
            else:
                hash_key = key
                range_key = None
            k = self.table.layer2.build_key_from_values(self.table.schema,
                                                        hash_key, range_key)
            key_list.append(k)
        batch_dict['Keys'] = key_list
        if self.attributes_to_get:
            batch_dict['AttributesToGet'] = self.attributes_to_get
        if self.consistent_read:
            batch_dict['ConsistentRead'] = True
        else:
            batch_dict['ConsistentRead'] = False
        return batch_dict


class BatchWrite(object):
    """
    Used to construct a BatchWrite request.  Each BatchWrite object
    represents a collection of PutItem and DeleteItem requests for
    a single Table.

    :ivar table: The Table object from which the item is retrieved.

    :ivar puts: A list of :class:`txboto.dynamodb.item.Item` objects
        that you want to write to DynamoDB.

    :ivar deletes: A list of scalar or tuple values.  Each element in the
        list represents one Item to delete.  If the schema for the
        table has both a HashKey and a RangeKey, each element in the
        list should be a tuple consisting of (hash_key, range_key).  If
        the schema for the table contains only a HashKey, each element
        in the list should be a scalar value of the appropriate type
        for the table schema.
    """

    def __init__(self, table, puts=None, deletes=None):
        self.table = table
        self.puts = puts or []
        self.deletes = deletes or []

    def to_dict(self):
        """
        Convert the Batch object into the format required for Layer1.
        """
        op_list = []
        for item in self.puts:
            d = {'Item': self.table.layer2.dynamize_item(item)}
            d = {'PutRequest': d}
            op_list.append(d)
        for key in self.deletes:
            if isinstance(key, tuple):
                hash_key, range_key = key
            else:
                hash_key = key
                range_key = None
            k = self.table.layer2.build_key_from_values(self.table.schema,
                                                        hash_key, range_key)
            d = {'Key': k}
            op_list.append({'DeleteRequest': d})
        return (self.table.name, op_list)


class BatchList(list):
    """
    A subclass of a list object that contains a collection of
    :class:`txboto.dynamodb.batch.Batch` objects.
    """

    def __init__(self, layer2):
        list.__init__(self)
        self.unprocessed = None
        self.layer2 = layer2

    def add_batch(self, table, keys, attributes_to_get=None,
                  consistent_read=False):
        """
        Add a Batch to this BatchList.

        :type table: :class:`txboto.dynamodb.table.Table`
        :param table: The Table object in which the items are contained.

        :type keys: list
        :param keys: A list of scalar or tuple values.  Each element in the
            list represents one Item to retrieve.  If the schema for the
            table has both a HashKey and a RangeKey, each element in the
            list should be a tuple consisting of (hash_key, range_key).  If
            the schema for the table contains only a HashKey, each element
            in the list should be a scalar value of the appropriate type
            for the table schema. NOTE: The maximum number of items that
            can be retrieved for a single operation is 100. Also, the
            number of items retrieved is constrained by a 1 MB size limit.

        :type attributes_to_get: list
        :param attributes_to_get: A list of attribute names.
            If supplied, only the specified attribute names will
            be returned.  Otherwise, all attributes will be returned.
        """
        self.append(Batch(table, keys, attributes_to_get, consistent_read))

    @defer.inlineCallbacks
    def resubmit(self):
        """
        Resubmit the batch to get the next result set. The request object is
        rebuild from scratch meaning that all batch added between ``submit``
        and ``resubmit`` will be lost.

        Note: This method is experimental and subject to changes in future releases
        """
        del self[:]

        if not self.unprocessed:
            return None

        for table_name, table_req in six.iteritems(self.unprocessed):
            table_keys = table_req['Keys']
            table = yield self.layer2.get_table(table_name)

            keys = []
            for key in table_keys:
                h = key['HashKeyElement']
                r = None
                if 'RangeKeyElement' in key:
                    r = key['RangeKeyElement']
                keys.append((h, r))

            attributes_to_get = None
            if 'AttributesToGet' in table_req:
                attributes_to_get = table_req['AttributesToGet']

            self.add_batch(table, keys, attributes_to_get=attributes_to_get)

        result = yield self.submit()
        defer.returnValue(result)

    @defer.inlineCallbacks
    def submit(self):
        result = yield self.layer2.batch_get_item(self)
        if 'UnprocessedKeys' in result:
            self.unprocessed = result['UnprocessedKeys']
        defer.returnValue(result)

    def to_dict(self):
        """
        Convert a BatchList object into format required for Layer1.
        """
        d = {}
        for batch in self:
            b = batch.to_dict()
            if b['Keys']:
                d[batch.table.name] = b
        return d


class BatchWriteList(list):
    """
    A subclass of a list object that contains a collection of
    :class:`txboto.dynamodb.batch.BatchWrite` objects.
    """

    def __init__(self, layer2):
        list.__init__(self)
        self.layer2 = layer2

    def add_batch(self, table, puts=None, deletes=None):
        """
        Add a BatchWrite to this BatchWriteList.

        :type table: :class:`txboto.dynamodb.table.Table`
        :param table: The Table object in which the items are contained.

        :type puts: list of :class:`txboto.dynamodb.item.Item` objects
        :param puts: A list of items that you want to write to DynamoDB.

        :type deletes: A list
        :param deletes: A list of scalar or tuple values.  Each element
            in the list represents one Item to delete.  If the schema
            for the table has both a HashKey and a RangeKey, each
            element in the list should be a tuple consisting of
            (hash_key, range_key).  If the schema for the table
            contains only a HashKey, each element in the list should
            be a scalar value of the appropriate type for the table
            schema.
        """
        self.append(BatchWrite(table, puts, deletes))

    @defer.inlineCallbacks
    def submit(self):
        result = yield self.layer2.batch_write_item(self)
        defer.returnValue(result)

    def to_dict(self):
        """
        Convert a BatchWriteList object into format required for Layer1.
        """
        d = {}
        for batch in self:
            table_name, batch_dict = batch.to_dict()
            d[table_name] = batch_dict
        return d
