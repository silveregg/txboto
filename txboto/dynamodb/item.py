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


from txboto.dynamodb.exceptions import DynamoDBItemError

from twisted.internet import defer


class Item(dict):
    """
    An item in Amazon DynamoDB.

    :ivar hash_key: The HashKey of this item.
    :ivar range_key: The RangeKey of this item or None if no RangeKey
        is defined.
    :ivar hash_key_name: The name of the HashKey associated with this item.
    :ivar range_key_name: The name of the RangeKey associated with this item.
    :ivar table: The Table this item belongs to.
    """

    def __init__(self, table, hash_key=None, range_key=None, attrs=None):
        self.table = table
        self._updates = None
        self._hash_key_name = self.table.schema.hash_key_name
        self._range_key_name = self.table.schema.range_key_name
        if attrs is None:
            attrs = {}
        if hash_key is None:
            hash_key = attrs.get(self._hash_key_name, None)
        self[self._hash_key_name] = hash_key
        if self._range_key_name:
            if range_key is None:
                range_key = attrs.get(self._range_key_name, None)
            self[self._range_key_name] = range_key
        self._updates = {}
        for key, value in attrs.items():
            if key != self._hash_key_name and key != self._range_key_name:
                self[key] = value
        self.consumed_units = 0

    @property
    def hash_key(self):
        return self[self._hash_key_name]

    @property
    def range_key(self):
        return self.get(self._range_key_name)

    @property
    def hash_key_name(self):
        return self._hash_key_name

    @property
    def range_key_name(self):
        return self._range_key_name

    def add_attribute(self, attr_name, attr_value):
        """
        Queue the addition of an attribute to an item in DynamoDB.
        This will eventually result in an UpdateItem request being issued
        with an update action of ADD when the save method is called.

        :type attr_name: str
        :param attr_name: Name of the attribute you want to alter.

        :type attr_value: int|long|float|set
        :param attr_value: Value which is to be added to the attribute.
        """
        self._updates[attr_name] = ("ADD", attr_value)

    def delete_attribute(self, attr_name, attr_value=None):
        """
        Queue the deletion of an attribute from an item in DynamoDB.
        This call will result in a UpdateItem request being issued
        with update action of DELETE when the save method is called.

        :type attr_name: str
        :param attr_name: Name of the attribute you want to alter.

        :type attr_value: set
        :param attr_value: A set of values to be removed from the attribute.
            This parameter is optional. If None, the whole attribute is
            removed from the item.
        """
        self._updates[attr_name] = ("DELETE", attr_value)

    def put_attribute(self, attr_name, attr_value):
        """
        Queue the putting of an attribute to an item in DynamoDB.
        This call will result in an UpdateItem request being issued
        with the update action of PUT when the save method is called.

        :type attr_name: str
        :param attr_name: Name of the attribute you want to alter.

        :type attr_value: int|long|float|str|set
        :param attr_value: New value of the attribute.
        """
        self._updates[attr_name] = ("PUT", attr_value)

    @defer.inlineCallbacks
    def save(self, expected_value=None, return_values=None):
        """
        Commits pending updates to Amazon DynamoDB.

        :type expected_value: dict
        :param expected_value: A dictionary of name/value pairs that
            you expect.  This dictionary should have name/value pairs
            where the name is the name of the attribute and the value is
            either the value you are expecting or False if you expect
            the attribute not to exist.

        :type return_values: str
        :param return_values: Controls the return of attribute name/value pairs
            before they were updated. Possible values are: None, 'ALL_OLD',
            'UPDATED_OLD', 'ALL_NEW' or 'UPDATED_NEW'. If 'ALL_OLD' is
            specified and the item is overwritten, the content of the old item
            is returned. If 'ALL_NEW' is specified, then all the attributes of
            the new version of the item are returned. If 'UPDATED_NEW' is
            specified, the new versions of only the updated attributes are
            returned.
        """
        result = yield self.table.layer2.update_item(self, expected_value,
                                                     return_values)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def delete(self, expected_value=None, return_values=None):
        """
        Delete the item from DynamoDB.

        :type expected_value: dict
        :param expected_value: A dictionary of name/value pairs that
            you expect.  This dictionary should have name/value pairs
            where the name is the name of the attribute and the value
            is either the value you are expecting or False if you expect
            the attribute not to exist.

        :type return_values: str
        :param return_values: Controls the return of attribute
            name-value pairs before then were changed.  Possible
            values are: None or 'ALL_OLD'. If 'ALL_OLD' is
            specified and the item is overwritten, the content
            of the old item is returned.
        """
        result = yield self.table.layer2.delete_item(self, expected_value,
                                                     return_values)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def put(self, expected_value=None, return_values=None):
        """
        Store a new item or completely replace an existing item
        in Amazon DynamoDB.

        :type expected_value: dict
        :param expected_value: A dictionary of name/value pairs that
            you expect.  This dictionary should have name/value pairs
            where the name is the name of the attribute and the value
            is either the value you are expecting or False if you expect
            the attribute not to exist.

        :type return_values: str
        :param return_values: Controls the return of attribute
            name-value pairs before then were changed.  Possible
            values are: None or 'ALL_OLD'. If 'ALL_OLD' is
            specified and the item is overwritten, the content
            of the old item is returned.
        """
        result = yield self.table.layer2.put_item(self, expected_value,
                                                  return_values)
        defer.returnValue(result)

    def __setitem__(self, key, value):
        """Overrwrite the setter to instead update the _updates
        method so this can act like a normal dict"""
        if self._updates is not None:
            self.put_attribute(key, value)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        """Remove this key from the items"""
        if self._updates is not None:
            self.delete_attribute(key)
        dict.__delitem__(self, key)

    # Allow this item to still be pickled
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)
