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


class Schema(object):
    """
    Represents a DynamoDB schema.

    :ivar hash_key_name: The name of the hash key of the schema.
    :ivar hash_key_type: The DynamoDB type specification for the
        hash key of the schema.
    :ivar range_key_name: The name of the range key of the schema
        or None if no range key is defined.
    :ivar range_key_type: The DynamoDB type specification for the
        range key of the schema or None if no range key is defined.
    :ivar dict: The underlying Python dictionary that needs to be
        passed to Layer1 methods.
    """

    def __init__(self, schema_dict):
        self._dict = schema_dict

    def __repr__(self):
        if self.range_key_name:
            s = 'Schema(%s:%s)' % (self.hash_key_name, self.range_key_name)
        else:
            s = 'Schema(%s)' % self.hash_key_name
        return s

    @classmethod
    def create(cls, hash_key, range_key=None):
        """Convenience method to create a schema object.

        Example usage::

            schema = Schema.create(hash_key=('foo', 'N'))
            schema2 = Schema.create(hash_key=('foo', 'N'),
                                    range_key=('bar', 'S'))

        :type hash_key: tuple
        :param hash_key: A tuple of (hash_key_name, hash_key_type)

        :type range_key: tuple
        :param hash_key: A tuple of (range_key_name, range_key_type)

        """
        reconstructed = {
            'HashKeyElement': {
                'AttributeName': hash_key[0],
                'AttributeType': hash_key[1],
            }
        }
        if range_key is not None:
            reconstructed['RangeKeyElement'] = {
                'AttributeName': range_key[0],
                'AttributeType': range_key[1],
            }
        instance = cls(None)
        instance._dict = reconstructed
        return instance

    @property
    def dict(self):
        return self._dict

    @property
    def hash_key_name(self):
        return self._dict['HashKeyElement']['AttributeName']

    @property
    def hash_key_type(self):
        return self._dict['HashKeyElement']['AttributeType']

    @property
    def range_key_name(self):
        name = None
        if 'RangeKeyElement' in self._dict:
            name = self._dict['RangeKeyElement']['AttributeName']
        return name

    @property
    def range_key_type(self):
        type = None
        if 'RangeKeyElement' in self._dict:
            type = self._dict['RangeKeyElement']['AttributeType']
        return type

    def __eq__(self, other):
        return (self.hash_key_name == other.hash_key_name and
                self.hash_key_type == other.hash_key_type and
                self.range_key_name == other.range_key_name and
                self.range_key_type == other.range_key_type)
