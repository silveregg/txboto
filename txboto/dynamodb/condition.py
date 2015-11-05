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


from txboto.dynamodb.types import dynamize_value


class Condition(object):
    """
    Base class for conditions.  Doesn't do a darn thing but allows
    is to test if something is a Condition instance or not.
    """

    def __eq__(self, other):
        if isinstance(other, Condition):
            return self.to_dict() == other.to_dict()


class ConditionNoArgs(Condition):
    """
    Abstract class for Conditions that require no arguments, such
    as NULL or NOT_NULL.
    """

    def __repr__(self):
        return '%s' % self.__class__.__name__

    def to_dict(self):
        return {'ComparisonOperator': self.__class__.__name__}


class ConditionOneArg(Condition):
    """
    Abstract class for Conditions that require a single argument
    such as EQ or NE.
    """

    def __init__(self, v1):
        self.v1 = v1

    def __repr__(self):
        return '%s:%s' % (self.__class__.__name__, self.v1)

    def to_dict(self):
        return {'AttributeValueList': [dynamize_value(self.v1)],
                'ComparisonOperator': self.__class__.__name__}


class ConditionTwoArgs(Condition):
    """
    Abstract class for Conditions that require two arguments.
    The only example of this currently is BETWEEN.
    """

    def __init__(self, v1, v2):
        self.v1 = v1
        self.v2 = v2

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self.v1, self.v2)

    def to_dict(self):
        values = (self.v1, self.v2)
        return {'AttributeValueList': [dynamize_value(v) for v in values],
                'ComparisonOperator': self.__class__.__name__}


class ConditionSeveralArgs(Condition):
    """
    Abstract class for conditions that require several argument (ex: IN).
    """

    def __init__(self, values):
        self.values = values

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,
                                 ', '.join(self.values))

    def to_dict(self):
        return {'AttributeValueList': [dynamize_value(v) for v in self.values],
                'ComparisonOperator': self.__class__.__name__}


class EQ(ConditionOneArg):

    pass


class NE(ConditionOneArg):

    pass


class LE(ConditionOneArg):

    pass


class LT(ConditionOneArg):

    pass


class GE(ConditionOneArg):

    pass


class GT(ConditionOneArg):

    pass


class NULL(ConditionNoArgs):

    pass


class NOT_NULL(ConditionNoArgs):

    pass


class CONTAINS(ConditionOneArg):

    pass


class NOT_CONTAINS(ConditionOneArg):

    pass


class BEGINS_WITH(ConditionOneArg):

    pass


class IN(ConditionSeveralArgs):

    pass


class BETWEEN(ConditionTwoArgs):

    pass
