# Copyright (c) 2014 Amazon.com, Inc. or its affiliates.  All Rights Reserved
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


from twisted.internet import defer


class ResultSet(object):
    """
    A class used to lazily handle page-to-page navigation through a set of
    results.

    It presents a transparent iterator interface, so that all the user has
    to do is use it in a typical ``for`` loop (or list comprehension, etc.)
    to fetch results, even if they weren't present in the current page of
    results.

    This is used by the ``Table.query`` & ``Table.scan`` methods.

    Example::

        >>> users = Table('users')
        >>> results = ResultSet()
        >>> results.to_call(users.query, username__gte='johndoe')
        # Now iterate. When it runs out of results, it'll fetch the next page.
        >>> for res in results:
        ...     print res['username']

    """
    def __init__(self, max_page_size=None):
        super(ResultSet, self).__init__()
        self.the_callable = None
        self.call_args = []
        self.call_kwargs = {}
        self._results = []
        self._offset = 0
        self._results_left = True
        self._last_key_seen = None
        self._fetches = 0
        self._max_page_size = max_page_size
        self._limit = None

    @property
    def first_key(self):
        return 'exclusive_start_key'

    def _reset(self):
        """
        Resets the internal state of the ``ResultSet``.

        This prevents results from being cached long-term & consuming
        excess memory.

        Largely internal.
        """
        self._results = []
        self._offset = 0

    def __iter__(self):
        return self

    def __next__(self):
        """
            this will either return a result or a deferred.
            if a deferred is returned we have to yield e.g.:

            for r in resultset:
                if isinstance(r, defer.Deferred):
                    yield r
                else:
                    # do your things...

        """
        if self._offset >= len(self._results):
            if self._results_left is False:
                raise StopIteration()

            # this will return a deferred so we have to yield in the for loop!!
            return self.fetch_more()

            # It's possible that previous call to ``fetch_more`` may not return
            # anything useful but there may be more results. Loop until we get
            # something back, making sure we guard for no results left.
            while not len(self._results) and self._results_left:
                # this will return a deferred so we have to yield in the for loop!!
                return self.fetch_more()

        if self._offset < len(self._results):
            if self._limit is not None:
                self._limit -= 1

                if self._limit < 0:
                    raise StopIteration()

            self._offset += 1
            return self._results[self._offset - 1]
        else:
            raise StopIteration()

    next = __next__

    def to_call(self, the_callable, *args, **kwargs):
        """
        Sets up the callable & any arguments to run it with.

        This is stored for subsequent calls so that those queries can be
        run without requiring user intervention.

        Example::

            # Just an example callable.
            >>> def squares_to(y):
            ...     for x in range(1, y):
            ...         yield x**2
            >>> rs = ResultSet()
            # Set up what to call & arguments.
            >>> rs.to_call(squares_to, y=3)

        """
        if not callable(the_callable):
            raise ValueError(
                'You must supply an object or function to be called.'
            )

        # We pop the ``limit``, if present, to track how many we should return
        # to the user. This isn't the same as the ``limit`` that the low-level
        # DDB api calls use (which limit page size, not the overall result set).
        self._limit = kwargs.pop('limit', None)

        if self._limit is not None and self._limit < 0:
            self._limit = None

        self.the_callable = the_callable
        self.call_args = args
        self.call_kwargs = kwargs

    @defer.inlineCallbacks
    def fetch_more(self):
        """
        When the iterator runs out of results, this method is run to re-execute
        the callable (& arguments) to fetch the next page.

        Largely internal.
        """
        self._reset()

        args = self.call_args[:]
        kwargs = self.call_kwargs.copy()

        if self._last_key_seen is not None:
            kwargs[self.first_key] = self._last_key_seen

        # If the page size is greater than limit set them
        #   to the same value
        if (self._limit and self._max_page_size and
                self._max_page_size > self._limit):
            self._max_page_size = self._limit

        # Put in the max page size.
        if self._max_page_size is not None:
            kwargs['limit'] = self._max_page_size
        elif self._limit is not None:
            # If max_page_size is not set and limit is available
            #   use it as the page size
            kwargs['limit'] = self._limit

        results = yield defer.maybeDeferred(self.the_callable, *args, **kwargs)
        self._fetches += 1
        new_results = results.get('results', [])
        self._last_key_seen = results.get('last_key', None)

        if len(new_results):
            self._results.extend(results['results'])

        # Check the limit, if it's present.
        if self._limit is not None and self._limit >= 0:
            limit = self._limit
            limit -= len(results['results'])
            # If we've exceeded the limit, we don't have any more
            # results to look for.
            if limit <= 0:
                self._results_left = False

        if self._last_key_seen is None:
            self._results_left = False


class BatchGetResultSet(ResultSet):
    def __init__(self, *args, **kwargs):
        self._keys_left = kwargs.pop('keys', [])
        self._max_batch_get = kwargs.pop('max_batch_get', 100)
        super(BatchGetResultSet, self).__init__(*args, **kwargs)

    @defer.inlineCallbacks
    def fetch_more(self):
        self._reset()

        args = self.call_args[:]
        kwargs = self.call_kwargs.copy()

        # Slice off the max we can fetch.
        kwargs['keys'] = self._keys_left[:self._max_batch_get]
        self._keys_left = self._keys_left[self._max_batch_get:]

        if len(self._keys_left) <= 0:
            self._results_left = False

        results = yield defer.maybeDeferred(self.the_callable, *args, **kwargs)

        if not len(results.get('results', [])):
            defer.returnValue(None)

        self._results.extend(results['results'])

        for offset, key_data in enumerate(results.get('unprocessed_keys', [])):
            # We've got an unprocessed key. Reinsert it into the list.
            # DynamoDB only returns valid keys, so there should be no risk of
            # missing keys ever making it here.
            self._keys_left.insert(offset, key_data)

        if len(self._keys_left) > 0:
            self._results_left = True

        # Decrease the limit, if it's present.
        if self.call_kwargs.get('limit'):
            self.call_kwargs['limit'] -= len(results['results'])
