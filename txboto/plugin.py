# Copyright 2010 Google Inc.
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
Implements plugin related api.

To define a new plugin just subclass Plugin, like this.

class AuthPlugin(Plugin):
    pass

Then start creating subclasses of your new plugin.

class MyFancyAuth(AuthPlugin):
    capability = ['sign', 'vmac']

The actual interface is duck typed.
"""

import glob
import imp
import os.path


class Plugin(object):
    """Base class for all plugins."""

    capability = []

    @classmethod
    def is_capable(cls, requested_capability):
        """Returns true if the requested capability is supported by this plugin
        """
        for c in requested_capability:
            if c not in cls.capability:
                return False
        return True


def get_plugin(cls, requested_capability=None):
    if not requested_capability:
        requested_capability = []
    result = []
    for handler in cls.__subclasses__():
        if handler.is_capable(requested_capability):
            result.append(handler)
    return result


def _import_module(filename):
    (path, name) = os.path.split(filename)
    (name, ext) = os.path.splitext(name)

    (file, filename, data) = imp.find_module(name, [path])
    try:
        return imp.load_module(name, file, filename, data)
    finally:
        if file:
            file.close()

_plugin_loaded = False


def load_plugins(config):
    global _plugin_loaded
    if _plugin_loaded:
        return
    _plugin_loaded = True

    if not config.has_option('Plugin', 'plugin_directory'):
        return
    directory = config.get('Plugin', 'plugin_directory')
    for file in glob.glob(os.path.join(directory, '*.py')):
        _import_module(file)
