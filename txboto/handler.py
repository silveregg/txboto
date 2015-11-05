# Copyright (c) 2006,2007 Mitch Garnaat http://garnaat.org/
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


import xml.sax

from txboto.compat import StringIO


class XmlHandler(xml.sax.ContentHandler):

    def __init__(self, root_node, connection):
        self.connection = connection
        self.nodes = [('root', root_node)]
        self.current_text = ''

    def startElement(self, name, attrs):
        self.current_text = ''
        new_node = self.nodes[-1][1].startElement(name, attrs, self.connection)
        if new_node is not None:
            self.nodes.append((name, new_node))

    def endElement(self, name):
        self.nodes[-1][1].endElement(name, self.current_text, self.connection)
        if self.nodes[-1][0] == name:
            if hasattr(self.nodes[-1][1], 'endNode'):
                self.nodes[-1][1].endNode(self.connection)
            self.nodes.pop()
        self.current_text = ''

    def characters(self, content):
        self.current_text += content


class XmlHandlerWrapper(object):
    def __init__(self, root_node, connection):
        self.handler = XmlHandler(root_node, connection)
        self.parser = xml.sax.make_parser()
        self.parser.setContentHandler(self.handler)
        self.parser.setFeature(xml.sax.handler.feature_external_ges, 0)

    def parseString(self, content):
        return self.parser.parse(StringIO(content))
