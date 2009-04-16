import sys
import string
import socket
import select
import threading
import time
import re

import unittest

# The dollar ($) char adds implicit carriage-return/newline (\r\n).
#
# c means client connection.
# P means proxy upstream connection.
# d means proxy downstream connection.
# S means connection on fake, mock memcached server.
#
def debug(x):
    if True:
        print(x)

class MockServer(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.host     = ''
        self.port     = port
        self.backlog  = 5
        self.server   = None
        self.running  = False
        self.sessions = []

    def closeSessions(self):
        for s in self.sessions:
            s.close()
        self.sessions = []

    def close(self):
        self.running = False
        self.closeSessions()
        if self.server:
            self.server.close()
        self.server = None

    def run(self):
        self.running = True
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(self.backlog)

            while self.running:
                debug("MockServer running " + str(self.port))
                client, address = self.server.accept()
                c = MockSession(client, address, self)
                debug("MockServer accepted " + str(self.port))
                self.sessions.insert(0, c)
                c.start()

        except KeyboardInterrupt:
            self.close()
            raise
        except socket.error, (value, message):
            self.close()
            debug("MockServer socket error: " + message)
            sys.exit(1)

        self.close()

class MockSession(threading.Thread):
    def __init__(self, client, address, server):
        threading.Thread.__init__(self)
        self.server  = server
        self.client  = client
        self.address = address
        self.recvlen = 1000
        self.running     = 0
        self.running_max = 10
        self.received = []

    def run(self):
        input = [self.client]

        try:
            self.running = 1
            while (self.running > 0 and
                   self.running < self.running_max):
                debug("MockSession running (" + str(self.running) + ")")
                self.running = self.running + 1

                iready, oready, eready = select.select(input, [], [], 1)
                if len(eready) > 0:
                    debug("MockSession select eready...")
                    self.running = 0
                elif len(iready) > 0:
                    debug("MockSession recv...")
                    data = self.client.recv(self.recvlen)
                    debug("MockSession recv done:" + data)

                    if data and len(data) > 0:
                        self.latest()
                        self.received.append(data)
                    else:
                        debug("MockSession recv no data")
                        self.close()

        except KeyboardInterrupt:
            raise
        except:
            1

        if self.running >= self.running_max:
            print "MockSession running too long, shutting down"

        debug("MockSession closing")
        self.close()

    def latest(self):
        if self in self.server.sessions:
            self.server.sessions.remove(self)
        self.server.sessions.insert(0, self)

    def close(self):
        debug("MockSession close")
        self.running = 0
        if self.client:
            self.client.close()
        self.client = None

sys.setcheckinterval(0)
g_mock_server_port = 11311
g_mock_server = MockServer(g_mock_server_port)
g_mock_server.start()
time.sleep(1)

class TestProxy(unittest.TestCase):
    def __init__(self, x):
        unittest.TestCase.__init__(self, x)
        self.proxy_port = 11333
        self.clients = {}

    def mock_server(self):
        global g_mock_server
        return g_mock_server

    def setUp(self):
        """setUp"""

    def tearDown(self):
        self.mock_close()
        for k in self.clients:
            self.client_close(k)
        self.clients = []

    def client_connect(self, idx=0):
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(("127.0.0.1", self.proxy_port))
        self.clients[idx] = c

    def client_send(self, what, idx=0):
        debug("client sending " + what)
        self.clients[idx].send(what)

    def mock_send(self, what, session=None):
        debug("mock sending " + what)

        if session is None:
            session = self.mock_server().sessions[0]

        self.assertTrue(session.client is not None)

        session.client.send(what)

    def client_recv(self, what, idx=0):
        debug("client_recv expect: " + what)

        s = self.clients[idx].recv(1024)

        debug("client_recv actual: " + s);

        self.assertTrue(what == s or re.match(what, s) is not None)

    def mock_recv(self, what, session=None):
        debug("mock_recv expect: " + what)

        wait_max = 5
        message = ""

        if session is None:
            i = 1
            while len(self.mock_server().sessions) <= 0 and i < wait_max:
                time.sleep(i)
                i = i * 2

            if len(self.mock_server().sessions) <= 0 and i >= wait_max:
                debug("waiting too long for mock_recv session " + str(i))

            session = self.mock_server().sessions[0]

        i = 1
        while len(session.received) <= 0 and i < wait_max:
            debug("sleeping waiting for mock_recv " + str(i))
            time.sleep(i)
            i = i * 2

        if len(session.received) <= 0 and i >= wait_max:
            debug("waiting too long for mock_recv " + str(i))

        if len(session.received) > 0:
            message = session.received.pop(0)

        debug("mock_recv expect: " + what);
        debug("mock_recv actual: " + message);

        self.assertTrue(what == message or re.match(what, message) is not None)

        return session

    def wait(self, x):
        debug("wait " + str(x))
        time.sleep(0.01 * x)

    def client_close(self, idx=0):
        if self.clients[idx]:
            self.clients[idx].close()
        self.clients[idx] = None

    def mock_close(self):
        if self.mock_server():
            self.mock_server().closeSessions()

    def mock_quiet(self, session=None):
        if len(self.mock_server().sessions) <= 0:
            return True

        if session is None:
            session = self.mock_server().sessions[0]

        return len(session.received) <= 0

    # -------------------------------------------------

    def testBasicVersion(self):
        """Test version command does not reach mock server"""
        self.client_connect()
        self.client_send("version\r\n")
        self.client_recv("VERSION .*\r\n")
        self.assertTrue(self.mock_quiet())
        self.client_connect()
        self.client_send("version\r\n")
        self.client_recv("VERSION .*\r\n")
        self.assertTrue(self.mock_quiet())

    def testBasicSerialGet(self):
        """Test quit command does reach mock server"""
        self.client_connect()
        self.client_send("get keyNotThere\r\n")
        self.mock_recv('get keyNotThere\r\n')
        self.mock_send('END\r\n')
        self.client_recv("END\r\n")

        self.client_send("get keyNotThere\r\n")
        self.mock_recv('get keyNotThere\r\n')
        self.mock_send('END\r\n')
        self.client_recv("END\r\n")

        self.client_send("get keyNotThere\r\n")
        self.mock_recv('get keyNotThere\r\n')
        self.mock_send('END\r\n')
        self.client_recv("END\r\n")

    def testBasicQuit(self):
        """Test quit command does not reach mock server"""
        self.client_connect()
        self.client_send("get keyNotThere\r\n")
        self.mock_recv('get keyNotThere\r\n')
        self.mock_send('END\r\n')
        self.client_recv("END\r\n")

        self.client_send("quit\r\n")
        self.assertTrue(self.mock_quiet())

    def testBogusCommand(self):
        """Test bogus commands do not reach mock server"""
        self.client_connect()
        self.client_send('bogus\r\n')
        self.client_recv('.*ERROR.*\r\n')
        self.assertTrue(self.mock_quiet())

    def testSimpleSet(self):
        """Test simple set against mock server"""
        self.client_connect()
        self.client_send('set simpleSet 0 0 1\r\n')
        self.client_send('1\r\n')
        self.mock_recv('set simpleSet 0 0 1\r\n1\r\n')
        self.mock_send('STORED\r\n')
        self.client_recv('STORED\r\n')

    def testFlushAllBroadcast(self):
        """Test flush_all scatter/gather"""
        self.client_connect()
        self.client_send('flush_all\r\n')
        self.mock_recv('flush_all\r\n')
        self.mock_send('OK\r\n')
        self.client_recv('OK\r\n')

    def testSplitResponseOverSeveralWrites(self):
        """Test split a response over several writes"""
        self.client_connect()
        self.client_send('set splitResponse 0 0 1\r\n')
        self.client_send('1\r\n')
        self.mock_recv("set splitResponse 0 0 1\r\n1\r\n")
        self.mock_send('STO')
        self.wait(1)
        self.mock_send('RED')
        self.wait(1)
        self.mock_send('\r')
        self.wait(1)
        self.mock_send('\n')
        self.client_recv('STORED\r\n')

    def testSplitRequestOverSeveralWrites(self):
        """Test split a request over several writes"""
        self.client_connect()
        self.client_send('set splitRequest ')
        self.wait(1)
        self.client_send('0 0 5\r')
        self.wait(1)
        self.client_send('\n')
        self.wait(1)
        self.client_send('hel')
        self.wait(1)
        self.client_send('lo\r\n')
        self.mock_recv("set splitRequest 0 0 5\r\nhello\r\n")
        self.mock_send('STORED\r\n')
        self.client_recv('STORED\r\n')

    def testTerminateResponseWithServerClose(self):
        """Test chop the response with a server close"""
        self.client_connect()
        self.client_send('set chopped 0 0 1\r\n')
        self.client_send('1\r\n')
        self.mock_recv("set chopped 0 0 1\r\n1\r\n")
        self.mock_close()
        self.client_recv('.*ERROR .*\r\n')

    def testGetValue(self):
        """Test chop the VALUE response with a server close"""
        self.client_connect()
        self.client_send('get someVal\r\n')
        self.mock_recv("get someVal\r\n")
        self.mock_send('VALUE someVal 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.client_recv('VALUE someVal 0 10\r\n0123456789\r\n')

    def testTerminateResponseWithServerCloseInValue(self):
        """Test chop the VALUE response with a server close"""
        self.client_connect()
        self.client_send('get someChoppedVal\r\n')
        self.mock_recv("get someChoppedVal\r\n")
        self.mock_send('VALUE someChoppedVal 0 10\r\n')
        self.mock_send('012345')
        self.mock_close()
        self.client_recv('END\r\n')

# Test chopped up responses from multiple mock servers.
# Test chopped up requests from multiple clients.
# Test servers going down during multiget write.
# Test chopped up server responses during multiget.
# Test if one server goes down during a broadcast/scatter-gather.
# Test server going down.
# Test server coming back up.
# Test server getting very slow.
# Test several backend servers, and one or more start flapping.
## test get and multi-get
## test simple update commands
## test broadcast commands like flush_all

if __name__ == '__main__':
    unittest.main()

