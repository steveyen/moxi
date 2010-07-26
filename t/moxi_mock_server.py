import sys
import string
import socket
import select
import unittest
import threading
import time
import re

def debug(level, x):
    if level < 1:
        print(x)

# A fake memcached server.
#
class MockServer(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.host     = ''
        self.port     = port
        self.backlog  = 5
        self.server   = None
        self.running  = False
        self.sessions = {}

    def closeSessions(self):
        sessions = self.sessions # Snapshot to avoid concurrent iteration mods.
        self.sessions = {}

        for k in sessions:
            sessions[k].close()

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
                debug(0, "MockServer running " + str(self.port))
                client, address = self.server.accept()
                c = MockSession(client, address, self)
                debug(0, "MockServer accepted " + str(self.port))
                self.sessions[len(self.sessions)] = c
                c.start()

        except KeyboardInterrupt:
            self.close()
            raise
        except socket.error, (value, message):
            self.close()
            debug(1, "MockServer socket error: " + message)
            sys.exit(1)

        self.close()

# A session in the fake memcached server.
#
class MockSession(threading.Thread):
    def __init__(self, client, address, server, recvlen_in=1024):
        threading.Thread.__init__(self)
        self.server  = server
        self.client  = client
        self.address = address
        self.recvlen = recvlen_in
        self.running     = 0
        self.running_max = 10
        self.received = []

    def run(self):
        input = [self.client]

        try:
            self.running = 1
            while (self.running > 0 and
                   self.running < self.running_max):
                debug(1, "MockSession running (" + str(self.running) + ")")
                self.running = self.running + 1

                iready, oready, eready = select.select(input, [], [], 1)
                if len(eready) > 0:
                    debug(1, "MockSession select eready...")
                    self.running = 0
                elif len(iready) > 0:
                    debug(1, "MockSession recv...")
                    data = self.client.recv(self.recvlen)
                    debug(1, "MockSession recv done:" + data)

                    if data and len(data) > 0:
                        self.received.append(data)
                    else:
                        debug(1, "MockSession recv no data")
                        self.close()

        except KeyboardInterrupt:
            raise
        except:
            1

        if self.running >= self.running_max:
            debug(1, "MockSession running too long, shutting down")

        debug(1, "MockSession closing")
        self.close()

    def close(self):
        debug(1, "MockSession close")
        self.running = 0
        if self.client:
            self.client.close()
        self.client = None

# Start a fake memcached server...
#
sys.setcheckinterval(0)
g_mock_server_port = 11311
g_mock_server = MockServer(g_mock_server_port)
g_mock_server.start()
time.sleep(1)

class ProxyClientBase(unittest.TestCase):
    vbucketId = 0

    def __init__(self, x):
        unittest.TestCase.__init__(self, x)

        # These tests assume a moxi proxy is running at
        # the self.proxy_port and is forwarding requests
        # to our fake memcached server.
        #
        # TODO: Fork a moxi proxy like the perl tests.
        #
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
        return c

    def client_send(self, what, idx=0):
        debug(1, "client sending " + what)
        self.clients[idx].send(what)

    def mock_send(self, what, session_idx=0):
        debug(1, "mock sending " + what)

        session = self.mock_server().sessions[session_idx]

        self.assertTrue(session.client is not None)

        session.client.send(what)

    def client_recv(self, what, idx=0, num_bytes=1024):
        debug(1, "client_recv expect: " + what)

        s = self.clients[idx].recv(num_bytes)

        debug(1, "client_recv actual: " + s);

        self.assertTrue(what == s or re.match(what, s) is not None)

    def mock_session(self, session_idx=0):
        wait_max = 5

        i = 1
        while len(self.mock_server().sessions) <= session_idx and i < wait_max:
            time.sleep(i)
            i = i * 2

        if len(self.mock_server().sessions) <= session_idx and i >= wait_max:
            debug(1, "waiting too long for mock_session " + str(i))

        return self.mock_server().sessions[session_idx]

    def mock_recv_message(self, what, session_idx=0):
        debug(1, "mock_recv expect: " + what)

        session = self.mock_session(session_idx)

        wait_max = 5
        i = 1
        while len(session.received) <= 0 and i < wait_max:
            debug(1, "sleeping waiting for mock_recv " + str(i))
            time.sleep(i)
            i = i * 2

        if len(session.received) <= 0 and i >= wait_max:
            debug(1, "waiting too long for mock_recv " + str(i))

        message = ""
        if len(session.received) > 0:
            message = session.received.pop(0)

        debug(1, "mock_recv actual: " + message);

        return message

    def mock_recv(self, what, session_idx=0):
        # Useful for ascii messages.
        message = self.mock_recv_message(what, session_idx)
        self.assertTrue(what == message or re.match(what, message) is not None)

    def wait(self, x):
        debug(1, "wait " + str(x))
        time.sleep(0.01 * x)

    def client_close(self, idx=0):
        if self.clients[idx]:
            self.clients[idx].close()
        self.clients[idx] = None

    def mock_close(self):
        if self.mock_server():
            self.mock_server().closeSessions()

    def mock_quiet(self, session_idx=0):
        if len(self.mock_server().sessions) <= session_idx:
            return True

        session = self.mock_server().sessions[session_idx]

        return len(session.received) <= 0

