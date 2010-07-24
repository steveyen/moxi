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
# sys.setcheckinterval(0)
# g_mock_server_port = 11311
# g_mock_server = MockServer(g_mock_server_port)
# g_mock_server.start()
# time.sleep(1)

