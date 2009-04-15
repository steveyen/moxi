import sys
import string
import socket
import select
import threading
import time
import re

# The dollar ($) char adds implicit carriage-return/newline (\r\n).
#
# c means client connection.
# P means proxy upstream connection.
# d means proxy downstream connection.
# S means connection on fake memcached server.
#
def debug(x):
    print(x)

test_name = None

conns = {}

class ExpectServer(threading.Thread):
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

    def run(self):
        self.running = True

        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.host, self.port))
            self.server.listen(self.backlog)
        except KeyboardInterrupt:
            raise
        except socket.error, (value, message):
            if self.server:
                self.server.close()
            print "Could not open socket: " + message
            sys.exit(1)

        while self.running:
            debug("ExpectServer running " + str(self.port))
            client, address = self.server.accept()
            c = ExpectSession(client, address, self)
            debug("ExpectServer accepted " + str(self.port))
            self.sessions.append(c)
            c.start()

        self.running = False
        debug("ExpectServer stopping")
        self.server.close()
        debug("ExpectServer done")

        self.closeSessions()

class ExpectSession(threading.Thread):
    def __init__(self, client, address, server):
        threading.Thread.__init__(self)
        self.server  = server
        self.client  = client
        self.address = address
        self.size    = 1024
        self.running     = 0
        self.running_max = 10

        global conns
        conns['S'] = self.client

    def run(self):
        global conns
        global expect_recv

        conns['S'] = self.client

        input = [self.client]

        self.running = 1
        while (self.running > 0 and
               self.running < self.running_max):
            debug("ExpectSession running (" + str(self.running) + ")")
            self.running = self.running + 1

            iready, oready, eready = select.select(input, [], [], 1)
            if len(eready) > 0:
                self.running = 0
            elif len(iready) > 0:
                data = self.client.recv(self.size)
                if data is None or data == "":
                    self.close()
                else:
                    expect_recv.append(data)
                    conns['S'] = self.client
                    debug("ExpectSession recv: " + str(expect_recv))

        if self.running >= self.running_max:
            print "ExpectSession running too long, shutting down"
            self.running = 0

    def close(self):
        self.running = 0
        self.client.close()

sys.setcheckinterval(0)

expect_recv = []
expect_server = ExpectServer(11311)
expect_server.start()

time.sleep(1)
if not expect_server.running:
    sys.exit(1)

def TEST(name):
    global test_name, conns, expect_server

    expect_server.closeSessions()

    close('S')
    close('c')

    time.sleep(1)

    conns['c'] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conns['c'].connect(("127.0.0.1", 11333))

    test_name = name
    print("TEST: " + test_name)

def send(src, dst, what):
    global test_name, conns
    w = string.strip(what, '^')
    w = string.replace(w, '$', "\r\n")
    print("sending " + w)
    conns[src].send(w)

def recv(src, dst, what):
    global test_name, conns, expect_recv

    print("recv: " + src + " " + dst + " " + what)

    if dst[0] == 'c':
        s = conns[dst].recv(1024)
    else:
        wait_max = 5

        s = ""
        i = 1
        while dst not in conns and i < wait_max:
            debug("sleeping waiting for " + dst + " " + str(i))
            time.sleep(i)
            i = i + 1
        if dst not in conns and i >= wait_max:
            print("waiting too long for " + dst + " " + str(i))

        if len(expect_recv) > 0:
            s = expect_recv.pop(0)

    w = string.strip(what, '^$')
    print("recv expecting: " + w)
    print("recv got: " + s)

def wait(x):
    print("wait " + str(x))
    time.sleep(x)

def close(x):
    global test_name, conns
    if x in conns:
        print("close " + str(x))
        conns[x].close()
        del conns[x]

def quiet(x):
    global test_name, conns, expect_recv
    print("quiet " + str(x))

    if x == 'c':
        debug("UNIMPLEMENTED")
        1 # TODO
    else:
        print("expect_recv length (should be 0): " + str(len(expect_recv)))

print sys.argv[1]

try:
    execfile(sys.argv[1])
except:
    sys.exit(1)

sys.exit(1)
