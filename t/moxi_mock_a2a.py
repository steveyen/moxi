import sys
import string
import socket
import select
import unittest
import threading
import time
import re

import moxi_mock_server

# Before you run moxi_mock_a2a.py, start a moxi like...
#
#   ./moxi -z 11333=localhost:11311 -p 0 -U 0 -vvv -t 1
#                -Z downstream_max=1
#
# Or, if you're using the vbucket-aware moxi...
#
#   ./moxi -z ./t/moxi_mock.cfg -p 0 -U 0 -vvv -t 1
#                -Z downstream_max=1,downstream_protocol=ascii
#
# Then...
#
#   python ./t/moxi_mock_a2a.py
#
# ----------------------------------

class TestProxyAscii(moxi_mock_server.ProxyClientBase):
    def __init__(self, x):
        moxi_mock_server.ProxyClientBase.__init__(self, x)

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
        """Test basic serial get commands"""
        self.client_connect()
        self.client_send("get keyNotThere0\r\n")
        self.mock_recv('get keyNotThere0\r\n')
        self.mock_send('END\r\n')
        self.client_recv("END\r\n")

        self.client_send("get keyNotThere1\r\n")
        self.mock_recv('get keyNotThere1\r\n')
        self.mock_send('END\r\n')
        self.client_recv("END\r\n")

        self.client_send("get keyNotThere2\r\n")
        self.mock_recv('get keyNotThere2\r\n')
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
        """Test the proxy handles VALUE response"""
        self.client_connect()
        self.client_send('get someVal0 someVal1\r\n')
        self.mock_recv("get someVal0 someVal1\r\n")
        self.mock_send('END\r\n')
        self.client_recv('END\r\n')

        self.client_send('get someVal0 someVal1\r\n')
        self.mock_recv("get someVal0 someVal1\r\n")
        self.mock_send('VALUE someVal0 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('END\r\n')
        self.client_recv('VALUE someVal0 0 10\r\n0123456789\r\nEND\r\n')

        self.client_send('get someVal0 someVal1\r\n')
        self.mock_recv("get someVal0 someVal1\r\n")
        self.mock_send('VALUE someVal0 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('VALUE someVal1 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('END\r\n')
        self.client_recv('VALUE someVal0 0 10\r\n0123456789\r\nVALUE someVal1 0 10\r\n0123456789\r\nEND\r\n')

    def testGetEmptyValue(self):
        """Test the proxy handles empty VALUE response"""
        self.client_connect()
        self.client_send('get someVal\r\n')
        self.mock_recv("get someVal\r\n")
        self.mock_send('VALUE someVal 0 0\r\n')
        self.mock_send('\r\n')
        self.mock_send('END\r\n')
        self.client_recv('VALUE someVal 0 0\r\n\r\nEND\r\n')

    def testTerminateResponseWithServerCloseInValue(self):
        """Test chop the VALUE response with a server close"""
        self.client_connect()
        self.client_send('get someChoppedVal\r\n')
        self.mock_recv("get someChoppedVal\r\n")
        self.mock_send('VALUE someChoppedVal 0 10\r\n')
        self.mock_send('012345')
        self.mock_close()
        self.client_recv('END\r\n')

    def testTerminateResponseWithServerCloseIn2ndValue(self):
        """Test chop the 2nd VALUE response with a server close"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv("get someWholeVal someChoppedVal\r\n")
        self.mock_send('VALUE someWholeVal 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('VALUE someChoppedVal 0')
        self.mock_close()
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\nEND\r\n')

    def testTerminateResponseWithServerCloseIn2ndValueData(self):
        """Test chop the 2nd VALUE data response with a server close"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv("get someWholeVal someChoppedVal\r\n")
        self.mock_send('VALUE someWholeVal 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('VALUE someChoppedVal 0 10\r\n')
        self.mock_send('012345')
        self.mock_close()
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\nEND\r\n')

    def testTerminateResponseWithServerCloseAfterValueHeader(self):
        """Test chop response after VALUE header"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv("get someWholeVal someChoppedVal\r\n")
        self.mock_send('VALUE someWholeVal 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('VALUE someChoppedVal 0 10\r\n')
        self.mock_close()
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\nEND\r\n')

    def testServerGoingDownAndUp(self):
        """Test server going up and down with no client impact"""
        self.client_connect()
        self.client_send('get someUp\r\n')
        self.mock_recv("get someUp\r\n")
        self.mock_send('VALUE someUp 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('END\r\n')
        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

        self.mock_close()

        self.client_send('get someUp\r\n')
        self.mock_recv("get someUp\r\n")
        self.mock_send('VALUE someUp 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('END\r\n')
        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

    def testServerGoingDownAndUpAfterEND(self):
        """Test server going up and down after END with no client impact"""
        self.client_connect()
        self.client_send('get someUp\r\n')
        self.mock_recv("get someUp\r\n")
        self.mock_send('VALUE someUp 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('END\r\n')

        self.mock_close() # Try close before client_recv().

        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

        self.client_send('get someUp\r\n')
        self.mock_recv("get someUp\r\n")
        self.mock_send('VALUE someUp 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('END\r\n')
        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

    def testTwoSerialClients(self):
        """Test two serial clients"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)
        self.client_send('get client0\r\n', 0)
        self.mock_recv("get client0\r\n", 0)
        self.mock_send('VALUE client0 0 10\r\n', 0)
        self.mock_send('0123456789\r\n', 0)
        self.mock_send('END\r\n', 0)
        self.client_recv('VALUE client0 0 10\r\n0123456789\r\nEND\r\n', 0)

        # Note that mock server sees 1 session that's reused
        # even though two clients are connected.

        self.client_connect(1)
        self.client_send('get client1\r\n', 1)
        self.mock_recv("get client1\r\n", 0)
        self.mock_send('VALUE client1 0 10\r\n', 0)
        self.mock_send('0123456789\r\n', 0)
        self.mock_send('END\r\n', 0)
        self.client_recv('VALUE client1 0 10\r\n0123456789\r\nEND\r\n', 1)

    def testTwoSerialClientsConnectingUpfront(self):
        """Test two serial clients that both connect upfront"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)
        self.client_connect(1)

        self.client_send('get client0\r\n', 0)
        self.mock_recv("get client0\r\n", 0)
        self.mock_send('VALUE client0 0 10\r\n', 0)
        self.mock_send('0123456789\r\n', 0)
        self.mock_send('END\r\n', 0)
        self.client_recv('VALUE client0 0 10\r\n0123456789\r\nEND\r\n', 0)

        # Note that mock server sees 1 session that's reused
        # even though two clients are connected.

        self.client_send('get client1\r\n', 1)
        self.mock_recv("get client1\r\n", 0)
        self.mock_send('VALUE client1 0 10\r\n', 0)
        self.mock_send('0123456789\r\n', 0)
        self.mock_send('END\r\n', 0)
        self.client_recv('VALUE client1 0 10\r\n0123456789\r\nEND\r\n', 1)

    # Dedupe of keys is disabled for now in vbucket-aware moxi.
    #
    def TODO_testGetSquash(self):
        """Test multiget by multiple clients are deduped"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)
        self.client_connect(1)
        self.client_connect(2)

        self.client_send('get cork0\r\n', 0)
        self.mock_recv('get cork0\r\n', 0)

        # Mock server is 'busy' at this point, so
        # any client sends should be able to be
        # de-duplicated by the proxy.

        self.client_send('get a b c\r\n', 1)
        self.client_send('get b c d\r\n', 2)

        self.wait(10)

        self.mock_send('END\r\n', 0)
        self.client_recv('END\r\n', 0)

        self.mock_recv('get a b c d\r\n', 0)
        self.mock_send('VALUE a 0 1\r\na\r\n', 0)
        self.mock_send('VALUE b 0 1\r\nb\r\n', 0)
        self.mock_send('VALUE c 0 1\r\nc\r\n', 0)
        self.mock_send('VALUE d 0 1\r\nd\r\n', 0)
        self.mock_send('END\r\n', 0)

        self.client_recv('VALUE a 0 1\r\na\r\n' +
                         'VALUE b 0 1\r\nb\r\n' +
                         'VALUE c 0 1\r\nc\r\n' +
                         'END\r\n', 1)
        self.client_recv('VALUE b 0 1\r\nb\r\n' +
                         'VALUE c 0 1\r\nc\r\n' +
                         'VALUE d 0 1\r\nd\r\n' +
                         'END\r\n', 2)

    # Dedupe of keys is disabled for now in vbucket-aware moxi.
    #
    def TODO_testGetSquashOneKey(self):
        """Test multiget of one key by multiple clients are deduped"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)
        self.client_connect(1)
        self.client_connect(2)
        self.client_connect(3)
        self.client_connect(4)

        self.client_send('get wait0\r\n', 0)
        self.mock_recv('get wait0\r\n', 0)

        # Mock server is 'busy' at this point, so
        # any client sends should be able to be
        # de-duplicated by the proxy.

        self.client_send('get a\r\n', 1)
        self.client_send('get a\r\n', 2)
        self.client_send('get a\r\n', 3)
        self.client_send('get a\r\n', 4)

        self.wait(10)

        self.mock_send('END\r\n', 0)
        self.client_recv('END\r\n', 0)

        self.mock_recv('get a\r\n', 0)
        self.mock_send('VALUE a 0 1\r\na\r\n', 0)
        self.mock_send('END\r\n', 0)

        self.client_recv('VALUE a 0 1\r\na\r\n' +
                         'END\r\n', 1)
        self.client_recv('VALUE a 0 1\r\na\r\n' +
                         'END\r\n', 2)
        self.client_recv('VALUE a 0 1\r\na\r\n' +
                         'END\r\n', 3)
        self.client_recv('VALUE a 0 1\r\na\r\n' +
                         'END\r\n', 4)

    # Dedupe of keys is disabled for now in vbucket-aware moxi.
    #
    def TODO_testGetSquashNoKeyOverlap(self):
        """Test multiget dedupe, but no key overlap"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)
        self.client_connect(1)
        self.client_connect(2)

        self.client_send('get cork0\r\n', 0)
        self.mock_recv('get cork0\r\n', 0)

        # Mock server is 'busy' at this point, so
        # any client sends should be able to be
        # de-duplicated by the proxy.

        self.client_send('get a\r\n', 1)
        self.client_send('get x\r\n', 2)

        self.wait(10)

        self.mock_send('END\r\n', 0)
        self.client_recv('END\r\n', 0)

        self.mock_recv('get a x\r\n', 0)
        self.mock_send('VALUE a 0 1\r\na\r\n', 0)
        self.mock_send('VALUE x 0 1\r\nx\r\n', 0)
        self.mock_send('END\r\n', 0)

        self.client_recv('VALUE a 0 1\r\na\r\n' +
                         'END\r\n', 1)
        self.client_recv('VALUE x 0 1\r\nx\r\n' +
                         'END\r\n', 2)

    def TODO_testTimeout(self):
        """Test downstream timeout handling"""
        return """TODO: Highly dependent on hardcoded downstream timeout val"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)

        self.client_send('get time0\r\n', 0)
        self.mock_recv('get time0\r\n', 0)

        # Mock server is 'busy' at this point, so
        # downstream timeout logic should kick in,
        # without our mock server having to send anything.

        self.wait(210)

        self.client_recv('END\r\n', 0)

        # TODO: The number of server sessions should be 0,
        # except the close might not have propagated.

    def TODO_testSharedServerConns(self):
        """Test proxy only uses a few server conns"""
        return "TODO: getting nondetermistic behavior here due to retry feature"

        self.assertEqual(len(self.clients), 0)
        self.assertEqual(len(self.mock_server().sessions), 0)

        large_num_clients = 30
        for i in range(0, large_num_clients):
            self.client_connect(i)
            self.client_send('get fromClient' + str(i) + '\r\n', i)

        self.assertEqual(len(self.clients), large_num_clients)

        self.wait(10)

        self.assertTrue(len(self.mock_server().sessions) < large_num_clients)
        self.assertTrue(len(self.mock_server().sessions) > 0)

    def TODO_testServerSeesRetry(self):
        """Test server going down sees a retry"""
        return "TODO: getting nondetermistic behavior here due to retry feature"

        self.client_connect()
        self.client_send('get someDown\r\n')
        self.mock_recv("get someDown\r\n")
        self.mock_send('VALUE someDown 0 10\r\n')

        self.mock_close() # Go down before full reply, we should see retry.

        self.mock_recv("get someDown\r\n")
        self.mock_send('VALUE someDown 0 10\r\n')
        self.mock_send('0123456789\r\n')
        self.mock_send('END\r\n')
        self.client_recv('VALUE someDown 0 10\r\n0123456789\r\nEND\r\n')

# More test ideas...
#
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
#
# Retest all the above with binary protocol.

if __name__ == '__main__':
    unittest.main()

