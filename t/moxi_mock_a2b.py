import sys
import string
import socket
import select
import unittest
import threading
import time
import re
import struct

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT

import memcacheConstants

import moxi_mock_server

# Before you run moxi_mock_a2binary.py, start a moxi like...
#
#   ./moxi-debug -z 11333=localhost:11311 -p 0 -U 0 -vvv -t 1
#                -Z downstream_max=1
#
# Or, if you're using the vbucket-aware moxi...
#
#   ./moxi-debug -z ./t/moxi_mock.cfg -p 0 -U 0 -vvv -t 1
#                -Z downstream_max=1,downstream_protocol=binary
#
# Then...
#
#   python ./t/moxi_mock_a2binary.py
#
# ----------------------------------

class TestProxyBinary(moxi_mock_server.ProxyClientBase):
    def __init__(self, x):
        moxi_mock_server.ProxyClientBase.__init__(self, x)

    # -------------------------------------------------

    def packReq(self, cmd, reserved=0, key='', val='', opaque=0, extraHeader='', cas=0):
        dtype=0
        msg=struct.pack(REQ_PKT_FMT, REQ_MAGIC_BYTE,
            cmd, len(key), len(extraHeader), dtype, reserved,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        return msg + extraHeader + key + val

    def packRes(self, cmd, status=0, key='', val='', opaque=0, extraHeader='', cas=0):
        dtype=0
        msg=struct.pack(REQ_PKT_FMT, RES_MAGIC_BYTE,
            cmd, len(key), len(extraHeader), dtype, status,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        return msg + extraHeader + key + val

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
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere0'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK,
                                    status=memcacheConstants.ERR_NOT_FOUND,
                                    key='keyNotThere0'))
        self.client_recv("END\r\n")

        self.client_send("get keyNotThere1\r\n")
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere1'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK,
                                    status=memcacheConstants.ERR_NOT_FOUND,
                                    key='keyNotThere1'))
        self.client_recv("END\r\n")

        self.client_send("get keyNotThere2\r\n")
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere2'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK,
                                    status=memcacheConstants.ERR_NOT_FOUND,
                                    key='keyNotThere2'))
        self.client_recv("END\r\n")

    def testBasicQuit(self):
        """Test quit command does not reach mock server"""
        self.client_connect()

        self.client_send("get keyNotThere\r\n")
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK,
                                    status=memcacheConstants.ERR_NOT_FOUND,
                                    key='keyNotThere'))
        self.client_recv("END\r\n")

        self.client_send("quit\r\n")
        self.assertTrue(self.mock_quiet())

    def testBogusCommand(self):
        """Test bogus commands do not reach mock server"""
        self.client_connect()
        self.client_send('bogus\r\n')
        self.client_recv('.*ERROR.*\r\n')
        self.assertTrue(self.mock_quiet())

    def doTestSimpleSet(self, flg=0, exp=0, val='1'):
        """Test simple set against mock server"""
        self.client_connect()
        self.client_send('set simpleSet %d %d %d\r\n' % (flg, exp, len(val)))
        self.client_send(val + '\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_SET, key='simpleSet',
                                    extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, flg, exp),
                                    val=val))
        self.mock_send(self.packRes(memcacheConstants.CMD_SET, status=0))
        self.client_recv('STORED\r\n')

    def testSimpleSet(self):
        self.doTestSimpleSet()

    def testSimpleSetWithParams(self):
        self.doTestSimpleSet(flg=1234, exp=4321)

    def testSimpleSetWithEmptyVal(self):
        self.doTestSimpleSet(flg=1234, exp=4321, val='')

    def doTestFlushAllBroadcast(self, exp=0):
        """Test flush_all scatter/gather"""
        self.client_connect()
        self.client_send('flush_all %d\r\n' % exp)
        self.mock_recv(self.packReq(memcacheConstants.CMD_FLUSH,
                                    extraHeader=struct.pack(memcacheConstants.FLUSH_PKT_FMT, exp)))
        self.mock_send(self.packRes(memcacheConstants.CMD_FLUSH, status=0))
        self.client_recv('OK\r\n')

    def testFlushAllBroadcast(self):
        self.doTestFlushAllBroadcast()

    def testFlushAllWithTime(self):
        self.doTestFlushAllBroadcast(exp=1234)

    def testSplitResponseOverSeveralWrites(self):
        """Test split a response over several writes"""
        self.client_connect()
        self.client_send('set splitResponse 0 0 1\r\n')
        self.client_send('1\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_SET, key='splitResponse',
                                    extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, 0, 0),
                                    val='1'))
        r = self.packRes(memcacheConstants.CMD_SET, status=0)
        self.mock_send(r[0:3])
        self.wait(1)
        self.mock_send(r[3:6])
        self.wait(1)
        self.mock_send(r[6:22])
        self.wait(1)
        self.mock_send(r[22:]) # Header is 24 bytes.
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
        self.mock_recv(self.packReq(memcacheConstants.CMD_SET, key='splitRequest',
                                    extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, 0, 0),
                                    val='hello'))
        self.mock_send(self.packRes(memcacheConstants.CMD_SET, status=0))
        self.client_recv('STORED\r\n')

    def testTerminateResponseWithServerClose(self):
        """Test chop the response with a server close"""
        self.client_connect()
        self.client_send('set chopped 0 0 1\r\n')
        self.client_send('1\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_SET, key='chopped',
                                    extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, 0, 0),
                                    val='1'))
        self.mock_close()
        self.client_recv('.*ERROR .*\r\n')

    def testMultiGetValueAllMiss(self):
        """Test the proxy handles VALUE response"""
        self.client_connect()
        self.client_send('get someVal0 someVal1\r\n')
        # Note: dependency on moxi's a2b implementation: the opaque's
        # each key's offsets into the multiget request string.
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someVal0', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someVal1', opaque=13) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        self.mock_send(self.packRes(memcacheConstants.CMD_NOOP))
        self.client_recv('END\r\n')

    def testMultiGetValueSomeHit(self):
        self.client_connect()
        self.client_send('get someVal0 someVal1\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someVal0', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someVal1', opaque=13) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETKQ, key='someVal0', opaque=4,
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.mock_send(self.packRes(memcacheConstants.CMD_NOOP))
        self.client_recv('VALUE someVal0 0 10\r\n0123456789\r\nEND\r\n')

    def testMultiGetValueAllHit(self):
        self.client_connect()
        self.client_send('get someVal0 someVal1\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someVal0', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someVal1', opaque=13) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETKQ, key='someVal0', opaque=4,
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETKQ, key='someVal1', opaque=13,
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.mock_send(self.packRes(memcacheConstants.CMD_NOOP))
        self.client_recv('VALUE someVal0 0 10\r\n0123456789\r\nVALUE someVal1 0 10\r\n0123456789\r\nEND\r\n')

    def testMultiGetValueAllReversedHit(self):
        self.client_connect()
        self.client_send('get someVal0 someVal1\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someVal0', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someVal1', opaque=13) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETKQ, key='someVal1', opaque=13,
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETKQ, key='someVal0', opaque=4,
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.mock_send(self.packRes(memcacheConstants.CMD_NOOP))
        self.client_recv('VALUE someVal1 0 10\r\n0123456789\r\nVALUE someVal0 0 10\r\n0123456789\r\nEND\r\n')

    def testGetEmptyValue(self):
        """Test the proxy handles empty VALUE response"""
        self.client_connect()
        self.client_send('get someVal\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='someVal'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='someVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val=''))
        self.client_recv('VALUE someVal 0 0\r\n\r\nEND\r\n')

    def testTerminateResponseWithServerCloseInValue(self):
        """Test chop the VALUE response with a server close"""
        self.client_connect()
        self.client_send('get someChoppedVal\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='someChoppedVal'))
        r = self.packRes(memcacheConstants.CMD_GETK, key='someChoppedVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789')
        self.mock_send(r[0:30]) # Header (24) + Ext (4) + Value (10) == 38, so missing a few bytes.
        self.mock_close()
        self.client_recv('END\r\n')

    def testTerminateResponseWithServerCloseIn2ndValue(self):
        """Test chop the 2nd VALUE response with a server close"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someWholeVal', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someChoppedVal', opaque=17) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        r = (self.packRes(memcacheConstants.CMD_GETK, key='someWholeVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=4,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_GETK, key='someChoppedVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=17,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_NOOP))
        self.mock_send(r[0:(24 + 4 + 12 + 10 + 5)])
        self.mock_close()
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\nEND\r\n')

    def testTerminateResponseWithServerCloseIn2ndValueData(self):
        """Test chop the 2nd VALUE data response with a server close"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someWholeVal', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someChoppedVal', opaque=17) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        r = (self.packRes(memcacheConstants.CMD_GETK, key='someWholeVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=4,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_GETK, key='someChoppedVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=17,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_NOOP))
        self.mock_send(r[0:(24 + 4 + 12 + 10 + 24 + 4 + 20)])
        self.mock_close()
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\nEND\r\n')

    def testTerminateResponseWithServerCloseInNOOP(self):
        """Test chop NOOP terminator"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someWholeVal', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someChoppedVal', opaque=17) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        r = (self.packRes(memcacheConstants.CMD_GETK, key='someWholeVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=4,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_GETK, key='someChoppedVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=17,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_NOOP))
        self.mock_send(r[:-4])
        self.mock_close()
        # TODO: Need to diagnose why moxi gives this response.
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\n' +
                         'END\r\n')

    def testTerminateResponseWithServerCloseAfterValueHeader(self):
        """Test chop response after 2nd item header"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someWholeVal', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someChoppedVal', opaque=17) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        r = (self.packRes(memcacheConstants.CMD_GETK, key='someWholeVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=4,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_GETK, key='someChoppedVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=17,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_NOOP))
        self.mock_send(r[0:(24 + 4 + 12 + 10 + 24)])
        self.mock_close()
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\n' +
                         'END\r\n')

    def testTerminateResponseWithServerCloseAfter1stItem(self):
        """Test chop response after 1st item"""
        self.client_connect()
        self.client_send('get someWholeVal someChoppedVal\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETKQ, key='someWholeVal', opaque=4) +
                       self.packReq(memcacheConstants.CMD_GETKQ, key='someChoppedVal', opaque=17) +
                       self.packReq(memcacheConstants.CMD_NOOP))
        r = (self.packRes(memcacheConstants.CMD_GETK, key='someWholeVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=4,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_GETK, key='someChoppedVal',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    opaque=17,
                                    val='0123456789') +
             self.packRes(memcacheConstants.CMD_NOOP))
        self.mock_send(r[0:(24 + 4 + 12 + 10)])
        self.mock_close()
        self.client_recv('VALUE someWholeVal 0 10\r\n0123456789\r\n' +
                         'END\r\n')

    def testServerGoingDownAndUp(self):
        """Test server going up and down with no client impact"""
        self.client_connect()
        self.client_send('get someUp\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='someUp'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='someUp',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

        self.mock_close()

        self.client_send('get someUp\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='someUp'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='someUp',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

    def testServerGoingDownAndUpAfterResponse(self):
        """Test server going up and down after response with no client impact"""
        self.client_connect()
        self.client_send('get someUp\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='someUp'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='someUp',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))

        self.mock_close() # Try close before client_recv().

        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

        self.client_send('get someUp\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='someUp'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='someUp',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.client_recv('VALUE someUp 0 10\r\n0123456789\r\nEND\r\n')

    def testTwoSerialClients(self):
        """Test two serial clients"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)
        self.client_send('get client0\r\n', 0)
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='client0'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='client0',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.client_recv('VALUE client0 0 10\r\n0123456789\r\nEND\r\n', 0)

        # Note that mock server sees 1 session that's reused
        # even though two clients are connected.

        self.client_connect(1)
        self.client_send('get client1\r\n', 1)
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='client1'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='client1',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.client_recv('VALUE client1 0 10\r\n0123456789\r\nEND\r\n', 1)

    def testTwoSerialClientsConnectingUpfront(self):
        """Test two serial clients that both connect upfront"""

        # Assuming proxy's downstream_max is 1,
        # and number of threads is 1.

        self.client_connect(0)
        self.client_connect(1)

        self.client_send('get client0\r\n', 0)
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='client0'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='client0',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
        self.client_recv('VALUE client0 0 10\r\n0123456789\r\nEND\r\n', 0)

        # Note that mock server sees 1 session that's reused
        # even though two clients are connected.

        self.client_send('get client1\r\n', 1)
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='client1'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='client1',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='0123456789'))
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

