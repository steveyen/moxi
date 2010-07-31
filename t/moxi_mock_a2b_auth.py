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

# Before you run moxi_mock_a2b_auth.py, start a moxi like...
#
#   ./moxi-debug -z url=http://127.0.0.1:4567/pools/default/buckets/default \
#                -p 0 -U 0 -vvv -t 1 \
#                -Z usr=TheUser,pwd=ThePassword,port_listen=11333,downstream_max=1,downstream_protocol=binary
#
# Then...
#
#   ruby ./t/rest_mock.rb
#
# Then...
#
#   python ./t/moxi_mock_a2b_auth.py
#
# ----------------------------------

class TestProxyBinary(moxi_mock_server.ProxyClientBase):
    def __init__(self, x):
        moxi_mock_server.ProxyClientBase.__init__(self, x)

    def testFirstAuth(self):
        """Test seeing a first AUTH"""
        self.client_connect()

        # First time, we should see a SASL plain auth.
        self.client_send("get keyNotThere0\r\n")
        self.mock_recv(self.packReq(memcacheConstants.CMD_SASL_AUTH,
                                    key='PLAIN',
                                    val="\0TheUser\0ThePassword"))
        self.mock_send(self.packRes(memcacheConstants.CMD_SASL_AUTH,
                                    status=0,
                                    val='Authenticated'))

        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere0'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK,
                                    status=memcacheConstants.ERR_NOT_FOUND,
                                    key='keyNotThere0'))
        self.client_recv("END\r\n")

        # Next time, we should not see any auth.
        self.client_send("get keyNotThere0\r\n")
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere0'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK,
                                    status=memcacheConstants.ERR_NOT_FOUND,
                                    key='keyNotThere0'))
        self.client_recv("END\r\n")

    def testFirstAuthSetGet(self):
        """Test that we can get and set after AUTH"""
        self.client_connect()

        flg = 0
        exp = 0
        val = "12345"

        # First time, we should see a SASL plain auth.
        self.client_send('set simpleSet %d %d %d\r\n' % (flg, exp, len(val)))
        self.client_send(val + '\r\n')
        self.mock_recv(self.packReq(memcacheConstants.CMD_SASL_AUTH,
                                    key='PLAIN',
                                    val="\0TheUser\0ThePassword"))
        self.mock_send(self.packRes(memcacheConstants.CMD_SASL_AUTH,
                                    status=0,
                                    val='Authenticated'))
        self.mock_recv(self.packReq(memcacheConstants.CMD_SET, key='simpleSet',
                                    extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, flg, exp),
                                    val=val))
        self.mock_send(self.packRes(memcacheConstants.CMD_SET, status=0))
        self.client_recv('STORED\r\n')

        # Next time, we should not see any auth.
        self.client_send("get simpleSet\r\n")
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='simpleSet'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK, key='simpleSet',
                                    extraHeader=struct.pack(memcacheConstants.GET_RES_FMT, 0),
                                    val='12345'))
        self.client_recv('VALUE simpleSet 0 5\r\n12345\r\nEND\r\n')

if __name__ == '__main__':
    unittest.main()

