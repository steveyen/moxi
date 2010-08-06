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

# Tests moxi multitenancy with a default bucket.
#
# Before you run moxi_multitenancy_default.py, start a moxi like...
#
#   ./moxi -z url=http://127.0.0.1:4567/pools/default/buckets/default \
#          -p 0 -U 0 -vvv -t 1 -O stderr \
#          -Z default_bucket_name=default,port_listen=11333,downstream_max=1,downstream_protocol=binary
#
# Then...
#
#   ruby ./t/rest_mock.rb ./t/moxi_multitenancy_rest_default.cfg
#
# Then...
#
#   python ./t/moxi_multitenancy_default.py
#
# ----------------------------------

class TestMultitenancyWithDefaultBucket(moxi_mock_server.ProxyClientBase):
    def __init__(self, x):
        moxi_mock_server.ProxyClientBase.__init__(self, x)

    def testDefaultBucketAscii(self):
        """Test basic serial get commands"""
        self.client_connect()

        self.client_send("get keyNotThere0\r\n")
        self.mock_recv(self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere0'))
        self.mock_send(self.packRes(memcacheConstants.CMD_GETK,
                                    status=memcacheConstants.ERR_NOT_FOUND,
                                    key='keyNotThere0'))
        self.client_recv("END\r\n")

    def testDefaultBucketSet(self):
        self.client_connect()
        self.doTestSimpleSet()

    def doTestSimpleSet(self, flg=1234, exp=0, val='9876'):
        """Test simple set against mock server"""
        self.client_send(self.packReq(memcacheConstants.CMD_SET, key='simpleSet',
                                      extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, flg, exp),
                                      val=val))
        self.mock_recv(self.packReq(memcacheConstants.CMD_SET, key='simpleSet',
                                    extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, flg, exp),
                                    val=val))
        self.mock_send(self.packRes(memcacheConstants.CMD_SET, status=0))
        self.client_recv(self.packRes(memcacheConstants.CMD_SET, status=0))

    def testGoodAuth(self):
        """Test seeing a first AUTH B2B"""
        self.client_connect()
        self.exerciseGoodAuth("userGood0", "passwordGood0")

    def testBadAuth(self):
        """Test seeing a bad AUTH B2B"""
        self.client_connect()

        auth_req = self.packReq(memcacheConstants.CMD_SASL_AUTH,
                                key="PLAIN",
                                val="\0userBad0\0passwordBad0");
        auth_res = self.packRes(memcacheConstants.CMD_SASL_AUTH,
                                status=memcacheConstants.ERR_AUTH_ERROR,
                                val="Auth failure")

        self.client_send(auth_req)
        self.client_recv(auth_res)

        self.exerciseGoodAuth("userGood0", "passwordGood0")

    def exerciseGoodAuth(self, user, password):
        auth_req = self.packReq(memcacheConstants.CMD_SASL_AUTH,
                                key="PLAIN",
                                val="\0" + user + "\0" + password);
        auth_res = self.packRes(memcacheConstants.CMD_SASL_AUTH,
                                val="Authenticated")

        self.client_send(auth_req)
        self.client_recv(auth_res)

        get_req = self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere0');
        get_res = self.packRes(memcacheConstants.CMD_GETK,
                               status=memcacheConstants.ERR_NOT_FOUND,
                               key='keyNotThere0')

        # First time, we should see a SASL plain auth.
        self.client_send(get_req)
        self.mock_recv(auth_req)
        self.mock_send(auth_res)
        self.mock_recv(get_req)
        self.mock_send(get_res)
        self.client_recv(get_res)

        # Next time, we should not see any auth.
        self.client_send(get_req)
        self.mock_recv(get_req)
        self.mock_send(get_res)
        self.client_recv(get_res)

if __name__ == '__main__':
    unittest.main()

