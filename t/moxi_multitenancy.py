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

# Tests moxi multitenancy.
#
# Before you run moxi_multitenancy.py, start a moxi like...
#
#   ./moxi -z url=http://127.0.0.1:4567/pools/default/buckets/default \
#          -p 0 -U 0 -vvv -t 1 -O stderr \
#          -Z default_bucket_name=,port_listen=11333,downstream_max=1,downstream_protocol=binary
#
# Then...
#
#   ruby ./t/rest_mock.rb ./t/moxi_multitenancy_rest.cfg
#
# Then...
#
#   python ./t/moxi_multitenancy.py
#
# ----------------------------------

class TestMultitenancy(moxi_mock_server.ProxyClientBase):
    def __init__(self, x):
        moxi_mock_server.ProxyClientBase.__init__(self, x)

    def testListMechs(self):
        """Test list mechs"""
        self.client_connect()

        list_req = self.packReq(memcacheConstants.CMD_SASL_LIST_MECHS);
        list_res = self.packRes(memcacheConstants.CMD_SASL_LIST_MECHS,
                                val="PLAIN")

        self.client_send(list_req)
        self.client_recv(list_res)

        self.assertTrue(self.mock_quiet())

        self.client_send(list_req)
        self.client_recv(list_res)

        self.assertTrue(self.mock_quiet())

    def testNullBucket(self):
        """Test null bucket"""
        self.client_connect()

        get_oom = self.packRes(memcacheConstants.CMD_GETK,
                               status=memcacheConstants.ERR_ENOMEM,
                               val="Out of memory")

        get_req = self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere0')
        self.client_send(get_req)
        self.client_recv(get_oom)

        get_req = self.packReq(memcacheConstants.CMD_GETK, key='keyNotThere0')
        self.client_send(get_req)
        self.client_recv(get_oom)

        self.assertTrue(self.mock_quiet())

    def testNullBucketSet(self):
        """Test null bucket sets"""
        self.client_connect()

        set_req = self.packReq(memcacheConstants.CMD_SET, key='simpleSet',
                               extraHeader=struct.pack(memcacheConstants.SET_PKT_FMT, 0, 0),
                               val='123')
        set_oom = self.packRes(memcacheConstants.CMD_SET,
                               status=memcacheConstants.ERR_ENOMEM,
                               val="Out of memory")

        self.client_send(set_req)
        self.client_recv(set_oom)

        self.client_send(set_req)
        self.client_recv(set_oom)

        self.assertTrue(self.mock_quiet())

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

    def testBadPassword(self):
        """Test seeing a bad AUTH B2B"""
        self.client_connect()

        auth_req = self.packReq(memcacheConstants.CMD_SASL_AUTH,
                                key="PLAIN",
                                val="\0userGood0\0passwordBad0");
        auth_res = self.packRes(memcacheConstants.CMD_SASL_AUTH,
                                status=memcacheConstants.ERR_AUTH_ERROR,
                                val="Auth failure")

        self.client_send(auth_req)
        self.client_recv(auth_res)

        self.exerciseGoodAuth("userGood1", "passwordGood1")

    def testBadEmptyUser(self):
        """Test seeing a bad AUTH B2B"""
        self.client_connect()

        auth_req = self.packReq(memcacheConstants.CMD_SASL_AUTH,
                                key="PLAIN",
                                val="\0\0somePassword");
        auth_res = self.packRes(memcacheConstants.CMD_SASL_AUTH,
                                status=memcacheConstants.ERR_AUTH_ERROR,
                                val="Auth failure")

        self.client_send(auth_req)
        self.client_recv(auth_res)

        self.exerciseGoodAuth("userGood1", "passwordGood1")

    def testBadEmptyPassword(self):
        """Test seeing a bad AUTH B2B"""
        self.client_connect()

        auth_req = self.packReq(memcacheConstants.CMD_SASL_AUTH,
                                key="PLAIN",
                                val="\0userGood0\0");
        auth_res = self.packRes(memcacheConstants.CMD_SASL_AUTH,
                                status=memcacheConstants.ERR_AUTH_ERROR,
                                val="Auth failure")

        self.client_send(auth_req)
        self.client_recv(auth_res)

        self.exerciseGoodAuth("userGood1", "passwordGood1")

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

