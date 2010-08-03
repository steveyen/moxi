/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#ifndef CONFIG_STATIC_H
#define CONFIG_STATIC_H 1

// The intention of this file is to avoid cluttering the code with #ifdefs

#ifdef WIN32
// HAVE_CONFIG_H is causing problems with pthreads.h on in32
#undef HAVE_CONFIG_H

#define _WIN32_WINNT    0x0501
#include <winsock2.h>
#include <ws2tcpip.h>

struct iovec {
    size_t iov_len;
    void* iov_base;
};

#include "win32/win32.h"

#define EX_USAGE EXIT_FAILURE
#define EX_OSERR EXIT_FAILURE

#else
#define initialize_sockets()
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#ifdef HAVE_PWD_H
#include <pwd.h>
#endif

#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#ifdef HAVE_SYSEXITS_H
#include <sysexits.h>
#endif

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef HAVE_SYSLOG_H
#include <syslog.h>
#define DEFAULT_ERRORLOG ERRORLOG_SYSLOG
#else
#define DEFAULT_ERRORLOG ERRORLOG_STDERR
#endif

#endif
