/* win32.h
 *
 */

#ifndef WIN32_H
#define WIN32_H

#undef  _WIN32_WINNT
#define _WIN32_WINNT    0x0501        /* Needed to resolve getaddrinfo et al. */

#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdio.h>
#include <io.h>
#include <time.h>
/* #include <fcntl.h> */
#include <errno.h>
#include <stdint.h>
#include <process.h>
#include <signal.h>

#define EWOULDBLOCK        EAGAIN
#define EAFNOSUPPORT       47
#define EADDRINUSE         WSAEADDRINUSE
#define EAI_SYSTEM         -11

#ifndef SIGHUP
#define SIGHUP -1
#endif

typedef char *caddr_t;

#define O_BLOCK 0
#define O_NONBLOCK 1
#define F_GETFL 3
#define F_SETFL 4

#define RUSAGE_SELF    0

#define IOV_MAX 1024

struct msghdr {
    void         *msg_name;         /* Socket name            */
    int          msg_namelen;       /* Length of name        */
    struct iovec *msg_iov;          /* Data blocks            */
    int          msg_iovlen;        /* Number of blocks        */
    void         *msg_accrights;    /* Per protocol magic (eg BSD file descriptor passing) */
    int          msg_accrightslen;  /* Length of rights list */
};

#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
  #define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#else
  #define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#endif

/* Structure which says how much of each resource has been used.  */
struct rusage {
    /* Total amount of user time used.  */
    struct timeval ru_utime;
    /* Total amount of system time used.  */
    struct timeval ru_stime;
    /* Maximum resident set size (in kilobytes).  */
    long int ru_maxrss;
    /* Amount of sharing of text segment memory
       with other processes (kilobyte-seconds).  */
    long int ru_ixrss;
    /* Amount of data segment memory used (kilobyte-seconds).  */
    long int ru_idrss;
    /* Amount of stack memory used (kilobyte-seconds).  */
    long int ru_isrss;
    /* Number of soft page faults (i.e. those serviced by reclaiming
       a page from the list of pages awaiting reallocation.  */
    long int ru_minflt;
    /* Number of hard page faults (i.e. those that required I/O).  */
    long int ru_majflt;
    /* Number of times a process was swapped out of physical memory.  */
    long int ru_nswap;
    /* Number of input operations via the file system.  Note: This
       and `ru_oublock' do not include operations with the cache.  */
    long int ru_inblock;
    /* Number of output operations via the file system.  */
    long int ru_oublock;
    /* Number of IPC messages sent.  */
    long int ru_msgsnd;
    /* Number of IPC messages received.  */
    long int ru_msgrcv;
    /* Number of signals delivered.  */
    long int ru_nsignals;
    /* Number of voluntary context switches, i.e. because the process
       gave up the process before it had to (usually to wait for some
       resource to be available).  */
    long int ru_nvcsw;
    /* Number of involuntary context switches, i.e. a higher priority process
       became runnable or the current process used up its time slice.  */
    long int ru_nivcsw;
};

int inet_aton(register const char *cp, struct in_addr *addr);

#define close(s) closesocket(s)

static inline void mapErr(int error) {
    switch(error) {
        default:
            break;
        case WSAEPFNOSUPPORT:
            errno = EAFNOSUPPORT;
            break;
        case WSA_IO_PENDING:
        case WSATRY_AGAIN:
            errno = EAGAIN;
            break;
        case WSAEWOULDBLOCK:
            errno = EWOULDBLOCK;
            break;
        case WSAEMSGSIZE:
            errno = E2BIG;
            break;
        case WSAECONNRESET:
            errno = 0;
            break;
    }
}

#define write mem_write

static inline size_t mem_write(int s, void *buf, size_t len)
{
    DWORD dwBufferCount = 0;
    int error;

    WSABUF wsabuf = { len, (char *)buf} ;
    if(WSASend(s, &wsabuf, 1, &dwBufferCount, 0, NULL, NULL) == 0) {
        return dwBufferCount;
    }
        error = WSAGetLastError();
    if(error == WSAECONNRESET) return 0;
        mapErr(error);
    return -1;
}

#define read mem_read

static inline size_t mem_read(int s, void *buf, size_t len)
{
    DWORD flags = 0;
    DWORD dwBufferCount;
    WSABUF wsabuf = { len, (char *)buf };
        int error;

    if(WSARecv((SOCKET)s,
        &wsabuf,
        1,
        &dwBufferCount,
        &flags,
        NULL,
        NULL
    ) == 0) {
        return dwBufferCount;
    }
    error = WSAGetLastError();
        if (error == WSAECONNRESET) return 0;
        mapErr(error);
    return -1;
}

static inline int sendmsg(int s, const struct msghdr *msg, int flags)
{
    DWORD dwBufferCount;
        int error;

    if(WSASendTo((SOCKET) s,
        (LPWSABUF)msg->msg_iov,
        (DWORD)msg->msg_iovlen,
        &dwBufferCount,
        flags,
        msg->msg_name,
        msg->msg_namelen,
        NULL,
        NULL
    ) == 0) {
        return dwBufferCount;
    }
    error = WSAGetLastError();
        if (error == WSAECONNRESET) return 0;
        mapErr(error);
    return -1;
}

int sleep(int seconds);

struct sigaction {
    void (*sa_handler)(int);
    int sa_mask;
    int sa_flags;
};

#define sigemptyset(a) 0
#define daemonize(a,b) spawn_memcached(argc, argv)

static int fcntl(SOCKET s, int cmd, int val)
{
    u_long imode = 1;
    switch(cmd) {
        case F_SETFL:
            switch(val) {
                case O_NONBLOCK:
                    imode = 1;
                    if(ioctlsocket(s, FIONBIO, &imode) == SOCKET_ERROR)
                        return -1;
                    break;
                case O_BLOCK:
                    imode = 0;
                    if(ioctlsocket(s, FIONBIO, &imode) == SOCKET_ERROR)
                        return -1;
                    break;
                default:
                    return -1;
            }
        case F_GETFL:
            return 0;
        default:
            return -1;
    }
}

static int createLocalListSock(struct sockaddr_in *serv_addr) {
    SOCKET sockfd;
    int slen;

    if ((sockfd = socket(AF_INET,SOCK_STREAM,IPPROTO_TCP)) == INVALID_SOCKET) {
        fprintf(stderr,"socket call for local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    serv_addr->sin_family = AF_INET;
    serv_addr->sin_addr.s_addr = inet_addr("127.0.0.1");
    serv_addr->sin_port = htons(0);
    if (bind(sockfd,(struct sockaddr *)serv_addr,sizeof(*serv_addr)) != 0) {
        fprintf(stderr,"bind of local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    slen = sizeof(*serv_addr);
    if (getsockname(sockfd,(struct sockaddr *)serv_addr,&slen) != 0) {
        fprintf(stderr,"getsockname on local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    if (listen(sockfd,5) == SOCKET_ERROR) {
        fprintf(stderr,"listen on local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    return((int)sockfd);
}

static int createLocalSocketPair(int listSock, int *fds, struct sockaddr_in *serv_addr) {
    struct sockaddr_in cli_addr;
    fd_set myset;
    struct timeval tv;
    socklen_t lon;
    int valopt, tmpVal;

    if ((fds[0] = (int)socket(AF_INET,SOCK_STREAM,IPPROTO_TCP)) == INVALID_SOCKET) {
        fprintf(stderr,"socket call for local client socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    if (fcntl(fds[0],F_SETFL,O_NONBLOCK) < 0) {
        fprintf(stderr,"fcntl call for local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    if (connect(fds[0],(struct sockaddr *)serv_addr,sizeof(*serv_addr)) == SOCKET_ERROR) {
        tmpVal = WSAGetLastError();
        if (tmpVal != WSAEWOULDBLOCK) {
            fprintf(stderr,"connect call for local server socket failed. Error Number %d.\n",tmpVal);
            fflush(stderr);
            return(-1);
        }
    }
    else {
        fprintf(stderr,"connect call for non-blocking local client socket unexpectedly succeeds.\n");
        fflush(stderr);
        return(-1);
    }
    Sleep(10);
    tmpVal = sizeof(cli_addr);
    if ((fds[1] = (int)accept(listSock, (struct sockaddr *)&cli_addr, &tmpVal))== INVALID_SOCKET) {
        fprintf(stderr,"accept call for local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    if (fcntl(fds[1],F_SETFL,O_NONBLOCK) < 0) {
        fprintf(stderr,"fcntl call for local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    tv.tv_sec = 15;
    tv.tv_usec = 0;
    FD_ZERO(&myset);
    FD_SET(fds[0], &myset);
    tmpVal = select(fds[0] + 1, NULL, &myset, NULL, &tv);
    if (tmpVal == SOCKET_ERROR) {
        fprintf(stderr,"socket call for local server socket failed. Error Number %d.\n",WSAGetLastError());
        fflush(stderr);
        return(-1);
    }
    else if (tmpVal > 0) {
        lon = sizeof(int);
        if (!getsockopt(fds[0], SOL_SOCKET, SO_ERROR, (void*)(&valopt), &lon)) {
            if (valopt) {
                fprintf(stderr,"getsockopt indicates error on connect completion.\n");
                return(-1);
            }
        }
        else {
            fprintf(stderr,"getsockopt call for local client socket failed. Error Number %d.\n",WSAGetLastError());
            fflush(stderr);
            return(-1);
        }
    }
    else if (!tmpVal) {
        fprintf(stderr,"select on connect complete timed out.\n");
        fflush(stderr);
        return(-1);
    }
    return(0);
}

static int kill(int pid, int sig) {
    (void)sig;
    if (TerminateProcess((HANDLE)pid, 0))
        return 0;
    return -1;
}

static int spawn_memcached(int argc, char **argv) {
    char buffer[4096];
    int offset=0;

    for (int ii = 1; ii < argc; ++ii) {
        if (strcmp("-d", argv[ii]) != 0) {
            offset += snprintf(buffer + offset, sizeof(buffer) - offset,
                               "%s ", argv[ii]);
        }
    }

    STARTUPINFO sinfo = { .cb = sizeof(sinfo) };
    PROCESS_INFORMATION pinfo;

    if (CreateProcess(argv[0], buffer, NULL, NULL, FALSE,
                      CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
                      NULL, NULL, &sinfo, &pinfo)) {
        exit(0);
    }

    return -1;
}

static int sigaction(int sig, struct sigaction *act, struct sigaction *oact)
{
    void (*ret)(int) = signal(sig, act->sa_handler);
    if (oact != NULL) {
        oact->sa_handler = ret;
    }
    if (ret == SIG_ERR) {
        return -1;
    }

    return 0;
}

static void initialize_sockets(void) {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2,0), &wsaData) != 0) {
        fprintf(stderr, "Socket Initialization Error. Program aborted\n");
        exit(EXIT_FAILURE);
   }
}

// Let's make a really really dumb slow implementation :P
static char *strsep(char **stringp, char *pattern) {
   char *ptr = *stringp;

   char *first = NULL;
   int len = strlen(pattern);

   for (int i = 0; i < len; ++i) {
      char *n = strchr(*stringp, pattern[i]);
      if (n != NULL && (first == NULL || n < first)) {
         first = n;
      }
   }

   if (first != NULL) {
      *first = '\0';
      *stringp = first + 1;
   } else {
      *stringp = NULL;
   }

   return ptr;
}

#define random() rand()

#endif
