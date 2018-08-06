#include <string>
#include <vector>
#include <array>
#include <queue>
#include <boost/asio.hpp>
#include <boost/chrono.hpp>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/iops.h>

using std::string;
using std::vector;

using boost::asio::ip::tcp;
using boost::system::error_code;
using namespace boost::asio;

class BoostSocket;
class BoostTimer;
struct H_Connect;
struct H_Read;

namespace {
typedef lcb_io_opt_st _IOPS;
typedef lcb_ioE_callback _ECB;
typedef lcb_sockdata_st _SD;
}

class BoostIOPS : _IOPS {
public:
    BoostIOPS(io_service *svc_in);
    void set_error() { set_error(errno); }
    void set_error(const error_code& ec) { set_error(ec.value()); }
    void set_error(int err) { LCB_IOPS_ERRNO(this) = err; }
    io_service& get_service() { return *svc; }

    void ref() {
        refcount++;
    }

    void unref() {
        if (--refcount) {
            return;
        }
        delete this;
    }

    void run_until_dead() {
        if (!is_service_owner) {
            return;
        }

        ref();
        while (refcount > 2) {
            // Run until refcount hits 0.
            svc->reset();
            svc->run();
        }
        delete this;
    }

    void run() {
        if (!is_service_owner) {
            return;
        }

        is_stopped = false;
        svc->reset();
        svc->run();
        if (!is_stopped) {
            //printf("Loop stopped on its own!\n");
        }
        is_stopped = true;
    }

    void stop() {
        //printf("Stop called explicitly!\n");
        if (!is_service_owner) {
            return;
        }

        if (is_stopped) {
            return;
        }
        is_stopped = true;
        svc->stop();
    }

private:
    boost::asio::io_service *svc;
    boost::asio::io_service svc_s;
    size_t refcount;
    bool is_stopped;
    bool is_service_owner;
};

class BoostSocket : public _SD {
public:
    BoostSocket(BoostIOPS *parent, int domain) : m_socket(parent->get_service()) {

        ::memset((_SD*)this, 0, sizeof (_SD));
        m_parent = parent;
        rdarg = NULL;
        rdcb = NULL;
        refcount = 1;
        wcount = 0;
        lcb_closed = false;

        ip::address ipaddr;
        if (domain == AF_INET) {
            ipaddr = (ip::address)ip::address_v4();
            m_socket.open(tcp::v4());
        } else {
            ipaddr = (ip::address)ip::address_v6();
            m_socket.open(tcp::v6());
        }
        m_socket.bind(tcp::endpoint(ipaddr, 0));
        m_parent->ref();
        socket = m_socket.native_handle(); // lcb_sockdata_t
    }

    ~BoostSocket() {
        m_parent->unref();
    }

    int start_connect(const sockaddr *saddr, lcb_io_connect_cb callback) {
        ip::address ipaddr;
        int port;
        if (m_socket.local_endpoint().address().is_v4()) {
            const sockaddr_in *sin = (const sockaddr_in *)saddr;
            port = sin->sin_port;
            ipaddr = ip::address_v4(htonl(sin->sin_addr.s_addr));

        } else if (m_socket.local_endpoint().address().is_v6()) {
            const sockaddr_in6 *sin6 = (const sockaddr_in6 *)saddr;
            port = sin6->sin6_port;
            std::array<unsigned char, 16> addrbytes;
            ::memcpy(addrbytes.data(), &sin6->sin6_addr, 16);
            ipaddr = ip::address_v6(addrbytes, sin6->sin6_scope_id);
        } else {
            m_parent->set_error(ENOTSUP);
            return -1;
        }

        port = htons(port);
        m_socket.async_connect(tcp::endpoint(ipaddr, port), H_Connect(callback, this));
        return 0;
    }

    int start_read(lcb_IOV *iov, size_t n, void *uarg, lcb_ioC_read2_callback cb) {
        rdcb = cb;
        rdarg = uarg;

        ref();
        std::vector<mutable_buffer> bufs;
        for (size_t ii = 0; ii < n; ii++) {
            bufs.push_back(mutable_buffer(iov[ii].iov_base, iov[ii].iov_len));
        }
        m_socket.async_read_some(bufs, H_Read(this));
        return 0;
    }

    struct H_Write {
        BoostSocket *sock;
        void *arg;
        lcb_ioC_write2_callback cb;
        H_Write(BoostSocket *s, lcb_ioC_write2_callback callback, void *cbarg) : sock(s), arg(cbarg), cb(callback) {}

        void operator() (const error_code& ec, size_t) {
            int val = 0;
            if (ec) { val = -1; sock->m_parent->set_error(ec); }
            sock->wcount--;
            cb(sock, val, arg);
            sock->flush_wpend();
            sock->unref();
        }

        void operator()() {
            sock->wcount--;
            cb(sock, -1, arg);
            sock->unref();
        }
    };

    struct H_QueuedWrite : H_Write {
        H_QueuedWrite(BoostSocket *s, lcb_ioC_write2_callback callback, void *cbarg, std::vector<const_buffer>& bufs) :
        H_Write(s, callback, cbarg), bufs(bufs) {}
        std::vector<const_buffer> bufs;
    };

    void flush_wpend() {
        while (!pending_writes.empty()) {
            H_QueuedWrite w = pending_writes.front();
            pending_writes.pop();
            if (!lcb_closed) {
                async_write(m_socket, w.bufs, w);
                break;
            } else {
                m_parent->get_service().post(w);
            }
        }
    }

    int start_write(lcb_IOV *iov, size_t niov, void *uarg, lcb_ioC_write2_callback cb) {
        ref();

        std::vector<const_buffer> bufs;
        for (size_t ii = 0; ii < niov; ii++) {
            bufs.push_back(const_buffer(iov[ii].iov_base, iov[ii].iov_len));
        }

        if (lcb_closed) {
            m_parent->set_error(ESHUTDOWN);
            return -1;
        }

        if (pending_writes.empty() && wcount == 0) {
            async_write(m_socket, bufs, H_Write(this, cb, uarg));
        } else {
            pending_writes.push(H_QueuedWrite(this, cb, uarg, bufs));
        }
        wcount++;
        return 0;
    }

    int is_closed(int flags) {
        if (!m_socket.is_open()) {
            return LCB_IO_SOCKCHECK_STATUS_CLOSED;
        }
        error_code ec;
        char buf;
        mutable_buffers_1 dummy(&buf, 1);

        while (true) {
            bool was_blocking = m_socket.non_blocking();
            m_socket.non_blocking(true, ec);
            if (ec) {
                return LCB_IO_SOCKCHECK_STATUS_UNKNOWN;
            }
            size_t nr = m_socket.receive(dummy, tcp::socket::message_peek, ec);
            m_socket.non_blocking(was_blocking);
            if (ec) {
                if (ec == error::would_block) {
                    return LCB_IO_SOCKCHECK_STATUS_OK;
                } else if (ec == error::interrupted) {
                    continue;
                } else {
                    return LCB_IO_SOCKCHECK_STATUS_CLOSED;
                }
            } else if (nr > 0 && (flags & LCB_IO_SOCKCHECK_PEND_IS_ERROR)) {
                return LCB_IO_SOCKCHECK_STATUS_CLOSED;
            } else {
                return LCB_IO_SOCKCHECK_STATUS_OK;
            }
        }
        return LCB_IO_SOCKCHECK_STATUS_UNKNOWN;
    }

    int get_nameinfo(lcb_nameinfo_st *ni) {
        // Much simpler to just use getsockname!
        int rv;
        socklen_t lenp;

        lenp = sizeof(sockaddr_storage);
        rv = getsockname(socket, ni->local.name, &lenp);

        if (rv != 0) {
            m_parent->set_error();
            return -1;
        }

        lenp = sizeof(sockaddr_storage);
        rv = getpeername(socket, ni->remote.name, &lenp);
        if (rv != 0) {
            m_parent->set_error();
            return -1;
        }
        *ni->local.len = lenp;
        *ni->remote.len = lenp;
        return 0;
    }

    void close() {
        error_code ecdummy;
        m_socket.shutdown(socket_base::shutdown_both, ecdummy);
        m_socket.close(ecdummy);
        lcb_closed = true;
        unref();
    }
    void ref() { refcount++; }
    void unref() { if (! --refcount) { delete this; } }

private:
    tcp::socket m_socket;
    BoostIOPS *m_parent;
    lcb_ioC_read2_callback rdcb;
    void *rdarg;
    size_t refcount;
    size_t wcount;
    bool lcb_closed; // Closed from libcouchbase
    std::queue<H_QueuedWrite> pending_writes;

    struct H_Connect {
        H_Connect(lcb_io_connect_cb cb, BoostSocket *s) : callback(cb), sock(s) {}
        lcb_io_connect_cb callback;
        BoostSocket *sock;
        void operator () (const error_code& ec) {
            int rv = ec ? -1 : 0;
            if (ec) { sock->m_parent->set_error(ec); }
            callback((_SD *)sock, rv);
        }
    };

    struct H_Read {
        BoostSocket *sock;
        H_Read(BoostSocket *s) : sock(s) {}

        void operator() (const error_code& ec, size_t nbytes) {
            ssize_t val = -1;
            if (ec) { sock->m_parent->set_error(ec); }
            else { val = nbytes; }

            sock->rdcb(sock, val, sock->rdarg);
            sock->unref();
        }
    };
};

class BoostTimer : public std::enable_shared_from_this<BoostTimer> {
public:
    struct H_Timer {
        std::shared_ptr<BoostTimer> parent;
        H_Timer(const std::shared_ptr<BoostTimer>& tm) : parent(tm) {}
        void operator() (const error_code& ec) {
            if (ec) { return; }
            // This callback can be called even after the timer has been canceled.
            if (parent->callback == NULL) { return; }
            parent->callback(-1, 0, parent->arg);
        }
    };

    BoostTimer(BoostIOPS *parent) : m_timer(parent->get_service()), m_parent(parent) {
        callback = NULL;
        arg = NULL;
        m_parent->ref();
    }

    ~BoostTimer() {
        m_parent->unref();
    }

    void schedule(uint32_t usec, _ECB cb, void *arg) {
        this->callback = cb;
        this->arg = arg;
        m_timer.expires_from_now(boost::posix_time::microseconds(usec));
        m_timer.async_wait(H_Timer(shared_from_this()));
    }

    void cancel() {
        callback = NULL;
        arg = NULL;
        m_timer.cancel();
    }

private:
    boost::asio::deadline_timer m_timer;
    BoostIOPS *m_parent;
    _ECB callback;
    void *arg;
};

static BoostIOPS *getIops(_IOPS* io) {
    return reinterpret_cast<BoostIOPS *>(io);
}

extern "C" {
static void run_loop(_IOPS* io) {
    getIops(io)->run();
}
static void stop_loop(_IOPS* io) {
    getIops(io)->stop();
}
static void* create_timer(_IOPS* io) {
    // This dynamically allocated shared_ptr will be deleted in
    // 'destroy_timer' function
    return new std::shared_ptr<BoostTimer>(new BoostTimer(getIops(io)));
}
static void destroy_timer(_IOPS*, void *timer) {
    auto* timer_ptr = static_cast<std::shared_ptr<BoostTimer>*>(timer);
    delete timer_ptr;

    // BoostTimer can survive after 'destroy_timer' function call because the
    // H_Timer callback can be called after the timer is canceled.
}
static int schedule_timer(_IOPS*, void *timer, uint32_t us, void *arg, _ECB cb) {
    auto& timer_ref = *static_cast<std::shared_ptr<BoostTimer>*>(timer);
    timer_ref->schedule(us, cb, arg);
    return 0;
}
static void cancel_timer(_IOPS*, void *timer) {
    auto& timer_ref = *static_cast<std::shared_ptr<BoostTimer>*>(timer);
    timer_ref->cancel();
}
static _SD *create_socket(_IOPS* io, int domain, int, int) {
    return new BoostSocket(getIops(io), domain);
}
static int connect_socket(_IOPS*, _SD* sock, const sockaddr* addr, unsigned, lcb_io_connect_cb cb) {
    return ((BoostSocket *)sock)->start_connect(addr, cb);
}
static int get_nameinfo(_IOPS*, _SD *sock, lcb_nameinfo_st *ni) {
    return ((BoostSocket *)sock)->get_nameinfo(ni);
}
static int read_socket(_IOPS*, _SD *sock, lcb_IOV *iov, lcb_SIZE niov, void *uarg, lcb_ioC_read2_callback cb) {
    return ((BoostSocket *)sock)->start_read(iov, niov, uarg, cb);
}
static int write_socket(_IOPS*, _SD* sock, lcb_IOV *iov, lcb_SIZE niov, void *uarg, lcb_ioC_write2_callback cb) {
    return ((BoostSocket *)sock)->start_write(iov, niov, uarg, cb);
}
static unsigned close_socket(_IOPS*, _SD* sock) {
    ((BoostSocket *)sock)->close(); return 0;
}
static int check_closed(_IOPS*, _SD* sock, int flags) {
    return ((BoostSocket *)sock)->is_closed(flags);
}
static void iops_dtor(_IOPS *io) {
    getIops(io)->run_until_dead();
}

static void get_procs(int, lcb_loop_procs *loop, lcb_timer_procs *tm,
    lcb_bsd_procs*, lcb_ev_procs*, lcb_completion_procs* iocp, lcb_iomodel_t *model)
{
    *model = LCB_IOMODEL_COMPLETION;
    loop->start = run_loop;
    loop->stop = stop_loop;

    tm->create = create_timer;
    tm->destroy = destroy_timer;
    tm->cancel = cancel_timer;
    tm->schedule = schedule_timer;

    iocp->socket = create_socket;
    iocp->connect = connect_socket;
    iocp->nameinfo = get_nameinfo;
    iocp->read2 = read_socket;
    iocp->write2 = write_socket;
    iocp->close = close_socket;
    iocp->is_closed = check_closed;
}
}

// Constructor
BoostIOPS::BoostIOPS(io_service *svc_in)
{
    ::memset((_IOPS*)this, 0, sizeof(_IOPS));
    refcount = 1;
    is_stopped = false;
    version = 2;
    v.v2.get_procs = get_procs;
    destructor = iops_dtor;

    if (svc_in != NULL) {
        svc = svc_in;
        is_service_owner = false;
    } else {
        svc = &svc_s;
        is_service_owner = true;
    }
}

// API Call
extern "C" {
lcb_error_t
lcb_create_boost_asio_io_opts(int, _IOPS **io, void *arg)
{
    *io = (_IOPS *) new BoostIOPS((io_service *)arg);
    return LCB_SUCCESS;
}
}
