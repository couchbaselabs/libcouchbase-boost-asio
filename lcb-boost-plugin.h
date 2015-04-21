#include <libcouchbase/couchbase.h>
#include <libcouchbase/iops.h>

namespace lcb {

class Buffer {
public:
    lcb_IOV *get_buf_segs() { return niov; }
    size_t get_num_segs() { return niov; }
    std::vector<lcb_IOV *> get_vector();

    typedef lcb_IOV* const_iterator;
    typedef lcb_IOV value_type;

    const_iterator begin() { return iov; }
    const_iterator end() { return iov + niov; }

private:
    lcb_IOV *iov;
    size_t niov;
};

/**
 * Implementation must support run(), stop()
 */
template <typename Implementation>
class CompletionRoutines {
    typedef lcb_io_connect_cb ConnectedHandler;
    typedef lcb_ioC_write2_callback WriteHandler;
    typedef lcb_ioC_read2_callback ReadHandler;
    typedef lcb_sockdata_st SD;
    typedef typename Implementation::socket_type socktype_;
    typedef typename Implementation::timer_type timer_;
    struct ConnectContext { ConnectedHandler handler; };
    struct ReadContext { ReadHandler handler; void *arg; };
    struct WriteContext { WriteHandler handler; void *arg; };
    struct TimerContext { lcb_ioE_callback handler; void *arg; };

    socktype_ newsock(int domain, int family, int protocol);

    bool connect_socket(socktype_&, const sockaddr*, unsigned, const ConnectContext&);
    void socket_connected(socktype_&, bool, const ConnectContext&);
    bool start_read(socktype_&, Buffer&, const ReadContext&);
    void got_read(socktype_&, bool, const ReadContext&);
    bool start_write(socktype_&, const Buffer&, const WriteContext&);
    void got_write(socktype_&, const WriteContext&);
    void close_socket(socktype_&);
    timer_ create_timer();
    void run();
    void pause();
};

}
