// Author : lzy
// Email  : pictureyong@163.com
// Date   : 2019-11-19 11:33
// Description: 

#ifndef _MUTEX_H__
#define _MUTEX_H__

#include <mutex>
#include <condition_variable>

namespace common {
    
typedef struct MLock {
    MLock() = delete;
    explicit MLock(std::recursive_mutex& mutex) : _mutex(mutex) {
        _mutex.lock();
    }
    ~MLock() {
        _mutex.unlock();
    }
    std::recursive_mutex& _mutex;
} TMutexLock;

class WFirstRWLock {
public:
    WFirstRWLock() = default;
    ~WFirstRWLock() = default;

public:
    void LockRead() {
        std::unique_lock<std::mutex> ulk(_counter_mutex);
        _condition_r.wait(ulk, [=]()->bool {return _write_cnt == 0;});
        ++_read_cnt;
    }
    void LockWrite() {
        std::unique_lock<std::mutex> ulk(_counter_mutex);
        ++_write_cnt;
        _condition_w.wait(ulk, [=]()->bool {return _read_cnt==0 && !_inwrite_flag;});
        _inwrite_flag = true;
    }
    void UnlockRead() {
        std::unique_lock<std::mutex> ulk(_counter_mutex);
        if ( --_read_cnt == 0 && _write_cnt > 0 ) {
            _condition_w.notify_one();
        }
    }
    void UnlockWrite() {
        std::unique_lock<std::mutex> ulk(_counter_mutex);
        if ( --_write_cnt == 0 ) {
            _condition_r.notify_all();
        } else {
            _condition_w.notify_one();
        }
        _inwrite_flag = false;
    }

private:
    volatile size_t _read_cnt{0};
    volatile size_t _write_cnt{0};
    volatile bool _inwrite_flag{false};
    std::mutex _counter_mutex;
    std::condition_variable _condition_w;
    std::condition_variable _condition_r;
};

template <typename _RWLockable>
class UniqueWriteGuard {

public:
    explicit UniqueWriteGuard(_RWLockable& rw_lockable) 
        : _rw_lockable(rw_lockable) {
        _rw_lockable.LockWrite();
    }
    ~UniqueWriteGuard() {
        _rw_lockable.UnlockWrite();
    }
    UniqueWriteGuard() = delete;
    UniqueWriteGuard(const UniqueWriteGuard& ) = delete;
    UniqueWriteGuard& operator=(const UniqueWriteGuard&) = delete;

private:
    _RWLockable& _rw_lockable;
};

template <typename _RWLockable>
class UniqueReadGuard {

public:
    explicit UniqueReadGuard(_RWLockable& rw_lockable)
        : _rw_lockable(rw_lockable) {
        _rw_lockable.LockRead();
    }
    ~UniqueReadGuard() {
        _rw_lockable.UnlockRead();
    }
    UniqueReadGuard() = delete;
    UniqueReadGuard(const UniqueReadGuard& ) = delete;
    UniqueReadGuard& operator=(const UniqueReadGuard&) = delete;

private:
    _RWLockable &_rw_lockable;
};

}

#endif
