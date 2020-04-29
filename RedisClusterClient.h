/**
 * @author	lzy
 * @email	pictureyong@163.com
 * @date	2018-09-15 17:08
 * @brief	
 */

#ifndef _REDIS_CLUSTER_CLIENT_H__
#define _REDIS_CLUSTER_CLIENT_H__

#include <assert.h>
#include <map>
#include <list>
#include <vector>
#include <string>

// #define THREAD_LOCAL

#ifdef THREAD_LOCAL
#include <pthread.h>
#endif

#include "Util.h"
#include "hiredis/hiredis.h"

// #include <boost/thread.hpp>
// #include <mutex>
// #include <shared_mutex>
#include "Mutex.hpp"

#define MAX_REDIS_POOLSIZE 242
#define MAX_TIME_OUT       5
#define REDIS_SECTION_NUM  11

/*
namespace common  {
    typedef boost::recursive_mutex Mutex;
    typedef boost::recursive_mutex::scoped_lock MutexLock;
    typedef boost::shared_mutex RwMutex;
    typedef boost::unique_lock<RwMutex> WriterMutexLock;
    typedef boost::shared_lock<RwMutex> ReaderMutexLock;
}
*/
namespace common {
    typedef std::recursive_mutex Mutex;
    typedef std::unique_lock<std::recursive_mutex> MutexLock;
    typedef WFirstRWLock RwMutex;
    typedef UniqueWriteGuard<RwMutex> WriterMutexLock;
    typedef UniqueReadGuard<RwMutex> ReaderMutexLock;
}

namespace redis {

typedef struct RedisServers {
    // bool ParseFromJson(const Json::Value& j_data);
    typedef struct IpPort {
        IpPort(const std::string& host = "127.0.0.1", int port = 6379) :
            host(host),
            port(port) {
                ;
            }
        std::string host;
        int         port;
    } TIpPort;
    typedef struct Options {
        Options(const std::string& password = "", int timeout = 5, int pool_size = 48) :
            password(password),
            timeout(timeout),
            poolSize(pool_size) {
                ;
        }
        std::string password;
        int         timeout;
        int         poolSize;
    } TOptions;
    std::vector<TIpPort>    ipPorts;
    TOptions                options;
} TRedisServers;

class RedisResult {
public:
    RedisResult(){}
    ~RedisResult() {  if (Reply.reply) { freeReplyObject((void*)Reply.reply); }
    }
    
public:
    class RedisReply {
    public:
        RedisReply(redisReply *r) { reply = r;}
        RedisReply() { reply = NULL; }
        ~RedisReply() {}
    
        int type() const { return reply->type; }
        long long integer() const {return reply->integer; }
        int len() const { return reply->len; }
        char* str() const { return reply->str; }
        size_t elements() const { return reply->elements; }
        struct RedisReply element(size_t index) const { return RedisReply(reply->element[index]); }
        private:
        friend class RedisResult;
        redisReply *reply;
    };
    
    void Init(redisReply *r) { Reply.reply = r; }
    int type() const { return Reply.type(); }
    long long integer() const {return Reply.integer(); }
    int len() const { return Reply.len(); }
    char* str() const { return Reply.str(); }
    size_t elements() const { return Reply.elements(); }
    RedisReply element(size_t index) const { return Reply.element(index); }

private:
    RedisReply Reply;
};

struct RedisNodeInfo {
    std::string strinfo;
    common::RwMutex rwMutex; 
    std::string id;  ///< The node ID, a 40 characters random string generated when a node is created and never changed again (unless CLUSTER RESET HARD is used)
    std::string ipPort;
    std::string ip;
    uint16_t port;
    std::string flags;      ///< A list of comma separated flags: myself, master, slave, fail?, fail, handshake, noaddr, noflags
    bool isFail;           ///< flags contain fail
    bool isMaster;         ///< true if node is master, false if node is salve
    std::string masterId;  ///< The replication master
    int pingSent;      ///< Milliseconds unix time at which the currently active ping was sent, or zero if there are no pending pings
    int pongRecv;      ///< Milliseconds unix time the last pong was received
    int epoch;
    bool connected;         ///< The state of the link used for the node-to-node cluster bus
    std::vector<std::pair<int, int> > matchSlots; ///< A hash slot number or range

    bool Update(const RedisNodeInfo& node);
    bool IsNeedUpdate(const RedisNodeInfo& node);
    void SetFail(bool fail);
    void SetMaster(bool is_master);
    bool CheckSlot(int slotindex);
    bool ParseNodeString(const std::string &nodeString);
    void ParseSlotString(const std::string &SlotString);
};

struct RedisConnection;
typedef struct RedisConnectionList {
    RedisConnectionList() {
        ;
    }
    ~RedisConnectionList() {
    }
    typedef struct ConnectionMutex {
        ConnectionMutex() : 
            redisConnections(NULL),
            mutex(NULL) {
                ;
            }
        ~ConnectionMutex() {
            delete mutex;
        }
        std::list<RedisConnection*>*    redisConnections;
        common::Mutex*                  mutex;
    } TConnectionMutex;
    TConnectionMutex                connectionMutexs[REDIS_SECTION_NUM];
    RedisNodeInfo*                  redisNodeInfo;
} TRedisConnectionList;

typedef std::list <RedisConnection *>::iterator TRedisConnectionIter;

typedef struct RedisConnection {
    RedisConnection(TRedisConnectionList::TConnectionMutex* redis_connection_list, const std::string& host, int port, 
            const std::string& passwd, int pool_size) :
        redisConnectionList(redis_connection_list),
        mHost(host),
        mPort(port),
        mPasswd(passwd),
        mPoolSize(pool_size),
        mCtx(NULL) {
        ;
    }
    ~RedisConnection() {
       Destory(); 
    }
    
    bool ConnectWithTimeout();
    bool ReConnectWithTimeout(bool is_must, bool& is_need);
    void Destory();

    TRedisConnectionList::TConnectionMutex*   redisConnectionList;    ///< 指向所属的链接池
    std::string     mHost;
    int             mPort;
    std::string     mPasswd;
    int             mPoolSize;
    redisContext    *mCtx;
} TRedisConnection;

class CRedisClusterClient;

typedef struct RedisConnectionWrap {
    RedisConnectionWrap( TRedisConnection* conn, CRedisClusterClient* redis_cluster_client ) :
        redisConnection( conn ),
        redisClusterClient( redis_cluster_client ) {
            ;
        }
    ~RedisConnectionWrap();
    void Reset(TRedisConnection* conn, CRedisClusterClient* redis_cluster_client);
    TRedisConnection* operator->() const  {
        assert(redisConnection != NULL);
        return redisConnection;
    }
    bool operator!=(TRedisConnection* p) const { return redisConnection != p; }
    bool operator==(TRedisConnection* p) const { return redisConnection == p; }
    TRedisConnection*   redisConnection;
    CRedisClusterClient*    redisClusterClient;
} TRedisConnectionWrap;

class CRedisClusterClient {

friend struct RedisConnection;
friend struct RedisConnectionWrap;

public:
    static CRedisClusterClient* Instance();

protected:
    CRedisClusterClient();
    CRedisClusterClient(const CRedisClusterClient& redis_cluster_client);
    ~CRedisClusterClient();

public:
    // bool ConnectRedis(const Json::Value& j_config);
    bool ConnectRedis(const TRedisServers& redis_servers);
    bool ConnectRedis(const std::string& host, int port, const std::string& passwd, int timeout,
            int pool_size = 6, bool is_from_out = true);
    bool ReConnectRedis(TRedisConnection *pConn);
    void Keepalive();
    bool RedisCommand(RedisResult &result, const char *format, ...);
    bool RedisCommandArgv(const std::vector<std::string>& vDataIn, RedisResult &result);

public:
    // key
    bool exists(const std::string& key);
    bool del(const std::string& key);
    bool expire(const std::string& key, unsigned int timeout);

    // string
    bool set(const std::string& key, const std::string& val, unsigned int ex_time = 0);
    bool get(const std::string& key, std::string& value);
    
    // hash
    bool hSet(const std::string& key, const std::string& hKey, const std::string& value, int ex_time = 0);
    bool hMSet(const std::string& key, const std::map<std::string, std::string>& values, int ex_time = 0);
    bool hGet(const std::string& key, const std::string& hKey, std::string& value);
    bool hMGet(const std::string& key, const std::vector<std::string>& hKeys, std::map<std::string, std::string>& value);
    bool hGetAll(const std::string& key, std::map<std::string, std::string>& value);
    bool hDel(const std::string& key, const std::string& hKey);

    // list
    bool rPush(const std::string& key, const std::string& value);
    bool lPush(const std::string& key, const std::string& value);
    bool lGet(const std::string& key, std::vector<std::string>& value);

    // set
    bool sAdd(const std::string& key, const std::string& value);
    bool sMembers(const std::string& key, std::vector<std::string>& value);
    
private:
    static uint16_t crc16(const char *buf, int len);

private:
    void release();
    bool getRedisInfo(const std::string& host, int port, const std::string& passwd, bool& is_cluster,
            std::vector<RedisNodeInfo*>& nodes);
    bool clusterEnabled(redisContext *ctx);
    bool clusterInfo(redisContext *ctx);
    bool getClusterNodes(redisContext *ctx, std::vector<RedisNodeInfo*>& nodes);
    void resizeRedisConns(std::vector<TRedisConnectionList*>& redis_connections, const std::vector<RedisNodeInfo*>& nodes);
    bool connectRedisNode(TRedisConnectionList* redis_connects, const std::string& host, int port,
            const std::string& passwd, int pool_size);
    /**
     * @brief 执行 key 相关命令出错时当处理
     * @return true 可以处理继续执行，false 不可处理当错误，停止执行
     */
    bool dealErrorReply(const std::string& func, const std::string& key, redisReply* reply, RedisConnectionWrap& redis_conn_wrap);
    /**
     * @brief 更新 redis cluster nodes 信息
     */
    bool updateRedisClientClusterNodesWrap();
    /**
     * @brief 链接中 redis 信息改变
     */
    bool updateRedisClientClusterNodes();
    TRedisConnection* findNodeConnection(const std::string& key);
    TRedisConnection* findNodeConnectionByMoved(const std::string& moved);
    TRedisConnection *getConnection(TRedisConnectionList* redis_connection_list);
    int keyHashSlot(const char *key, size_t keylen);
    void freeConnection(TRedisConnection * pRedis);

public:
    int getKeySlotIndex(const std::string& key);

private:
#ifdef THREAD_LOCAL
    static std::once_flag s_redis_connect_flag;
    static pthread_key_t s_redis_connect;
    static void initPthreadKey();
    TRedisConnection* getRedisConnect();
    static void destructorRedisConnect(void* ptr);
#else
    std::vector<TRedisConnectionList*>  _redis_connections;
#endif
    static CRedisClusterClient* s_redis_cluster_client;
     
    bool                _cluster_enabled;
    common::RwMutex     _rw_mutex;
    common::Mutex       _update_mutex;
    int64               _recently_update_time_sec;
    TRedisServers       _redis_servers;
    std::string         _host;
    int                 _port;
    std::string         _password;
    std::string         _timeout;
};

typedef CRedisClusterClient RedisWrapper;

}

#endif
