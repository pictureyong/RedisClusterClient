# RedisClusterClient
## introduce
### This is a Redis client, based on hiredis and written in C++11.
### Connection pool and thread safety 
### Support Redis Cluster client
### Support master-standby switching
### Support slot adjustment

# Install
```
git clone https://github.com/pictureyong/RedisClusterClient.git --recursive 
make
```

# use
> 如果没用 C++11 环境，可以替换代码库中锁相关的实现则可以编译出 libredis_cluster_client.a \
```
// RedisClusterClient.h
/*
namespace common  {
    typedef boost::recursive_mutex Mutex;
    typedef boost::recursive_mutex::scoped_lock MutexLock;
    typedef boost::shared_mutex RwMutex;
    typedef boost::unique_lock<RwMutex> WriterMutexLock;
    typedef boost::shared_lock<RwMutex> ReaderMutexLock;
}
*/
// 需 C++ 11 支持
#include "Mutex.hpp"
namespace common {
    typedef std::recursive_mutex Mutex;
    typedef std::unique_lock<std::recursive_mutex> MutexLock;
    typedef WFirstRWLock RwMutex;
    typedef UniqueWriteGuard<RwMutex> WriterMutexLock;
    typedef UniqueReadGuard<RwMutex> ReaderMutexLock;
}

```
> 可参考 TestCluster.cpp 实现 \
```
// 连接初始化
bool InitGetClient() {
    std::vector<redis::TRedisServers::TIpPort> ip_ports;
    redis::TRedisServers::TIpPort ip_port_1("127.0.0.1", 6379);
    // redis 单点/集群 的 ip 和 port
    ip_ports.push_back(ip_port_1);
    // redis 密码  超时  链接池链接数量 
    redis::TRedisServers::TOptions options("1234567", 5, 20);
    redis::TRedisServers redis_servers;
    redis_servers.ipPorts = ip_ports;
    redis_servers.options = options;
    //  使用 配置式链接，配置支持 json 格式的配置，需要安装第三方 json 库， 并打开代码中 Json 相关注释
    //  推荐 jsoncpp ：https://github.com/open-source-parsers/jsoncpp.git
    /* json 配置格式
    {
        "ip_ports": [
        {
            "host": "10.0.0.10",
            "port": 6379
        },
        {
            "host": "10.0.2.201",
            "port": 16379
        },
        {
            "host": "10.0.202",
            "port": 16379
        }
        ],
        "options": {
            "password": "1234567",
            "pool_size": 10
        }
    }
    */
    // 通过配置结构体连接 redis
    if ( !redis::CRedisClusterClient::Instance()->ConnectRedis(redis_servers) ) {
    // 通过参数连接 redis
    // if ( !redis::CRedisClusterClient::Instance()->ConnectRedis("127.0.0.1", 6389, "1234567", 6, 20) ) {
        VLOG(FATAL) << __FUNCTION__ << ", ConnectRedis fail.";
        return false;
    }
    return true;
}
```
> 主要RPC服务调用 redis （单点和集群）\
> make 默认产出静态库 libredis_cluster_client.a
