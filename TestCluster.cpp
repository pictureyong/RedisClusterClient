#include "RedisClusterClient.h"
#include <thread>
#include <iostream>
#include <set>

#define NUM_THREAD 100

bool InitGetClient() {
    std::vector<redis::TRedisServers::TIpPort> ip_ports;
    redis::TRedisServers::TIpPort ip_port_1("127.0.0.1", 6389);
    // redis 单点/集群 的 ip 和 port
    ip_ports.push_back(ip_port_1);
    // redis 密码  超时  链接池链接数量 
    redis::TRedisServers::TOptions options("1234567Myc", 5, NUM_THREAD);
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

bool TestHash(const std::string& pre) {
    VLOG(DEBUG) << __FUNCTION__ << ",START";
    std::string key = pre + "_hash_key";
    std::string hkey = pre + "__hash_hkey";
    redis::CRedisClusterClient::Instance()->del( key );
    std::string value(10, 'a');
    if ( !redis::CRedisClusterClient::Instance()->hSet(key, hkey, value) ) {
        VLOG(ERROR) << __FUNCTION__ << ", HSet Error.";
        return false;
    }
    std::string get_value;
    if ( !redis::CRedisClusterClient::Instance()->hGet(key, hkey, get_value) ) {
        VLOG(ERROR) << __FUNCTION__ << ", hGet Error.";
        return false;
    }

    if ( value != get_value ) {
        VLOG(ERROR) << __FUNCTION__ << ", value != get_value.";
        return false;
    }

    if ( !redis::CRedisClusterClient::Instance()->del(key) ) {
        VLOG(ERROR) << __FUNCTION__ << ", del Error.";
        return false;
    }

    std::vector<std::string> hkeys;
    std::map<std::string, std::string> values;
    for (int i = 0;i < 1000; ++i ) {
        std::string t_hkey = hkey + std::to_string(i);
        hkeys.push_back( t_hkey );
        values.insert(std::pair<std::string, std::string>(t_hkey, value) );
    }
    if ( !redis::CRedisClusterClient::Instance()->hMSet(key, values) ) {
        VLOG(ERROR) << __FUNCTION__ << ", hMSet Error.";
        return false;
    }
    std::map<std::string, std::string> hmget_values;
    if ( !redis::CRedisClusterClient::Instance()->hMGet(key, hkeys, hmget_values) ) {
        VLOG(ERROR) << __FUNCTION__ << ", hMGet Error.";
        return false;
    }
    if ( values != hmget_values ) {
        VLOG(ERROR) << __FUNCTION__ << ", values != hmget_values.";
        return false;
    }
    std::map<std::string, std::string> get_values;
    if ( !redis::CRedisClusterClient::Instance()->hGetAll(key, get_values) ) {
        VLOG(ERROR) << __FUNCTION__ << ", hGetAll Error.";
        return false;
    }

    if ( values != get_values ) {
        VLOG(ERROR) << __FUNCTION__ << ", values != get_values.";
        return false;
    }
    for ( std::map<std::string, std::string>::const_iterator iter = get_values.begin();
            iter != get_values.end(); ++iter ) {
        if ( !redis::CRedisClusterClient::Instance()->hDel(key, iter->first) ) {
            VLOG(ERROR) << __FUNCTION__ << ", HDel Error.";
            return false;
        }
    }
    return true;
}

bool TestList(const std::string& pre) {
    VLOG(DEBUG) << __FUNCTION__ << ",START";
    std::string key = pre + "_list_key";
    redis::CRedisClusterClient::Instance()->del( key );
    std::vector<std::string> values;
    for ( int i = 0;i < 1000; ++i ) {
        values.push_back( std::string(i+100, 'a') );
        if ( !redis::CRedisClusterClient::Instance()->rPush(key, values[i]) ) {
            VLOG(ERROR) << __FUNCTION__ << ", rPush Error.";
            return false;
        }
    }
    std::vector<std::string> get_values;
    if ( !redis::CRedisClusterClient::Instance()->lGet(key, get_values) ) {
        VLOG(ERROR) << __FUNCTION__ << ", lGet Error.";
        return false;
    }
    VLOG(DEBUG) << __FUNCTION__ << ", values.size: " << values.size() << ", get_values.size: " << get_values.size();
    if ( values != get_values ) {
        VLOG(ERROR) << __FUNCTION__ << ", values != get_values.";
        return false;
    }
    if ( !redis::CRedisClusterClient::Instance()->del(key) ) {
        VLOG(ERROR) << __FUNCTION__ << ", del Error.";
        return false;
    }
    return true;
}

bool TestSet(const std::string& pre) {
    VLOG(DEBUG) << __FUNCTION__ << ",START";
    std::string key = pre + "_set_key";
    redis::CRedisClusterClient::Instance()->del( key );
    std::set<std::string> values;
    for ( int i = 0;i < 100; ++i ) {
        std::string value(i+100, 'a');
        values.insert( value );
        if ( !redis::CRedisClusterClient::Instance()->sAdd(key, value) ) {
            VLOG(ERROR) << __FUNCTION__ << ", rPush Error.";
            return false;
        }
    }
    std::vector<std::string> get_values;
    if ( !redis::CRedisClusterClient::Instance()->sMembers(key, get_values) ) {
        VLOG(ERROR) << __FUNCTION__ << ", lGet Error.";
        return false;
    }
    VLOG(DEBUG) << __FUNCTION__ << ", values.size: " << values.size() << ", get_values.size: " << get_values.size();
    if ( values != std::set<std::string>(get_values.begin(), get_values.end()) ) {
        for (size_t i = 0;i < get_values.size(); ++i ) {
            std::cout << get_values[i] << std::endl;
        }
        VLOG(ERROR) << __FUNCTION__ << ", values != get_values.";
        return false;
    }
    if ( !redis::CRedisClusterClient::Instance()->del(key) ) {
        VLOG(ERROR) << __FUNCTION__ << ", del Error.";
        return false;
    }
    return true;
}

bool TestString(const std::string& pre) {
    VLOG(DEBUG) << __FUNCTION__ << ",START";
    std::string key = pre + "_strg_key";
    std::string value(1000, 'c');

    redis::CRedisClusterClient::Instance()->del( key );

    if ( !redis::CRedisClusterClient::Instance()->set(key, value) ) {
        VLOG(ERROR) << __FUNCTION__ << ", set Error.";
        return false;
    }
    std::string get_value;
    if ( !redis::CRedisClusterClient::Instance()->get(key, get_value) ) {
        VLOG(ERROR) << __FUNCTION__ << ", get Error.";
        return false;
    }
    if ( value != get_value ) {
        VLOG(ERROR) << __FUNCTION__ << ", value != get_value.";
        return false;
    }
    if ( !redis::CRedisClusterClient::Instance()->del(key) ) {
        VLOG(ERROR) << __FUNCTION__ << ", del Error.";
        return false;
    }
    return true;
}

bool ComprehensiveTest(const std::string& pre) {
    VLOG(DEBUG) << __FUNCTION__ << ",START";
    for (int i = 0;i < 10; ++i ) {
        std::string key = pre + "_" + std::to_string(i);
        std::string value(10, 'a');
        if ( !redis::CRedisClusterClient::Instance()->set(key, value, 500) ) {
            VLOG(ERROR) << __FUNCTION__ << ", set Error.";
            return false;
        }
        for (int j = 0;j < 50; ++j ) {
            std::string key = pre + "_hash_" + std::to_string(i);
            std::string hkey = "key_" + std::to_string(j);
            if ( !redis::CRedisClusterClient::Instance()->hSet(key, hkey, value, 500) ) {
                VLOG(ERROR) << __FUNCTION__ << ", HSet Error.";
                return false;
            }
            key = pre + "_set_" + std::to_string(i);
            if ( !redis::CRedisClusterClient::Instance()->sAdd(key, value) ) {
                VLOG(ERROR) << __FUNCTION__ << ", rPush Error.";
                return false;
            }
        }
    }
    do {
        redis::CRedisClusterClient::Instance()->set("test_string", "hello world");
        if ( redis::CRedisClusterClient::Instance()->hSet("test_string", "hkey", "value") ) {
            VLOG(ERROR) << __FUNCTION__ << ", Check error..";
            return false;
        }
        if ( redis::CRedisClusterClient::Instance()->sAdd("test_string", "hello world") ) {
            VLOG(ERROR) << __FUNCTION__ << ", Check error..";
            return false;
        }
        std::map<std::string, std::string> values;
        if ( redis::CRedisClusterClient::Instance()->hGetAll("test_string", values) ) {
            VLOG(ERROR) << __FUNCTION__ << ", Check error..";
            return false;
        }
        std::vector<std::string> vvs;
        if ( redis::CRedisClusterClient::Instance()->sMembers("test_string", vvs) ) {
            VLOG(ERROR) << __FUNCTION__ << ", Check error..";
            return false;
        }
    } while(0);
    do {
        redis::CRedisClusterClient::Instance()->hSet("test_h", "hkey", "value");
        std::vector<std::string> hkeys;
        hkeys.push_back("hkey");
        hkeys.push_back("hkey");
        hkeys.push_back("test");
        std::map<std::string, std::string> value;
        redis::CRedisClusterClient::Instance()->hMGet("test_h", hkeys, value);
        for (std::map<std::string, std::string>::iterator iter = value.begin();iter != value.end(); ++iter ) {
            std::cout << "KEY: " << iter->first << ", VALUE: " << iter->second << std::endl;
        }
    } while(0);
    return true;
}

void Run(int idx) {
    VLOG(DEBUG) << __FUNCTION__ << ",START, idx: " << idx;
    int64 st = GetTimeInMs();
    for (size_t i = 0;i < 10; ++i )  {
    std::string pre = std::to_string(idx) + "_" + std::to_string(st);
    if ( !TestHash( pre ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", TestHash Fail.";
        exit(-1);
        return ;
    }
    if ( !TestList( pre ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", TestList Fail.";
        exit(-1);
        return ;
    }
    if ( !TestSet( pre ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", TestSet Fail.";
        exit(-1);
        return ;
    }
    if ( !TestString( pre ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", TestString Fail.";
        exit(-1);
        return ;
    }
    /*
    if ( !ComprehensiveTest( pre) ) {
        VLOG(ERROR) << __FUNCTION__ << ", ComprehensiveTest Fail.";
        exit(-1);
        return ;
    }
    */
    }
    VLOG(DATA) << __FUNCTION__ << "OVER, HS: " <<  GetTimeInMs() - st;
    VLOG(DATA) << "";
}

bool MutilThreadTest() {
    int64 st = GetTimeInMs();
    std::vector<std::thread> threads;
    for (int i = 0;i < NUM_THREAD; ++i ) {
        threads.push_back( std::thread(Run, i) );
    }
    for (auto& th : threads ) th.join();

    VLOG(NOTE) << __FUNCTION__ << "MAIN OVER, HS: " <<  GetTimeInMs() - st;
    return true;
}

int main(int argc, char** argv)
{
    int64 st = GetTimeInMs();
    if ( !InitGetClient() ) {
        VLOG(ERROR) << __FUNCTION__ << ", InitGetClient fail.";
        return -1;
    }
    //Run(0);
    MutilThreadTest();

    VLOG(DATA) << __FUNCTION__ << "OVER, HS: " <<  GetTimeInMs() - st;

    VLOG(DATA) << "";

    return 0;
}
