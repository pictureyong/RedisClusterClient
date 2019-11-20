#include "RedisClusterClient.h"

namespace redis {

bool CRedisClusterClient::exists(const std::string& key) {
    RedisResult result;

    if ( !RedisCommand(result, "exists %s", key.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    return (result.integer() == 1);
}

bool CRedisClusterClient::del(const std::string& key) {
    RedisResult result;

    if ( !RedisCommand(result, "del %s", key.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    // return (result.integer() == 1);
    return true;
}

bool CRedisClusterClient::expire(const std::string& key, unsigned int timeout) {
    RedisResult result;

    if ( !RedisCommand(result, "expire %s %u", key.c_str(), timeout ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    return (result.integer() == 1);
}

bool CRedisClusterClient::set(const std::string& key, const std::string& val, unsigned int ex_time) {
    RedisResult result;

    if ( !RedisCommand(result, "set %s %s", key.c_str(), val.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    if ( 0 != ex_time ) {
        return expire(key, ex_time);
    }
    return true;
}

bool CRedisClusterClient::get(const std::string& key, std::string& value) {
    RedisResult result;

    if ( !RedisCommand(result, "get %s", key.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    value.assign(result.str(), result.len());
    return true;
}

bool CRedisClusterClient::hSet(const std::string& key, const std::string& hKey, const std::string& value, int ex_time) {
    RedisResult result;

    if ( !RedisCommand(result, "hset %s %s %s", key.c_str(), hKey.c_str(), value.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    if ( 0 != ex_time ) {
        return expire(key, ex_time);
    }
    return true;
}

bool CRedisClusterClient::hMSet(const std::string& key, const std::map<std::string, std::string>& values, int ex_time) {
    if ( values.empty() )  {
        VLOG(WARNING) << __FUNCTION__ << ", values.empty(), key: " << key;
        return true;
    }
    RedisResult result;
    std::vector<std::string> params(1, "HMSET");
    params.push_back( key );
    for (std::map<std::string, std::string>::const_iterator iter = values.begin(); 
            iter != values.end(); ++iter ) {
        params.push_back(iter->first);
        params.push_back(iter->second);
    }
    if ( !RedisCommandArgv(params, result ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommandArgv fail, key: " << key;
        return false;
    }
    if ( 0 != ex_time ) {
        return expire(key, ex_time);
    }
    return true;
}

bool CRedisClusterClient::hGet(const std::string& key, const std::string& hKey, std::string& value) {
    RedisResult result;

    if ( !RedisCommand(result, "hget %s %s", key.c_str(), hKey.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    value.assign(result.str(), result.len());
    return true;
}

bool CRedisClusterClient::hMGet(const std::string& key, const std::vector<std::string>& hKeys, std::map<std::string, std::string>& value) {
    RedisResult result;
    std::vector<std::string> params(1, "HMGET");
    params.push_back( key );
    params.insert(params.end(), hKeys.begin(), hKeys.end());

    if ( !RedisCommandArgv(params, result) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommandArgv fail, key: " << key;
        return false;
    }
    for ( size_t i = 0;i < hKeys.size() && i < result.elements(); ++i ) {
        if ( result.element(i).str() == NULL ) {
            value.insert(std::pair<std::string, std::string>(hKeys[i], "") );
        } else {
            value.insert(std::pair<std::string, std::string>(hKeys[i], result.element(i).str()) );
        }
    }
    return true;
}

bool CRedisClusterClient::hGetAll(const std::string& key, std::map<std::string, std::string>& value) {
    RedisResult result;
    value.clear();
    if ( !RedisCommand(result, "hgetall %s", key.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    for (size_t i = 0;i+1 < result.elements(); i+=2 ) {
        value.insert(std::pair<std::string, std::string>(result.element(i).str(), result.element(i+1).str()) );
    }
    return true;
}

bool CRedisClusterClient::hDel(const std::string& key, const std::string& hKey) {
    RedisResult result;

    if ( !RedisCommand(result, "hdel %s %s", key.c_str(), hKey.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    // return (result.integer() == 1);
    return true;
}

bool CRedisClusterClient::rPush(const std::string& key, const std::string& value) {
    RedisResult result;

    if ( !RedisCommand(result, "rpush %s %s", key.c_str(), value.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    return true;
}

bool CRedisClusterClient::lPush(const std::string& key, const std::string& value) {
    RedisResult result;
    if ( !RedisCommand(result, "lpush %s %s", key.c_str(), value.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    return true;
}

bool CRedisClusterClient::lGet(const std::string& key, std::vector<std::string>& value) {
    RedisResult result;
    value.clear();

    if ( !RedisCommand(result, "lrange %s 0 -1", key.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    for (size_t i = 0;i < result.elements(); ++i ) {
        value.push_back( result.element(i).str() );
    }
    return true;
}

bool CRedisClusterClient::sAdd(const std::string& key, const std::string& value) {
    RedisResult result;

    if ( !RedisCommand(result, "sadd %s %s", key.c_str(), value.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    return true;
}

bool CRedisClusterClient::sMembers(const std::string& key, std::vector<std::string>& value) {
    RedisResult result;
    value.clear();
    if ( !RedisCommand(result, "smembers %s", key.c_str() ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", RedisCommand fail, key: " << key;
        return false;
    }
    for (size_t i = 0;i < result.elements(); ++i ) {
        value.push_back( result.element(i).str() );
    }
    return true;
}


}
