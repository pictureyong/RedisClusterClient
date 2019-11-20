#include "RedisClusterClient.h"

#include <string.h>
#include <unistd.h>
// #include <sys/time.h>
#include <iostream>
#include <memory>

//  #include "Common/Base/stringUtil.h"
// #include "Common/Base/logging_level.h"
// #include "Common/Base/time.h"

namespace redis {

#define REDIS_MAX_RETRY_COUNTER 3
#define UPDATE_MIN_INTERVAL_SEC 30

#define FREE_REDIS_REPLY( reply ) \
    do {\
        if ( reply != NULL ) {\
            freeReplyObject( reply );\
        }\
    } while(0)\

#define CHECK_REPLY( reply ) \
    do{\
        if( reply == NULL || reply->type == REDIS_REPLY_ERROR ) {\
            if( reply ) {\
                VLOG(WARNING) << __FUNCTION__ << ", redisCommand error, " << reply->str;\
                freeReplyObject(reply);\
            } else {\
                VLOG(WARNING) << __FUNCTION__ << ", redisCommand error, reply is NULL;";\
            }\
            return false;\
        }\
    } while(0)\
/*
bool RedisServers::ParseFromJson(const Json::Value& j_data) {
    const Json::Value& j_ip_ports = j_data["ip_ports"];
    if ( j_ip_ports.isArray() ) {
        for (int i = 0;i < j_ip_ports.size(); ++i  ) {
            TIpPort ip_port;
            if ( j_ip_ports[i]["host"].isString() ) {
                ip_port.host = j_ip_ports[i]["host"].asString();
            } else {
                VLOG(ERROR) << __FUNCTION__ << ", host is not string.";
                return false;
            }
            if ( j_ip_ports[i]["port"].isInt() ) {
                ip_port.port = j_ip_ports[i]["port"].asInt();
            } else {
                VLOG(ERROR) << __FUNCTION__ << ", port is not int.";
                return false;
            }
            ipPorts.push_back( ip_port );
        }
    } else {
        VLOG(ERROR) << __FUNCTION__ << ", j_ip_ports is not array.";
        return false;
    }
    const Json::Value& j_options = j_data["options"];
    if ( j_options["password"].isString() ) {
        options.password = j_options["password"].asString();
    }
    if ( j_options["timeout"].isInt() ) {
        options.timeout = j_options["timeout"].asInt();
    }
    if ( j_options["pool_size"].isInt() ) {
        options.poolSize = j_options["pool_size"].asInt();
    }
    return true;
}
*/
bool RedisConnection::ConnectWithTimeout() {
    struct timeval timeoutVal;
    timeoutVal.tv_sec = MAX_TIME_OUT;
    timeoutVal.tv_usec = 0;
    
    mCtx = redisConnectWithTimeout(mHost.c_str(), mPort, timeoutVal);
    if ( mCtx == NULL || mCtx->err ) {
        if ( mCtx ) {
            VLOG(ERROR) << __FUNCTION__ << ", redisConnectWithTimeout fail, host: " << mHost << ", port: " << mPort;
        } else {
            VLOG(ERROR) << __FUNCTION__ << ", redisConnectWithTimeout can not allocate redis context,  host: "
                << mHost << ", port: " << mPort;
        }
        return false;
    } else {
        VLOG(DATA) << __FUNCTION__ << ", Connect success, host: " << mHost << ", port: " << mPort;
    }
    if ( !mPasswd.empty() ) {
        VLOG(DEBUG) << __FUNCTION__ << ", AUTH...";
        redisReply* reply = (redisReply*)redisCommand(mCtx, "AUTH %s", mPasswd.c_str() );
        CHECK_REPLY( reply );
        freeReplyObject( reply );
    }
    return true;
}

bool RedisConnection::ReConnectWithTimeout(bool is_must, bool& is_need) {
    if ( !is_must && mCtx != NULL && mCtx->err == 0 ) {
        VLOG(WARNING) << __FUNCTION__ << ", No need do.";
        is_need = false;
        return false;
    } else {
        is_need = true;
        if ( mCtx != NULL ) {
            VLOG(DATA) << __FUNCTION__ << ", err: " << mCtx->err << ", msg: " << mCtx->errstr;
        }
        Destory();
        return ConnectWithTimeout();
    }
}

void RedisConnection::Destory() {
    if ( mCtx != NULL ) {
        redisFree( mCtx );
        mCtx = NULL;
    }
}

RedisConnectionWrap::~RedisConnectionWrap() {
    if ( redisConnection != NULL && redisClusterClient != NULL ) {
        redisClusterClient->freeConnection( redisConnection );
    }
}

void RedisConnectionWrap::Reset(TRedisConnection* conn, CRedisClusterClient* redis_cluster_client) {
    if ( redisConnection != NULL && redisClusterClient != NULL ) {
        redisClusterClient->freeConnection( redisConnection );
    }
    redisConnection = conn;
    redisClusterClient = redis_cluster_client;
}

bool RedisNodeInfo::Update(const RedisNodeInfo& node) {
    if ( !IsNeedUpdate(node) ) {
        VLOG(DATA) << __FUNCTION__ << ", Not need to do.";
        return true;
    }
    common::WriterMutexLock lock( rwMutex );
    strinfo = node.strinfo;
    isFail = node.isFail;
    isMaster = node.isMaster;
    masterId = node.masterId;
    pingSent = node.pingSent;
    pongRecv = node.pongRecv;
    epoch = node.epoch;
    connected = node.connected;
    matchSlots = node.matchSlots;
    return true;
}

bool RedisNodeInfo::IsNeedUpdate(const RedisNodeInfo& node) {
    common::ReaderMutexLock lock( rwMutex );
    if ( strinfo == node.strinfo ) {
        VLOG(DATA) << __FUNCTION__ << ", Not need, strinfo is same.";
        return false;
    }
    size_t i = 0, j = 0;
    for ( i = 0;i < node.matchSlots.size(); ++i ) {
        for ( j = 0;j < matchSlots.size(); ++j ) {
            if ( node.matchSlots[i] == matchSlots[j] ) {
                break;
            }
        }
        if ( j >= matchSlots.size() ) {
            VLOG(DATA) << __FUNCTION__ << ", matchSlots not same.";
            return true;
        }
    }
    if ( isFail != node.isFail ) {
        VLOG(DATA) << __FUNCTION__ << ", isFail is not same.";
        return true;
    }
    if ( isMaster != node.isMaster  ) {
        VLOG(DATA) << __FUNCTION__ << ", mast slave flag is not same.";
        return true;
    }
    if ( connected != node.connected ) {
        VLOG(DATA) << __FUNCTION__ << ", connected is not same.";
        return true;
    }
    return false;
}

void RedisNodeInfo::SetFail(bool fail) {
    common::WriterMutexLock lock( rwMutex );
    isFail = fail;
}

void RedisNodeInfo::SetMaster(bool is_master) {
    common::WriterMutexLock lock( rwMutex );
    isMaster = is_master;
}

bool RedisNodeInfo::CheckSlot(int slotindex) {
    common::ReaderMutexLock lock( rwMutex );
    if ( isFail || !isMaster ) {
//        VLOG(DEBUG) << __FUNCTION__ << ", strinfo: " << strinfo << ", isFail: " << isFail << ", isMaster: " << isMaster;
        return false;
    }
    std::vector<std::pair<int, int> >::const_iterator  citer = matchSlots.begin();
    for (; citer != matchSlots.end(); ++citer) {
        if ((slotindex >= citer->first) && (slotindex <= citer->second)) {
            return true;
        }
    }
    return false;
}

bool RedisNodeInfo::ParseNodeString(const std::string& node_string) {
    common::WriterMutexLock lock( rwMutex );
    std::string::size_type pos = node_string.find(':');
    if (pos == std::string::npos) {
        VLOG(ERROR) << __FUNCTION__ << ", ip:port format error, " << node_string;
        return false;
    } else {
        const std::string port_str = node_string.substr(pos + 1);
        port = atoi(port_str.c_str());
        ip = node_string.substr(0, pos);
        ipPort = ip + ":" + std::to_string(port);
        return true;
    }
}

void RedisNodeInfo::ParseSlotString(const std::string &SlotString) {
    common::WriterMutexLock lock( rwMutex );
    size_t StartSlot = 0;
    size_t EndSlot = 0;
    std::string::size_type BarPos = SlotString.find('-');
    if (BarPos == std::string::npos) {
        StartSlot = atoi(SlotString.c_str());
        EndSlot = StartSlot;
    } else {
        const std::string EndSlotStr = SlotString.substr(BarPos + 1);
        EndSlot = atoi(EndSlotStr.c_str());
        StartSlot = atoi(SlotString.substr(0, BarPos).c_str());
    }
    matchSlots.push_back(std::make_pair(StartSlot, EndSlot));
}

static const uint16_t crc16tab[256]= {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

CRedisClusterClient* CRedisClusterClient::Instance() {
    if ( s_redis_cluster_client == NULL ) {
        s_redis_cluster_client = new CRedisClusterClient();
    }
    return s_redis_cluster_client;
}

CRedisClusterClient* CRedisClusterClient::s_redis_cluster_client = NULL;

CRedisClusterClient::CRedisClusterClient() :
    _cluster_enabled(false),
    _recently_update_time_sec(0) {
        ;
}

CRedisClusterClient::CRedisClusterClient(const CRedisClusterClient& redis_cluster_client) {
    ;
}
 
CRedisClusterClient::~CRedisClusterClient() {
    release();
}

uint16_t CRedisClusterClient::crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
            crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}

/*
bool CRedisClusterClient::ConnectRedis(const Json::Value& j_config) {
    if ( !_redis_servers.ParseFromJson( j_config ) ) {
        VLOG(ERROR) << __FUNCTION__ << ", ParseFromJson fail, " << j_config;
        return false;
    }
    if ( !ConnectRedis(_redis_servers) ) {
        VLOG(ERROR) << __FUNCTION__ << ", fail..";
        return false;
    }
    return true;
}
*/

bool CRedisClusterClient::ConnectRedis(const TRedisServers& redis_servers) {
    _redis_servers = redis_servers;
    for ( size_t i = 0;i < redis_servers.ipPorts.size(); ++i ) {
        const RedisServers::TIpPort& ip_port = redis_servers.ipPorts[i];
        if ( ConnectRedis(ip_port.host, ip_port.port, redis_servers.options.password,
                    redis_servers.options.timeout, redis_servers.options.poolSize, false) ) {
            return true;
        }
    }
    return false;
}

bool CRedisClusterClient::ConnectRedis(const std::string& host, int port, const std::string& passwd, int timeout,
        int pool_size, bool is_from_out) {
    VLOG(DEBUG) << __FUNCTION__ << ", host: " << host << ", port: " <<  port << ", pool_size: " << pool_size;
    if ( is_from_out ) {
        _redis_servers.ipPorts.push_back( TRedisServers::IpPort(host, port) );
        _redis_servers.options.password = passwd;
        _redis_servers.options.timeout = timeout;
        _redis_servers.options.poolSize = pool_size;
    }
    std::vector<RedisNodeInfo*> nodes;
    if ( !getRedisInfo(host, port, passwd, _cluster_enabled, nodes) ) {
        VLOG(ERROR) << __FUNCTION__ << ", getRedisInfo fali, host: " << host << ", port: " << port << ", passwd: " << passwd;
        return false;
    }
    if ( !_cluster_enabled ) {
        std::vector<RedisNodeInfo*> nodes(1, NULL);
        resizeRedisConns(_redis_connections, nodes);
        connectRedisNode(_redis_connections[0], host, port, passwd, pool_size);
    } else {
        resizeRedisConns( _redis_connections, nodes );
        for ( size_t i = 0;i < _redis_connections.size(); ++i ) {
            if ( _redis_connections[i] != NULL && _redis_connections[i]->redisNodeInfo != NULL ) {
                connectRedisNode(_redis_connections[i], _redis_connections[i]->redisNodeInfo->ip,
                        _redis_connections[i]->redisNodeInfo->port, passwd, pool_size);
            } else {
                VLOG(ERROR) << __FUNCTION__ << ", _redis_connections[i] is NULL.";
                return false;
            }
        }
    }
    return true;
}

bool CRedisClusterClient::ReConnectRedis(TRedisConnection *pConn) {
    release();

    return ConnectRedis(pConn->mHost, pConn->mPort, pConn->mPasswd, pConn->mPoolSize);
}

void CRedisClusterClient::Keepalive() {
    for ( size_t i = 0;i < _redis_connections.size(); ++i ) {
        for ( size_t j = 0;j < REDIS_SECTION_NUM; ++j ) {
            if ( _redis_connections[i] != NULL ) {
                TRedisConnectionList::TConnectionMutex& connection_mutex = _redis_connections[i]->connectionMutexs[j];
                common::MutexLock lock( *connection_mutex.mutex );
                for ( TRedisConnectionIter iter = connection_mutex.redisConnections->begin();
                        iter != connection_mutex.redisConnections->end(); ++iter ) {
                    TRedisConnection* conn = *iter;
                    redisReply* reply = static_cast<redisReply*>( redisCommand(conn->mCtx, "PING") );
                    if (NULL != reply) {
                        freeReplyObject((void*)reply);
                    }
                }
            }
        }
    }
}

bool CRedisClusterClient::RedisCommand(RedisResult& result, const char *format, ...) {
    va_list args;
    va_start(args, format);
    char* key = va_arg(args, char*);
    va_end(args);
    
    if( 0 == strlen(key) ) {
        VLOG(ERROR) << __FUNCTION__ << ", key is NULL.";
        return false;
    }
    RedisConnectionWrap pRedisConn( findNodeConnection(key), this );
    if ( pRedisConn == NULL ) {
        VLOG(ERROR) << __FUNCTION__ << ", pRedisConn is NULL, key: " << key;
        return false;
    }
    for (int failed = 0;failed < REDIS_MAX_RETRY_COUNTER; ++failed) {
        va_start(args, format);
        redisReply *reply = static_cast<redisReply*>(redisvCommand(pRedisConn->mCtx, format, args));
        va_end(args);
        if ( reply == NULL || reply->type == REDIS_REPLY_ERROR ) {
            if ( !dealErrorReply("redisvCommand", key, reply, pRedisConn) ) {
                VLOG(ERROR) << __FUNCTION__ << ", dealErrorReply fail, format: " << format << ", key: " << key;
                FREE_REDIS_REPLY( reply );
                return false;
            }
            FREE_REDIS_REPLY( reply );
        } else {
            result.Init(reply);
            return true;
        }
    }
    VLOG(ERROR) << __FUNCTION__ << ", error, key: " << key;
    return false;
}

bool CRedisClusterClient::RedisCommandArgv(const std::vector<std::string>& vDataIn, RedisResult &result) {
    const std::string &key = vDataIn[1];

    RedisConnectionWrap pRedisConn( findNodeConnection(key), this );
    if (  pRedisConn == NULL ) {
        VLOG(ERROR) << __FUNCTION__ << ", pRedisConn is NULL, key: " << key;
        return false;
    }
    std::vector<const char*> argv(vDataIn.size());
    std::vector<size_t> argvlen(vDataIn.size());
    size_t j = 0;
    for (std::vector<std::string>::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }
    for (int failed = 0;failed < REDIS_MAX_RETRY_COUNTER; ++failed) {
        redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->mCtx, argv.size(), &(argv[0]), &(argvlen[0])));
        if ( reply == NULL || reply->type == REDIS_REPLY_ERROR ) {
            if ( !dealErrorReply("redisCommandArgv", key, reply, pRedisConn) ) {
                VLOG(ERROR) << __FUNCTION__ << ", dealErrorReply fail, key: " << key;
                FREE_REDIS_REPLY( reply );
                return false;
            }
            FREE_REDIS_REPLY( reply );
        } else {
            result.Init(reply);
            return true;
        }
    }
    VLOG(ERROR) << __FUNCTION__ << ", error, key: " << key;
    return false;
}

bool CRedisClusterClient::getRedisInfo(const std::string& host, int port, const std::string& passwd, bool& is_cluster,
        std::vector<RedisNodeInfo*>& nodes) {
    std::auto_ptr<TRedisConnection> redis_conn( new RedisConnection(0, host, port, passwd, 1) );
    if( redis_conn.get() == NULL || !redis_conn->ConnectWithTimeout() ) {
        VLOG(ERROR) << __FUNCTION__ << ", redis_conn Connect fail.";
        return false;
    }
    is_cluster = clusterEnabled( redis_conn->mCtx );
    VLOG(DATA) << __FUNCTION__ << ", clusterEnabled: " << is_cluster;
    if ( is_cluster ) {
        if( !clusterInfo( redis_conn->mCtx ) ) {
            VLOG(ERROR) << __FUNCTION__ << ", clusterInfo fail, host: " << host << ", port: " << port;
            return false;
        }
        getClusterNodes( redis_conn->mCtx, nodes );
    }
    return true;
}

void CRedisClusterClient::release() {
    for (size_t  i = 0;i < _redis_connections.size(); ++i ) {
        if ( _redis_connections[i] != NULL ) {
            for (size_t j = 0;j < REDIS_SECTION_NUM; ++j ) {
                TRedisConnectionList::TConnectionMutex& connection_mutex = _redis_connections[i]->connectionMutexs[j];
                for ( TRedisConnectionIter iter = connection_mutex.redisConnections->begin();
                        iter != connection_mutex.redisConnections->end(); ++iter ) {
                    delete *iter;
                }
            }
            delete _redis_connections[i];
            _redis_connections[i] = NULL;
        }
    }
    _redis_connections.clear();
}

bool CRedisClusterClient::clusterEnabled(redisContext *ctx) {
    redisReply *redis_reply = (redisReply*)redisCommand(ctx, "info");
    CHECK_REPLY( redis_reply );
    char *p = strstr(redis_reply->str, "cluster_enabled:");
    bool bRet = p != NULL && (0 == strncmp(p + strlen("cluster_enabled:"), "1", 1));
    freeReplyObject(redis_reply);

    return bRet;
}

bool CRedisClusterClient::clusterInfo(redisContext *ctx) {
    redisReply *redis_reply = (redisReply*)redisCommand(ctx, "CLUSTER info");
    CHECK_REPLY( redis_reply );
    char *p = strstr(redis_reply->str, ":");
    bool bRet = (0 == strncmp(p+1, "ok", 2));
    freeReplyObject(redis_reply);
    return bRet;
}

bool CRedisClusterClient::getClusterNodes(redisContext *redis_ctx, std::vector<RedisNodeInfo*>& nodes) {
    redisReply *redis_reply = (redisReply*)redisCommand(redis_ctx, "CLUSTER NODES");
    CHECK_REPLY( redis_reply );

    std::vector<std::string> vlines;
    SplitString(redis_reply->str, '\n', vlines);

    for (size_t i = 0; i < vlines.size(); ++i) {
        VLOG(DEBUG) << __FUNCTION__ << ", cluster: " << vlines[i];
        if ( vlines[i].find("master") == std::string::npos ) {
            VLOG(DATA) << __FUNCTION__ << ", not is master, info: " << vlines[i];
            continue;
        }
        std::vector<std::string> nodeinfo;
        SplitString(vlines[i], ' ', nodeinfo);
        if ( nodeinfo.size() < 9 ) {
            VLOG(WARNING) << __FUNCTION__ << ", line format error, line: " << vlines[i];
            continue;
        }
        // TODO 暂时不处理 slave 节点
        if ( nodeinfo[2].find("master") == std::string::npos ) {
            VLOG(DATA) << __FUNCTION__ << ", not is master, info: " << vlines[i];
            continue;
        }
        if ( nodeinfo[2].find("fail") != std::string::npos ) {
            VLOG(DATA) << __FUNCTION__ << ", reids state fail, info: " << vlines[i];
            continue;
        }
        if ( nodeinfo[7] == "disconnected" ) {
            VLOG(DATA) << __FUNCTION__ << ", redis disconnected, info: " << vlines[i];
            continue;
        }
        RedisNodeInfo* node = new RedisNodeInfo();
        node->strinfo = vlines[i];
        node->id = nodeinfo[0];
        node->ParseNodeString(nodeinfo[1]);
        node->flags = nodeinfo[2];
        node->isMaster = true;
        if ( nodeinfo[2].find("fail") != std::string::npos ) {
            node->isFail = true;
        } else {
            node->isFail = false;
        }
        if ( nodeinfo[7] == "connected" ) {
            node->connected = true;
        } else if ( nodeinfo[7] == "disconnected" ) {
            node->connected = false;
        } else {
            VLOG(WARNING) << __FUNCTION__ << ", nodeinfo idx==7 format error, " << nodeinfo[7];
        }
        for (size_t j = 8;j < nodeinfo.size(); ++j ) {
            node->ParseSlotString(nodeinfo[j]);
        }
        nodes.push_back( node );
    }
    freeReplyObject(redis_reply);
    return true;
}

void CRedisClusterClient::resizeRedisConns(std::vector<TRedisConnectionList*>& redis_connections,
        const std::vector<RedisNodeInfo*>& nodes) {
    //// TODO 多了一次 拷贝构造，考虑切换成指针
    redis_connections.resize( nodes.size() );
    for ( size_t i = 0;i < redis_connections.size(); ++i ) {
        redis_connections[i] = new TRedisConnectionList();
        for ( size_t j = 0;j < REDIS_SECTION_NUM; ++j ) {
            redis_connections[i]->connectionMutexs[j].mutex = new common::Mutex();
            redis_connections[i]->connectionMutexs[j].redisConnections = new std::list<RedisConnection*>();
        }
        redis_connections[i]->redisNodeInfo = nodes[i];
    }
}

bool CRedisClusterClient::connectRedisNode(TRedisConnectionList* redis_connects, const std::string& host, int port,
        const std::string& passwd, int pool_size) {
    if ( redis_connects == NULL ) {
        VLOG(ERROR) << __FUNCTION__ << ", redis_connects is NULL.";
        return false;
    }
    //同时打开 CONNECTION_NUM 个连接
    pool_size = pool_size > MAX_REDIS_POOLSIZE ? MAX_REDIS_POOLSIZE : pool_size;
    pool_size = pool_size < REDIS_SECTION_NUM ? REDIS_SECTION_NUM : pool_size;

    for (size_t i = 0; i < pool_size; ++i) {
        TRedisConnectionList::TConnectionMutex& connection_mutex = redis_connects->connectionMutexs[i% REDIS_SECTION_NUM];
        TRedisConnection *pRedisconn = new RedisConnection(&connection_mutex, host, port, passwd, pool_size);
        if( NULL == pRedisconn || !pRedisconn->ConnectWithTimeout() ) {
            VLOG(ERROR) << __FUNCTION__ << ", pRedisconn Connect fail.";
            delete pRedisconn;
            continue;
        }
        connection_mutex.redisConnections->push_back( pRedisconn );
    }

    return true;
}

bool CRedisClusterClient::dealErrorReply(const std::string& func, const std::string& key, redisReply *reply,
        RedisConnectionWrap& redis_conn_wrap) {
    if ( reply == NULL ) {
        VLOG(WARNING) << __FUNCTION__ << ", callFunc: " << func << ", reply is NULL, key: " << key;
        // redis server set timeout != 0
        bool is_need = true;
        if ( !redis_conn_wrap->ReConnectWithTimeout(false, is_need) ) {
            if ( is_need ) {
                updateRedisClientClusterNodesWrap();
                redis_conn_wrap.Reset( findNodeConnection(key), this );
                if ( redis_conn_wrap == NULL ) {
                    VLOG(ERROR) << __FUNCTION__ << ", callFunc: " << func
                        << ", updateRedisClientClusterNodesWrap not userful, key: " << key;
                    return false;
                }
            } else {
                VLOG(ERROR) << __FUNCTION__ << ", callFunc: " << func << ", Connect is not error.";
                return false;
            }
        }
    } else if ( reply->type == REDIS_REPLY_ERROR ) {
        VLOG(WARNING) << __FUNCTION__ << ", callFunc: " << func << ", errorMsg: " << reply->str << ", key: " << key;
        if ( reply->str != NULL && strstr(reply->str, "MOVED") != NULL ) {
            updateRedisClientClusterNodesWrap();
            redis_conn_wrap.Reset( findNodeConnectionByMoved(reply->str), this );
            if ( redis_conn_wrap == NULL ) {
                VLOG(ERROR) << __FUNCTION__ << ", callFunc: " << func << ", MOVED not find client, key: " << key;
                return false;
            }
        } else if ( reply->str != NULL && strstr(reply->str, "ASK") != NULL ) {
            redis_conn_wrap.Reset( findNodeConnectionByMoved(reply->str), this );
            if ( redis_conn_wrap == NULL ) {
                VLOG(ERROR) << __FUNCTION__ << ", callFunc: " << func << ", ASK not find client, key: " << key;
                return false;
            } else {
                redisReply *reply = static_cast<redisReply*>(redisCommand(redis_conn_wrap->mCtx, "ASKING"));
                CHECK_REPLY( reply );
            }
        } else {
            VLOG(ERROR) << __FUNCTION__ << ", Can not deal error, " << reply->str;
            return false;
        }
    }
    return true;
}

bool CRedisClusterClient::updateRedisClientClusterNodesWrap() {
    int64 cur_sec = GetTimeInSecond();
    common::MutexLock lock( _update_mutex );
    if ( cur_sec - _recently_update_time_sec > UPDATE_MIN_INTERVAL_SEC ) {
        updateRedisClientClusterNodes();
        _recently_update_time_sec = GetTimeInSecond();
    } else {
        VLOG(DATA) << __FUNCTION__ << ", Not need updateRedisClientClusterNodes, Just update, cur_sec: "
            << cur_sec << ", recently_sec: " << _recently_update_time_sec;
    }
    return true;
}

bool CRedisClusterClient::updateRedisClientClusterNodes() {
    VLOG(DATA) << __FUNCTION__ << ", start.";
    bool is_get_info = false;
    bool is_cluster = false;
    std::vector<RedisNodeInfo*> nodes;

    for ( size_t i = 0;i < _redis_servers.ipPorts.size(); ++i ) {
        const RedisServers::TIpPort& ip_port = _redis_servers.ipPorts[i];
        if ( getRedisInfo(ip_port.host, ip_port.port, _redis_servers.options.password, is_cluster, nodes) ) {
            is_get_info = true;
            break;
        }
    }
    if ( !is_get_info ) {
        VLOG(ERROR) << __FUNCTION__ << ", Can not get redis info.";
        return false;
    }
    if ( !is_cluster ) {
        VLOG(ERROR) << __FUNCTION__ << ", redis is not cluster.";
        return false;
    }
    do {
        common::ReaderMutexLock lock( _rw_mutex );
        std::vector<bool> updates(_redis_connections.size(), false);
        for (std::vector<RedisNodeInfo*>::iterator iter = nodes.begin();iter != nodes.end(); ) {
            const RedisNodeInfo* node = *iter;
            if ( node == NULL ) {
                VLOG(ERROR) << __FUNCTION__ << ", node is NULL.";
                return false;
            }
            bool is_find = false;
            for (size_t i = 0;i < _redis_connections.size(); ++i ) {
                if ( _redis_connections[i] != NULL && _redis_connections[i]->redisNodeInfo != NULL ) {
                    if ( _redis_connections[i]->redisNodeInfo->ipPort == node->ipPort ) {
                        _redis_connections[i]->redisNodeInfo->Update( *node );
                        iter = nodes.erase( iter );
                        delete node;
                        is_find = true;
                        updates[i] = true;
                        break;
                    }
                }
            }
            if ( !is_find ) {
                ++iter;
            }
        }
        for ( size_t i = 0;i < _redis_connections.size(); ++i ) {
            if ( updates[i] == false && _redis_connections[i] != NULL && _redis_connections[i]->redisNodeInfo != NULL ) {
                VLOG(DATA) << __FUNCTION__ << ", redis not master, strinfo: " << _redis_connections[i]->redisNodeInfo->strinfo;
                _redis_connections[i]->redisNodeInfo->SetFail( true );
                _redis_connections[i]->redisNodeInfo->SetMaster( false );
            }
        }
    } while(0);
    if ( !nodes.empty() ) {
        VLOG(DATA) << __FUNCTION__ << ", New add node, size: " << nodes.size();
        std::vector<TRedisConnectionList*> redis_connections;
        resizeRedisConns( redis_connections, nodes );
        for ( size_t i = 0;i < redis_connections.size(); ++i ) {
            if ( redis_connections[i] != NULL && redis_connections[i]->redisNodeInfo != NULL ) {
                connectRedisNode(redis_connections[i], redis_connections[i]->redisNodeInfo->ip,
                        redis_connections[i]->redisNodeInfo->port, _redis_servers.options.password,
                        _redis_servers.options.poolSize);
            } else {
                VLOG(ERROR) << __FUNCTION__ << ", redis_connections[i] is NULL.";
                return false;
            }
        }
        common::WriterMutexLock lock( _rw_mutex );
        _redis_connections.insert(_redis_connections.begin(), redis_connections.begin(), redis_connections.end());
    }
    return true;
}

TRedisConnection* CRedisClusterClient::findNodeConnection(const std::string& key) {
    if ( !_cluster_enabled ) {
        if ( !_redis_connections.empty() ) {
            return getConnection(_redis_connections[0]);
        } else {
            VLOG(ERROR) << __FUNCTION__ << ", _redis_connections is empty.";
            return NULL;
        }
    }
    int slot = getKeySlotIndex(key);

    for (size_t i = 0;i < _redis_connections.size(); ++i ) {
        if ( _redis_connections[i] != NULL && _redis_connections[i]->redisNodeInfo != NULL &&
                _redis_connections[i]->redisNodeInfo->CheckSlot( slot ) ) {
            return getConnection( _redis_connections[i] );
        }
    }
    VLOG(FATAL) << __FUNCTION__ <<  ", Not get match slot range, key: " << key;
    return NULL;
}

TRedisConnection* CRedisClusterClient::findNodeConnectionByMoved(const std::string& moved) {
    if ( !_cluster_enabled ) {
        VLOG(ERROR) << __FUNCTION__ << ", Not is cluster.";
        return NULL;
    }
    std::vector<std::string> moveds;
    SplitString(moved, ' ', moveds);
    if ( moveds.size() != 3 ) {
        VLOG(ERROR) << __FUNCTION__ << ", MOVED format error, " << moved;
        return NULL;
    }
    for (size_t i = 0;i < _redis_connections.size(); ++i ) {
        if ( _redis_connections[i] != NULL && _redis_connections[i]->redisNodeInfo != NULL &&
                _redis_connections[i]->redisNodeInfo->ipPort == moveds[2] ) {
            return getConnection( _redis_connections[i] );
        }
    }
    VLOG(ERROR) << __FUNCTION__ << ", Not find ipPort, " << moved;
    return NULL;
}

int CRedisClusterClient::getKeySlotIndex(const std::string& key)
{
    if ( !key.empty() ) {
        return keyHashSlot(key.c_str(), key.length());
    }
    return 0;
}

TRedisConnection *CRedisClusterClient::getConnection(TRedisConnectionList* redis_connection_list) {
    if ( redis_connection_list == NULL ) {
        VLOG(ERROR) << __FUNCTION__ << ", redis_connection_list is NULL.";
        return NULL;
    }
    while (true) {
        int idx = rand() % REDIS_SECTION_NUM;
        for ( size_t i = 0;i < REDIS_SECTION_NUM; ++i, ++idx ) {
            do {
                TRedisConnectionList::TConnectionMutex& connect_mutex =
                    redis_connection_list->connectionMutexs[idx% REDIS_SECTION_NUM];
                common::MutexLock lock( *connect_mutex.mutex );
                if ( connect_mutex.redisConnections->size() > 0 ) {
                    TRedisConnection* redis_conn = connect_mutex.redisConnections->front();
                    connect_mutex.redisConnections->pop_front();
                    return redis_conn;
                } else {
                    VLOG(WARNING) << __FUNCTION__ << ", Not available connection.";
                }
            } while(0);
            usleep(1000);
        }
    }
    return NULL;
}

/* Copy from cluster.c
* We have 16384 hash slots. The hash slot of a given key is obtained
* as the least significant 14 bits of the crc16 of the key.
*
* However if the key contains the {...} pattern, only the part between
* { and } is hashed. This may be useful in the future to force certain
* keys to be in the same node (assuming no resharding is in progress). */
int CRedisClusterClient::keyHashSlot(const char *key, size_t keylen) {
    size_t s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key, keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing betweeen {} ? Hash the whole key. */
    if (e == keylen || e == s + 1) return crc16(key, keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
    * what is in the middle between { and }. */
    return crc16(key + s + 1, e - s - 1) & 0x3FFF; // 0x3FFF == 16383
}

void CRedisClusterClient::freeConnection(TRedisConnection *pRedisConn) {
    if ( pRedisConn != NULL && pRedisConn->redisConnectionList != NULL ) {
        common::MutexLock lock( *(pRedisConn->redisConnectionList->mutex) );
        pRedisConn->redisConnectionList->redisConnections->push_back( pRedisConn );
    } else {
        VLOG(FATAL) << __FUNCTION__ << ", Can not free..";
    }
}

}
