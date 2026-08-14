# nb-redis

## 说明

`nb-redis` 是一个独立的 Java 服务，目标是以最小可用方式兼容 `RedisHelper` 当前依赖的 Redis 能力。

当前实现方式：

- 使用 `Netty` 提供 RESP 协议服务
- 使用 JVM 内存作为底层存储
- 支持 `TTL / String / Hash / Set / List / EVAL(有限脚本兼容)`

注意：

- 当前是内存实现，重启后数据会丢失
- 当前没有做主从、持久化、集群分片
- 当前 `Lua` 只兼容 `RedisHelper` 锁脚本和 `LuaScriptLoader#getWorkId()` 使用的脚本模式
- 当前目标是兼容 `RedisHelper` 方法，不是完整 Redis 服务

## 已支持命令

### 连接与基础

- `PING`
- `AUTH`
- `SELECT`
- `CLIENT`
- `COMMAND`
- `QUIT`

### Key / String

- `EXISTS`
- `DEL`
- `EXPIRE`
- `TTL`
- `GET`
- `SET`
- `SETNX`
- `INCRBY`

### Hash

- `HGET`
- `HKEYS`
- `HGETALL`
- `HMSET`
- `HSET`
- `HDEL`
- `HEXISTS`
- `HINCRBYFLOAT`

### Set

- `SMEMBERS`
- `SISMEMBER`
- `SADD`
- `SCARD`
- `SREM`

### List

- `LRANGE`
- `LLEN`
- `LINDEX`
- `RPUSH`
- `LTRIM`
- `LSET`
- `LREM`
- `LPOP`
- `RPOP`

### Lua

- `EVAL`
  - 兼容 `lockLua` 加锁脚本
  - 兼容 `unlockLua` 解锁脚本
  - 兼容 `LuaScriptLoader#getWorkId()` 的 workId 脚本

## 配置

配置文件：`src/main/resources/application.yml`

关键配置：

```yaml
nbchat:
  redis-server:
    host: 0.0.0.0
    port: 6379
    boss-threads: 1
    worker-threads: 0
    cleanup-interval-seconds: 5
```

## 启动

```bash
mvn spring-boot:run
```

或打包后：

```bash
java -jar nb-redis-service/target/nb-redis-service-2.0.2.jar
```

## 适用范围

适合以下场景：

- 内网去 Redis 化验证
- 单机替代测试
- `RedisHelper` 调用兼容验证

不建议直接用于以下场景：

- 高并发生产缓存
- 多实例分布式锁强一致场景
- 依赖完整 Redis Lua / 事务 / 发布订阅 / 有序集合 的场景
