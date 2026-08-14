package com.tydic.nbchat.redis.server;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import com.tydic.nbchat.redis.config.RedisServerProperties;
import com.tydic.nbchat.redis.protocol.RespCommand;
import com.tydic.nbchat.redis.protocol.RespResponseWriter;
import com.tydic.nbchat.redis.script.LuaScriptExecutor;
import com.tydic.nbchat.redis.store.InMemoryRedisStore;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@ChannelHandler.Sharable
public class RedisCommandHandler extends SimpleChannelInboundHandler<RespCommand> {

    private static final Logger log = LoggerFactory.getLogger(RedisCommandHandler.class);
    private static final AttributeKey<List<RespCommand>> MULTI_QUEUE =
            AttributeKey.valueOf("nbchat.redis.multi.queue");

    private final InMemoryRedisStore store;

    private final LuaScriptExecutor luaScriptExecutor;

    private final RedisServerProperties properties;

    public RedisCommandHandler(InMemoryRedisStore store, LuaScriptExecutor luaScriptExecutor,
                               RedisServerProperties properties) {
        this.store = store;
        this.luaScriptExecutor = luaScriptExecutor;
        this.properties = properties;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (properties.isConnectionLogEnabled()) {
            log.info("RESP连接建立 channelId={} remote={}", ctx.channel().id().asShortText(), remoteAddress(ctx));
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (properties.isConnectionLogEnabled()) {
            log.info("RESP连接关闭 channelId={} remote={}", ctx.channel().id().asShortText(), remoteAddress(ctx));
        }
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RespCommand command) {
        try {
            if (properties.isCommandLogEnabled()) {
                log.info("收到命令 channelId={} remote={} cmd={} args={}",
                        ctx.channel().id().asShortText(),
                        remoteAddress(ctx),
                        command.getName(),
                        summarizeArgs(command.getArguments()));
            }
            ByteBuf response;
            if (shouldQueue(ctx, command)) {
                queueCommand(ctx, command);
                response = RespResponseWriter.simpleString("QUEUED");
            } else {
                response = handle(ctx, command);
            }
            if ("QUIT".equals(command.getName())) {
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                return;
            }
            ctx.writeAndFlush(response);
        } catch (IllegalStateException e) {
            ctx.writeAndFlush(RespResponseWriter.error("WRONGTYPE " + e.getMessage()));
        } catch (Exception e) {
            log.error("处理命令失败: {}", command.getName(), e);
            ctx.writeAndFlush(RespResponseWriter.error("ERR " + e.getMessage()));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("RESP连接异常 channelId={} remote={}", ctx.channel().id().asShortText(), remoteAddress(ctx), cause);
        ctx.close();
    }

    private ByteBuf handle(ChannelHandlerContext ctx, RespCommand command) {
        List<byte[]> args = command.getArguments();
        switch (command.getName()) {
            case "PING":
                return args.isEmpty() ? RespResponseWriter.simpleString("PONG") : RespResponseWriter.bulk(args.get(0));
            case "AUTH":
            case "SELECT":
            case "CLIENT":
            case "WATCH":
                return RespResponseWriter.simpleString("OK");
            case "INFO":
                return handleInfo(args);
            case "COMMAND":
                return RespResponseWriter.array(new ArrayList<>());
            case "SCRIPT":
                return handleScript(args);
            case "MULTI":
                ctx.channel().attr(MULTI_QUEUE).set(new ArrayList<>());
                return RespResponseWriter.simpleString("OK");
            case "EXEC":
                return handleExec(ctx);
            case "QUIT":
                return RespResponseWriter.simpleString("OK");
            case "EXISTS":
                return handleExists(args);
            case "DEL":
                return handleDel(args);
            case "EXPIRE":
                return handleExpire(args);
            case "TTL":
                return RespResponseWriter.integer(store.ttl(key(args, 0)));
            case "GET":
                return RespResponseWriter.bulk(store.get(key(args, 0)));
            case "SET":
                return handleSet(args);
            case "SETEX":
                return handleSetex(args);
            case "PSETEX":
                return handlePsetex(args);
            case "SETNX":
                return RespResponseWriter.integer(store.setNx(key(args, 0), arg(args, 1), null) ? 1 : 0);
            case "INCRBY":
                return RespResponseWriter.integer(store.incrBy(key(args, 0), parseLong(arg(args, 1))));
            case "DECRBY":
                return RespResponseWriter.integer(store.decrBy(key(args, 0), parseLong(arg(args, 1))));
            case "HGET":
                return RespResponseWriter.bulk(store.hget(key(args, 0), arg(args, 1)));
            case "HKEYS":
                return RespResponseWriter.array(new ArrayList<>(store.hkeys(key(args, 0))));
            case "HGETALL":
                return RespResponseWriter.arrayFromMap(store.hgetAll(key(args, 0)));
            case "HMSET":
                return handleHmset(args);
            case "HSET":
                return handleHset(args);
            case "HSETNX":
                return RespResponseWriter.integer(store.hsetnx(key(args, 0), arg(args, 1), arg(args, 2), null));
            case "HDEL":
                return handleHdel(args);
            case "HEXISTS":
                return RespResponseWriter.integer(store.hexists(key(args, 0), arg(args, 1)) ? 1 : 0);
            case "HINCRBYFLOAT":
                return RespResponseWriter.bulk(String.valueOf(store.hincrByFloat(key(args, 0), arg(args, 1), parseDouble(arg(args, 2)))).getBytes(StandardCharsets.UTF_8));
            case "HINCRBY":
                return RespResponseWriter.integer(store.hincrBy(key(args, 0), arg(args, 1), parseLong(arg(args, 2))));
            case "SMEMBERS":
                return RespResponseWriter.array(new ArrayList<>(store.smembers(key(args, 0))));
            case "SISMEMBER":
                return RespResponseWriter.integer(store.sismember(key(args, 0), arg(args, 1)) ? 1 : 0);
            case "SADD":
                return RespResponseWriter.integer(store.sadd(key(args, 0), tail(args, 1), null));
            case "SCARD":
                return RespResponseWriter.integer(store.scard(key(args, 0)));
            case "SREM":
                return RespResponseWriter.integer(store.srem(key(args, 0), tail(args, 1)));
            case "LRANGE":
                return RespResponseWriter.array(store.lrange(key(args, 0), parseLong(arg(args, 1)), parseLong(arg(args, 2))));
            case "LLEN":
                return RespResponseWriter.integer(store.llen(key(args, 0)));
            case "LINDEX":
                return RespResponseWriter.bulk(store.lindex(key(args, 0), parseLong(arg(args, 1))));
            case "RPUSH":
                return RespResponseWriter.integer(store.rpush(key(args, 0), tail(args, 1), null));
            case "LPUSH":
                return RespResponseWriter.integer(store.lpush(key(args, 0), tail(args, 1), null));
            case "LTRIM":
                return RespResponseWriter.simpleString(store.ltrim(key(args, 0), parseLong(arg(args, 1)), parseLong(arg(args, 2))) ? "OK" : "OK");
            case "LSET":
                return handleLset(args);
            case "LREM":
                return RespResponseWriter.integer(store.lrem(key(args, 0), parseLong(arg(args, 1)), arg(args, 2)));
            case "LPOP":
                return RespResponseWriter.bulk(store.lpop(key(args, 0)));
            case "RPOP":
                return RespResponseWriter.bulk(store.rpop(key(args, 0)));
            case "BLPOP":
                return handleBlpop(args);
            case "BRPOP":
                return handleBrpop(args);
            case "ZADD":
                return handleZadd(args);
            case "ZREM":
                return handleZrem(args);
            case "ZCARD":
                return RespResponseWriter.integer(store.zcard(key(args, 0)));
            case "ZREMRANGEBYSCORE":
                return handleZremrangeByScore(args);
            case "EVAL":
                return handleEval(args);
            case "EVALSHA":
                return handleEvalSha(args);
            default:
                return RespResponseWriter.error("ERR unsupported command '" + command.getName() + "'");
        }
    }

    private boolean shouldQueue(ChannelHandlerContext ctx, RespCommand command) {
        List<RespCommand> queued = ctx.channel().attr(MULTI_QUEUE).get();
        if (queued == null) {
            return false;
        }
        String name = command.getName();
        return !"EXEC".equals(name) && !"MULTI".equals(name) && !"WATCH".equals(name) && !"QUIT".equals(name);
    }

    private void queueCommand(ChannelHandlerContext ctx, RespCommand command) {
        List<RespCommand> queued = ctx.channel().attr(MULTI_QUEUE).get();
        if (queued != null) {
            queued.add(command);
        }
    }

    private ByteBuf handleExec(ChannelHandlerContext ctx) {
        List<RespCommand> queued = ctx.channel().attr(MULTI_QUEUE).get();
        if (queued == null) {
            return RespResponseWriter.arrayOfBuffers(new ArrayList<>());
        }
        ctx.channel().attr(MULTI_QUEUE).set(null);
        List<ByteBuf> responses = new ArrayList<>();
        for (RespCommand item : queued) {
            responses.add(handle(ctx, item));
        }
        return RespResponseWriter.arrayOfBuffers(responses);
    }

    private ByteBuf handleExists(List<byte[]> args) {
        long count = 0L;
        for (byte[] arg : args) {
            if (store.exists(asString(arg))) {
                count++;
            }
        }
        return RespResponseWriter.integer(count);
    }

    private ByteBuf handleInfo(List<byte[]> args) {
        String section = args.isEmpty() ? "default" : asString(args.get(0)).toLowerCase();
        StringBuilder info = new StringBuilder();
        if ("default".equals(section) || "server".equals(section) || "all".equals(section)) {
            info.append("# Server\r\n")
                .append("redis_version:7.0.0\r\n")
                .append("redis_mode:standalone\r\n")
                .append("os:Linux\r\n")
                .append("arch_bits:64\r\n")
                .append("process_id:1\r\n");
        }
        if ("default".equals(section) || "clients".equals(section) || "all".equals(section)) {
            info.append("# Clients\r\n")
                .append("connected_clients:1\r\n");
        }
        if ("default".equals(section) || "memory".equals(section) || "all".equals(section)) {
            info.append("# Memory\r\n")
                .append("used_memory:0\r\n")
                .append("used_memory_human:0B\r\n");
        }
        if ("default".equals(section) || "stats".equals(section) || "all".equals(section)) {
            info.append("# Stats\r\n")
                .append("total_connections_received:1\r\n")
                .append("total_commands_processed:1\r\n");
        }
        if ("default".equals(section) || "replication".equals(section) || "all".equals(section)) {
            info.append("# Replication\r\n")
                .append("role:master\r\n")
                .append("connected_slaves:0\r\n");
        }
        if (info.length() == 0) {
            info.append("# ").append(section).append("\r\n");
        }
        return RespResponseWriter.bulk(info.toString().getBytes(StandardCharsets.UTF_8));
    }

    private ByteBuf handleScript(List<byte[]> args) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("SCRIPT参数不足");
        }
        String subCommand = asString(args.get(0)).toUpperCase();
        switch (subCommand) {
            case "LOAD":
                return RespResponseWriter.bulk(luaScriptExecutor.registerScript(asString(arg(args, 1)))
                        .getBytes(StandardCharsets.UTF_8));
            case "EXISTS":
                List<String> sha1List = new ArrayList<>();
                for (int i = 1; i < args.size(); i++) {
                    sha1List.add(asString(args.get(i)));
                }
                return RespResponseWriter.integerArray(luaScriptExecutor.scriptExists(sha1List));
            case "FLUSH":
                luaScriptExecutor.flushScripts();
                return RespResponseWriter.simpleString("OK");
            default:
                return RespResponseWriter.error("ERR unsupported script subcommand '" + subCommand + "'");
        }
    }

    private ByteBuf handleDel(List<byte[]> args) {
        List<String> keys = new ArrayList<>();
        for (byte[] arg : args) {
            keys.add(asString(arg));
        }
        return RespResponseWriter.integer(store.del(keys));
    }

    private ByteBuf handleExpire(List<byte[]> args) {
        boolean success = store.expire(key(args, 0), parseLong(arg(args, 1)));
        return RespResponseWriter.integer(success ? 1 : 0);
    }

    private ByteBuf handleSet(List<byte[]> args) {
        String key = key(args, 0);
        byte[] value = arg(args, 1);
        Long ttlSeconds = null;
        if (args.size() > 2) {
            for (int i = 2; i < args.size(); i++) {
                String option = asString(args.get(i)).toUpperCase();
                if ("EX".equals(option) && i + 1 < args.size()) {
                    ttlSeconds = parseLong(args.get(++i));
                } else if ("PX".equals(option) && i + 1 < args.size()) {
                    long millis = parseLong(args.get(++i));
                    ttlSeconds = Math.max(1L, (millis + 999L) / 1000L);
                } else if ("NX".equals(option)) {
                    boolean success = store.setNx(key, value, ttlSeconds);
                    return success ? RespResponseWriter.simpleString("OK") : RespResponseWriter.bulk((byte[]) null);
                }
            }
        }
        store.set(key, value, ttlSeconds);
        return RespResponseWriter.simpleString("OK");
    }

    private ByteBuf handleHmset(List<byte[]> args) {
        Map<byte[], byte[]> values = new LinkedHashMap<>();
        for (int i = 1; i < args.size(); i += 2) {
            values.put(arg(args, i), arg(args, i + 1));
        }
        store.hmset(key(args, 0), values, null);
        return RespResponseWriter.simpleString("OK");
    }

    private ByteBuf handleSetex(List<byte[]> args) {
        String key = key(args, 0);
        long ttlSeconds = parseLong(arg(args, 1));
        byte[] value = arg(args, 2);
        store.set(key, value, ttlSeconds);
        return RespResponseWriter.simpleString("OK");
    }

    private ByteBuf handlePsetex(List<byte[]> args) {
        String key = key(args, 0);
        long ttlMillis = parseLong(arg(args, 1));
        long ttlSeconds = Math.max(1L, (ttlMillis + 999L) / 1000L);
        byte[] value = arg(args, 2);
        store.set(key, value, ttlSeconds);
        return RespResponseWriter.simpleString("OK");
    }

    private ByteBuf handleHset(List<byte[]> args) {
        if ((args.size() - 1) % 2 != 0) {
            throw new IllegalArgumentException("HSET参数数量不正确");
        }
        long added = 0L;
        for (int i = 1; i < args.size(); i += 2) {
            added += store.hset(key(args, 0), arg(args, i), arg(args, i + 1), null);
        }
        return RespResponseWriter.integer(added);
    }

    private ByteBuf handleHdel(List<byte[]> args) {
        return RespResponseWriter.integer(store.hdel(key(args, 0), tail(args, 1)));
    }

    private ByteBuf handleLset(List<byte[]> args) {
        boolean success = store.lset(key(args, 0), parseLong(arg(args, 1)), arg(args, 2));
        if (!success) {
            return RespResponseWriter.error("ERR index out of range");
        }
        return RespResponseWriter.simpleString("OK");
    }

    private ByteBuf handleBlpop(List<byte[]> args) {
        if (args.size() < 2) {
            throw new IllegalArgumentException("BLPOP参数数量不正确");
        }
        long timeoutSeconds = parseLong(args.get(args.size() - 1));
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < args.size() - 1; i++) {
            keys.add(asString(args.get(i)));
        }

        long deadline = timeoutSeconds <= 0
                ? Long.MAX_VALUE
                : System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() <= deadline) {
            for (String key : keys) {
                byte[] value = store.lpop(key);
                if (value != null) {
                    List<byte[]> result = new ArrayList<>(2);
                    result.add(key.getBytes(StandardCharsets.UTF_8));
                    result.add(value);
                    return RespResponseWriter.array(result);
                }
            }
            if (timeoutSeconds == 0) {
                sleepQuietly(100L);
                continue;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            sleepQuietly(Math.min(100L, remaining));
        }
        return RespResponseWriter.bulk((byte[]) null);
    }

    private ByteBuf handleBrpop(List<byte[]> args) {
        if (args.size() < 2) {
            throw new IllegalArgumentException("BRPOP参数数量不正确");
        }
        long timeoutSeconds = parseLong(args.get(args.size() - 1));
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < args.size() - 1; i++) {
            keys.add(asString(args.get(i)));
        }

        long deadline = timeoutSeconds <= 0
                ? Long.MAX_VALUE
                : System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() <= deadline) {
            for (String key : keys) {
                byte[] value = store.rpop(key);
                if (value != null) {
                    List<byte[]> result = new ArrayList<>(2);
                    result.add(key.getBytes(StandardCharsets.UTF_8));
                    result.add(value);
                    return RespResponseWriter.array(result);
                }
            }
            if (timeoutSeconds == 0) {
                sleepQuietly(100L);
                continue;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            sleepQuietly(Math.min(100L, remaining));
        }
        return RespResponseWriter.bulk((byte[]) null);
    }

    private ByteBuf handleZadd(List<byte[]> args) {
        if ((args.size() - 1) < 2 || (args.size() - 1) % 2 != 0) {
            throw new IllegalArgumentException("ZADD参数数量不正确");
        }
        List<InMemoryRedisStore.ZSetMember> members = new ArrayList<>();
        for (int i = 1; i < args.size(); i += 2) {
            double score = parseDouble(args.get(i));
            byte[] member = args.get(i + 1);
            members.add(new InMemoryRedisStore.ZSetMember(score, member));
        }
        return RespResponseWriter.integer(store.zadd(key(args, 0), members, null));
    }

    private ByteBuf handleZrem(List<byte[]> args) {
        if (args.size() < 2) {
            throw new IllegalArgumentException("ZREM参数数量不正确");
        }
        return RespResponseWriter.integer(store.zrem(key(args, 0), tail(args, 1)));
    }

    private ByteBuf handleZremrangeByScore(List<byte[]> args) {
        if (args.size() != 3) {
            throw new IllegalArgumentException("ZREMRANGEBYSCORE参数数量不正确");
        }
        return RespResponseWriter.integer(
                store.zremrangeByScore(key(args, 0), parseDouble(arg(args, 1)), parseDouble(arg(args, 2)))
        );
    }

    private ByteBuf handleEval(List<byte[]> args) {
        String script = asString(arg(args, 0));
        int numKeys = (int) parseLong(arg(args, 1));
        List<String> keys = new ArrayList<>();
        List<byte[]> scriptArgs = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            keys.add(asString(arg(args, 2 + i)));
        }
        for (int i = 2 + numKeys; i < args.size(); i++) {
            scriptArgs.add(args.get(i));
        }
        Object result = luaScriptExecutor.execute(script, keys, scriptArgs);
        if (result instanceof Long) {
            return RespResponseWriter.integer((Long) result);
        }
        if (result instanceof String) {
            return RespResponseWriter.bulk(((String) result).getBytes(StandardCharsets.UTF_8));
        }
        if (result instanceof byte[]) {
            return RespResponseWriter.bulk((byte[]) result);
        }
        return RespResponseWriter.bulk((byte[]) null);
    }

    private ByteBuf handleEvalSha(List<byte[]> args) {
        String sha1 = asString(arg(args, 0));
        int numKeys = (int) parseLong(arg(args, 1));
        List<String> keys = new ArrayList<>();
        List<byte[]> scriptArgs = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            keys.add(asString(arg(args, 2 + i)));
        }
        for (int i = 2 + numKeys; i < args.size(); i++) {
            scriptArgs.add(args.get(i));
        }
        Object result = luaScriptExecutor.executeSha(sha1, keys, scriptArgs);
        if (result instanceof Long) {
            return RespResponseWriter.integer((Long) result);
        }
        if (result instanceof String) {
            return RespResponseWriter.bulk(((String) result).getBytes(StandardCharsets.UTF_8));
        }
        if (result instanceof byte[]) {
            return RespResponseWriter.bulk((byte[]) result);
        }
        return RespResponseWriter.bulk((byte[]) null);
    }

    private String key(List<byte[]> args, int index) {
        return asString(arg(args, index));
    }

    private byte[] arg(List<byte[]> args, int index) {
        if (index >= args.size()) {
            throw new IllegalArgumentException("命令参数不足");
        }
        return args.get(index);
    }

    private List<byte[]> tail(List<byte[]> args, int start) {
        List<byte[]> result = new ArrayList<>();
        for (int i = start; i < args.size(); i++) {
            result.add(args.get(i));
        }
        return result;
    }

    private long parseLong(byte[] bytes) {
        return Long.parseLong(asString(bytes));
    }

    private double parseDouble(byte[] bytes) {
        return Double.parseDouble(asString(bytes));
    }

    private String asString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String summarizeArgs(List<byte[]> args) {
        if (args == null || args.isEmpty()) {
            return "[]";
        }
        List<String> parts = new ArrayList<>(args.size());
        for (byte[] arg : args) {
            parts.add(summarizeValue(arg));
        }
        return parts.toString();
    }

    private String summarizeValue(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        String text = new String(bytes, StandardCharsets.UTF_8)
                .replace("\r", "\\r")
                .replace("\n", "\\n");
        int maxLen = Math.max(16, properties.getMaxLoggedValueLength());
        if (text.length() > maxLen) {
            text = text.substring(0, maxLen) + "...";
        }
        return "\"" + text + "\"(len=" + bytes.length + ")";
    }

    private String remoteAddress(ChannelHandlerContext ctx) {
        return String.valueOf(ctx.channel().remoteAddress());
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
// [AI:END]
