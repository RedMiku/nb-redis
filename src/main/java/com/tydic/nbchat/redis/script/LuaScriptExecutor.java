package com.tydic.nbchat.redis.script;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import com.tydic.nbchat.redis.store.InMemoryRedisStore;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LuaScriptExecutor {

    private static final String LOCK_SCRIPT_MARK = "redis.call('setnx',keys[1],argv[1])";
    private static final String SIMPLE_UNLOCK_SCRIPT_MARK = "redis.call('get', keys[1]) == argv[1]";
    private static final String SIMPLE_EXTEND_SCRIPT_MARK = "redis.call('expire', keys[1], argv[2])";
    private static final String WORK_ID_SCRIPT_MARK = "local isexist = redis.call('exists', keys[1])";
    private static final String REDIS_LOCK_UNLOCK_MARK = "redis.call(\"get\", keys[1]) ~= argv[1]";
    private static final String REDIS_LOCK_SIGNAL_MARK = "redis.call(\"lpush\", keys[2], 1)";
    private static final String REDIS_LOCK_EXTEND_MARK = "redis.call(\"ttl\", keys[1]) < 0";
    private static final String REDIS_LOCK_RESET_MARK = "return redis.call('del', keys[1])";

    private static final String REDIS_LOCK_UNLOCK_SCRIPT =
            "if redis.call(\"get\", KEYS[1]) ~= ARGV[1] then\n" +
            "    return 1\n" +
            "else\n" +
            "    redis.call(\"del\", KEYS[2])\n" +
            "    redis.call(\"lpush\", KEYS[2], 1)\n" +
            "    redis.call(\"pexpire\", KEYS[2], ARGV[2])\n" +
            "    redis.call(\"del\", KEYS[1])\n" +
            "    return 0\n" +
            "end";

    private static final String REDIS_LOCK_EXTEND_SCRIPT =
            "if redis.call(\"get\", KEYS[1]) ~= ARGV[1] then\n" +
            "    return 1\n" +
            "elseif redis.call(\"ttl\", KEYS[1]) < 0 then\n" +
            "    return 2\n" +
            "else\n" +
            "    redis.call(\"expire\", KEYS[1], ARGV[2])\n" +
            "    return 0\n" +
            "end";

    private static final String REDIS_LOCK_RESET_SCRIPT =
            "redis.call('del', KEYS[2])\n" +
            "redis.call('lpush', KEYS[2], 1)\n" +
            "redis.call('pexpire', KEYS[2], ARGV[2])\n" +
            "return redis.call('del', KEYS[1])";

    private final InMemoryRedisStore store;
    private final Map<String, String> scriptCache = new ConcurrentHashMap<>();

    public LuaScriptExecutor(InMemoryRedisStore store) {
        this.store = store;
        preloadSupportedScripts();
    }

    public Object execute(String script, List<String> keys, List<byte[]> args) {
        registerScript(script);
        String normalized = normalizeScript(script);
        if (normalized.contains(LOCK_SCRIPT_MARK)) {
            return executeLockScript(keys, args);
        }
        if (normalized.contains(SIMPLE_UNLOCK_SCRIPT_MARK) && normalized.contains("redis.call('del', keys[1])")) {
            return executeUnlockScript(keys, args);
        }
        if (normalized.contains(SIMPLE_UNLOCK_SCRIPT_MARK) && normalized.contains(SIMPLE_EXTEND_SCRIPT_MARK)) {
            return executeExtendScript(keys, args);
        }
        if (normalized.contains(REDIS_LOCK_UNLOCK_MARK) && normalized.contains(REDIS_LOCK_SIGNAL_MARK)) {
            return executeRedisLockUnlockScript(keys, args);
        }
        if (normalized.contains(REDIS_LOCK_UNLOCK_MARK) && normalized.contains(REDIS_LOCK_EXTEND_MARK)) {
            return executeRedisLockExtendScript(keys, args);
        }
        if (normalized.contains(REDIS_LOCK_SIGNAL_MARK) && normalized.contains(REDIS_LOCK_RESET_MARK)) {
            return executeRedisLockResetScript(keys, args);
        }
        if (normalized.contains(WORK_ID_SCRIPT_MARK) && normalized.contains("(workerId + 1) % 32")) {
            return executeWorkIdScript(keys);
        }
        throw new UnsupportedOperationException("暂不支持该Lua脚本");
    }

    public Object executeSha(String sha1, List<String> keys, List<byte[]> args) {
        String script = scriptCache.get(sha1);
        if (script == null) {
            script = resolveKnownScriptBySignature(keys, args);
        }
        if (script == null) {
            throw new IllegalArgumentException("NOSCRIPT No matching script. Please use EVAL.");
        }
        return execute(script, keys, args);
    }

    public String registerScript(String script) {
        String raw = script == null ? "" : script;
        String sha1 = sha1Hex(raw);
        scriptCache.put(sha1, raw);
        cacheScriptAliases(raw);
        return sha1;
    }

    public List<Long> scriptExists(List<String> sha1List) {
        List<Long> result = new ArrayList<>(sha1List.size());
        for (String sha1 : sha1List) {
            result.add(scriptCache.containsKey(sha1) ? 1L : 0L);
        }
        return result;
    }

    public void flushScripts() {
        scriptCache.clear();
        preloadSupportedScripts();
    }

    private void preloadSupportedScripts() {
        registerScript(REDIS_LOCK_UNLOCK_SCRIPT);
        registerScript(REDIS_LOCK_EXTEND_SCRIPT);
        registerScript(REDIS_LOCK_RESET_SCRIPT);
    }

    private String resolveKnownScriptBySignature(List<String> keys, List<byte[]> args) {
        if (keys == null || keys.isEmpty()) {
            return null;
        }
        String firstKey = keys.get(0);
        if (firstKey != null && firstKey.startsWith("{lock}:")) {
            if (keys.size() >= 2 && args.size() >= 2) {
                return REDIS_LOCK_UNLOCK_SCRIPT;
            }
            if (keys.size() == 1 && args.size() >= 2) {
                return REDIS_LOCK_EXTEND_SCRIPT;
            }
            if (keys.size() >= 2 && args.size() == 1) {
                return REDIS_LOCK_RESET_SCRIPT;
            }
        }
        return null;
    }

    private void cacheScriptAliases(String script) {
        String withoutCr = script.replace("\r", "");
        scriptCache.putIfAbsent(sha1Hex(withoutCr), withoutCr);

        String trimmed = withoutCr.trim();
        scriptCache.putIfAbsent(sha1Hex(trimmed), trimmed);

        String normalized = normalizeScript(script);
        scriptCache.putIfAbsent(sha1Hex(normalized), normalized);
    }

    private String normalizeScript(String script) {
        return (script == null ? "" : script)
                .replace("\r", "")
                .trim()
                .toLowerCase();
    }

    private String sha1Hex(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1算法不可用", e);
        }
    }

    private Long executeLockScript(List<String> keys, List<byte[]> args) {
        if (keys.size() != 1 || args.size() < 2) {
            return 0L;
        }
        byte[] requestId = args.get(0);
        long expireTime = Long.parseLong(new String(args.get(1), StandardCharsets.UTF_8));
        boolean locked = store.setNx(keys.get(0), requestId, expireTime);
        if (!locked) {
            return 0L;
        }
        byte[] current = store.get(keys.get(0));
        if (current == null || !java.util.Arrays.equals(current, requestId)) {
            return 0L;
        }
        return store.expire(keys.get(0), expireTime) ? 1L : 0L;
    }

    private Long executeUnlockScript(List<String> keys, List<byte[]> args) {
        if (keys.isEmpty() || args.isEmpty()) {
            return 0L;
        }
        byte[] current = store.get(keys.get(0));
        if (current == null || !java.util.Arrays.equals(current, args.get(0))) {
            return 0L;
        }
        long removed = store.del(Collections.singletonList(keys.get(0)));
        if (removed <= 0) {
            return 0L;
        }
        if (keys.size() > 1) {
            store.rpush(keys.get(1), Collections.singletonList("1".getBytes(StandardCharsets.UTF_8)), null);
        }
        return 1L;
    }

    private Long executeExtendScript(List<String> keys, List<byte[]> args) {
        if (keys.size() != 1 || args.size() < 2) {
            return 0L;
        }
        byte[] current = store.get(keys.get(0));
        if (current == null || !java.util.Arrays.equals(current, args.get(0))) {
            return 0L;
        }
        long expireTime = Long.parseLong(new String(args.get(1), StandardCharsets.UTF_8));
        return store.expire(keys.get(0), expireTime) ? 1L : 0L;
    }

    private Long executeRedisLockUnlockScript(List<String> keys, List<byte[]> args) {
        if (keys.size() < 2 || args.size() < 2) {
            return 1L;
        }
        byte[] current = store.get(keys.get(0));
        if (current == null || !java.util.Arrays.equals(current, args.get(0))) {
            return 1L;
        }
        store.del(Collections.singletonList(keys.get(1)));
        store.lpush(keys.get(1), Collections.singletonList("1".getBytes(StandardCharsets.UTF_8)), null);
        long signalExpireMillis = Long.parseLong(new String(args.get(1), StandardCharsets.UTF_8));
        store.pexpire(keys.get(1), signalExpireMillis);
        store.del(Collections.singletonList(keys.get(0)));
        return 0L;
    }

    private Long executeRedisLockExtendScript(List<String> keys, List<byte[]> args) {
        if (keys.size() != 1 || args.size() < 2) {
            return 1L;
        }
        byte[] current = store.get(keys.get(0));
        if (current == null || !java.util.Arrays.equals(current, args.get(0))) {
            return 1L;
        }
        if (store.ttl(keys.get(0)) < 0) {
            return 2L;
        }
        long expireTime = Long.parseLong(new String(args.get(1), StandardCharsets.UTF_8));
        return store.expire(keys.get(0), expireTime) ? 0L : 2L;
    }

    private Long executeRedisLockResetScript(List<String> keys, List<byte[]> args) {
        if (keys.size() < 2 || args.isEmpty()) {
            return 0L;
        }
        store.del(Collections.singletonList(keys.get(1)));
        store.lpush(keys.get(1), Collections.singletonList("1".getBytes(StandardCharsets.UTF_8)), null);
        long signalExpireMillis = Long.parseLong(new String(args.get(0), StandardCharsets.UTF_8));
        store.pexpire(keys.get(1), signalExpireMillis);
        return store.del(Collections.singletonList(keys.get(0)));
    }

    private String executeWorkIdScript(List<String> keys) {
        if (keys.size() != 1) {
            return "0";
        }
        String key = keys.get(0);
        byte[] current = store.get(key);
        long next = 0L;
        if (current != null) {
            long workerId = Long.parseLong(new String(current, StandardCharsets.UTF_8));
            next = (workerId + 1) % 32;
        }
        store.set(key, String.valueOf(next).getBytes(StandardCharsets.UTF_8), null);
        return String.valueOf(next);
    }
}
// [AI:END]
