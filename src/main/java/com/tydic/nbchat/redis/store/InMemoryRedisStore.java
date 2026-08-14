package com.tydic.nbchat.redis.store;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import com.tydic.nbchat.redis.config.RedisServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Component
public class InMemoryRedisStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryRedisStore.class);

    private final ConcurrentHashMap<String, RedisEntry> store = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleanupExecutor;

    private final RedisServerProperties properties;

    public InMemoryRedisStore(RedisServerProperties properties) {
        this.properties = properties;
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "nbchat-redis-cleaner");
            thread.setDaemon(true);
            return thread;
        };
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(factory);
        this.cleanupExecutor.scheduleAtFixedRate(
                this::cleanupExpiredKeys,
                properties.getCleanupIntervalSeconds(),
                properties.getCleanupIntervalSeconds(),
                TimeUnit.SECONDS
        );
    }

    public boolean expire(String key, long seconds) {
        if (seconds <= 0) {
            logData("expire", key, "ttl=" + seconds + ", success=false");
            return false;
        }
        RedisEntry entry = getLiveEntry(key);
        if (entry == null) {
            logData("expire", key, "ttl=" + seconds + ", success=false, reason=missing");
            return false;
        }
        entry.setExpireAtMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(seconds));
        logData("expire", key, "ttl=" + seconds + ", success=true");
        return true;
    }

    public boolean pexpire(String key, long millis) {
        if (millis <= 0) {
            logData("pexpire", key, "ttlMillis=" + millis + ", success=false");
            return false;
        }
        RedisEntry entry = getLiveEntry(key);
        if (entry == null) {
            logData("pexpire", key, "ttlMillis=" + millis + ", success=false, reason=missing");
            return false;
        }
        entry.setExpireAtMillis(System.currentTimeMillis() + millis);
        logData("pexpire", key, "ttlMillis=" + millis + ", success=true");
        return true;
    }

    public long ttl(String key) {
        RedisEntry entry = getLiveEntry(key);
        if (entry == null) {
            logData("ttl", key, "result=-2");
            return -2L;
        }
        if (entry.getExpireAtMillis() < 0) {
            logData("ttl", key, "result=-1");
            return -1L;
        }
        long ttlMillis = entry.getExpireAtMillis() - System.currentTimeMillis();
        if (ttlMillis <= 0) {
            store.remove(key, entry);
            logData("ttl", key, "result=-2");
            return -2L;
        }
        long ttl = (ttlMillis + 999L) / 1000L;
        logData("ttl", key, "result=" + ttl);
        return ttl;
    }

    public long pttl(String key) {
        RedisEntry entry = getLiveEntry(key);
        if (entry == null) {
            logData("pttl", key, "result=-2");
            return -2L;
        }
        if (entry.getExpireAtMillis() < 0) {
            logData("pttl", key, "result=-1");
            return -1L;
        }
        long ttlMillis = entry.getExpireAtMillis() - System.currentTimeMillis();
        if (ttlMillis <= 0) {
            store.remove(key, entry);
            logData("pttl", key, "result=-2");
            return -2L;
        }
        logData("pttl", key, "result=" + ttlMillis);
        return ttlMillis;
    }

    public boolean exists(String key) {
        boolean exists = getLiveEntry(key) != null;
        logData("exists", key, "result=" + exists);
        return exists;
    }

    public long del(Collection<String> keys) {
        long deleted = 0L;
        for (String key : keys) {
            RedisEntry removed = store.remove(key);
            if (removed != null) {
                deleted++;
            }
        }
        logData("del", keys.toString(), "deleted=" + deleted);
        return deleted;
    }

    public byte[] get(String key) {
        RedisEntry entry = requireType(key, RedisDataType.STRING, false);
        if (entry == null) {
            logData("get", key, "value=null");
            return null;
        }
        byte[] result = copyBytes((byte[]) entry.getValue());
        logData("get", key, "value=" + summarizeBytes(result));
        return result;
    }

    public void set(String key, byte[] value, Long ttlSeconds) {
        RedisEntry entry = new RedisEntry(RedisDataType.STRING, copyBytes(value));
        if (ttlSeconds != null && ttlSeconds > 0) {
            entry.setExpireAtMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ttlSeconds));
        }
        store.put(key, entry);
        logData("set", key, "value=" + summarizeBytes(value) + ", ttl=" + ttlSeconds);
    }

    public boolean setNx(String key, byte[] value, Long ttlSeconds) {
        RedisEntry newEntry = new RedisEntry(RedisDataType.STRING, copyBytes(value));
        if (ttlSeconds != null && ttlSeconds > 0) {
            newEntry.setExpireAtMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ttlSeconds));
        }
        while (true) {
            RedisEntry existing = store.get(key);
            if (existing != null && !existing.isExpired(System.currentTimeMillis())) {
                return false;
            }
            if (existing != null) {
                store.remove(key, existing);
            }
            RedisEntry previous = store.putIfAbsent(key, newEntry);
            if (previous == null) {
                logData("setNx", key, "value=" + summarizeBytes(value) + ", ttl=" + ttlSeconds + ", success=true");
                return true;
            }
        }
    }

    public long incrBy(String key, long delta) {
        RedisEntry entry = store.compute(key, (ignored, existing) -> {
            if (existing == null || existing.isExpired(System.currentTimeMillis())) {
                RedisEntry created = new RedisEntry(RedisDataType.STRING, longBytes(delta));
                return created;
            }
            ensureType(existing, RedisDataType.STRING);
            synchronized (existing.getMutex()) {
                long next = parseLong((byte[]) existing.getValue()) + delta;
                RedisEntry updated = new RedisEntry(RedisDataType.STRING, longBytes(next));
                updated.setExpireAtMillis(existing.getExpireAtMillis());
                return updated;
            }
        });
        long result = parseLong((byte[]) entry.getValue());
        logData("incrBy", key, "delta=" + delta + ", value=" + result);
        return result;
    }

    public long decrBy(String key, long delta) {
        return incrBy(key, -delta);
    }

    public byte[] hget(String key, byte[] field) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, false);
        if (entry == null) {
            logData("hget", key, "field=" + summarizeBytes(field) + ", value=null");
            return null;
        }
        synchronized (entry.getMutex()) {
            Map<BytesKey, byte[]> map = castHash(entry);
            byte[] result = copyBytes(map.get(new BytesKey(field)));
            logData("hget", key, "field=" + summarizeBytes(field) + ", value=" + summarizeBytes(result));
            return result;
        }
    }

    public Set<byte[]> hkeys(String key) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, false);
        if (entry == null) {
            return Collections.emptySet();
        }
        synchronized (entry.getMutex()) {
            Set<byte[]> result = new LinkedHashSet<>();
            for (BytesKey field : castHash(entry).keySet()) {
                result.add(field.bytes());
            }
            return result;
        }
    }

    public Map<byte[], byte[]> hgetAll(String key) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, false);
        if (entry == null) {
            return Collections.emptyMap();
        }
        synchronized (entry.getMutex()) {
            Map<byte[], byte[]> result = new LinkedHashMap<>();
            for (Map.Entry<BytesKey, byte[]> mapEntry : castHash(entry).entrySet()) {
                result.put(mapEntry.getKey().bytes(), copyBytes(mapEntry.getValue()));
            }
            return result;
        }
    }

    public void hmset(String key, Map<byte[], byte[]> values, Long ttlSeconds) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, true);
        synchronized (entry.getMutex()) {
            Map<BytesKey, byte[]> map = castHash(entry);
            for (Map.Entry<byte[], byte[]> item : values.entrySet()) {
                map.put(new BytesKey(item.getKey()), copyBytes(item.getValue()));
            }
            applyTtl(entry, ttlSeconds);
        }
        logData("hmset", key, "fields=" + values.size() + ", ttl=" + ttlSeconds);
    }

    public long hset(String key, byte[] field, byte[] value, Long ttlSeconds) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, true);
        synchronized (entry.getMutex()) {
            Map<BytesKey, byte[]> map = castHash(entry);
            BytesKey hashKey = new BytesKey(field);
            boolean exists = map.containsKey(hashKey);
            map.put(hashKey, copyBytes(value));
            applyTtl(entry, ttlSeconds);
            long result = exists ? 0L : 1L;
            logData("hset", key, "field=" + summarizeBytes(field) + ", value=" + summarizeBytes(value)
                    + ", ttl=" + ttlSeconds + ", added=" + result);
            return result;
        }
    }

    public long hsetnx(String key, byte[] field, byte[] value, Long ttlSeconds) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, true);
        synchronized (entry.getMutex()) {
            Map<BytesKey, byte[]> map = castHash(entry);
            BytesKey hashKey = new BytesKey(field);
            if (map.containsKey(hashKey)) {
                logData("hsetnx", key, "field=" + summarizeBytes(field) + ", added=0");
                return 0L;
            }
            map.put(hashKey, copyBytes(value));
            applyTtl(entry, ttlSeconds);
            logData("hsetnx", key, "field=" + summarizeBytes(field) + ", value=" + summarizeBytes(value)
                    + ", ttl=" + ttlSeconds + ", added=1");
            return 1L;
        }
    }

    public long hdel(String key, List<byte[]> fields) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            long removed = 0L;
            Map<BytesKey, byte[]> map = castHash(entry);
            for (byte[] field : fields) {
                if (map.remove(new BytesKey(field)) != null) {
                    removed++;
                }
            }
            logData("hdel", key, "fields=" + fields.size() + ", removed=" + removed);
            return removed;
        }
    }

    public boolean hexists(String key, byte[] field) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, false);
        if (entry == null) {
            return false;
        }
        synchronized (entry.getMutex()) {
            return castHash(entry).containsKey(new BytesKey(field));
        }
    }

    public double hincrByFloat(String key, byte[] field, double by) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, true);
        synchronized (entry.getMutex()) {
            Map<BytesKey, byte[]> map = castHash(entry);
            BytesKey hashKey = new BytesKey(field);
            double current = 0D;
            byte[] value = map.get(hashKey);
            if (value != null) {
                current = Double.parseDouble(new String(value, StandardCharsets.UTF_8));
            }
            double next = current + by;
            map.put(hashKey, doubleBytes(next));
            logData("hincrByFloat", key, "field=" + summarizeBytes(field) + ", delta=" + by + ", value=" + next);
            return next;
        }
    }

    public long hincrBy(String key, byte[] field, long by) {
        RedisEntry entry = requireType(key, RedisDataType.HASH, true);
        synchronized (entry.getMutex()) {
            Map<BytesKey, byte[]> map = castHash(entry);
            BytesKey hashKey = new BytesKey(field);
            long current = 0L;
            byte[] value = map.get(hashKey);
            if (value != null) {
                current = Long.parseLong(new String(value, StandardCharsets.UTF_8));
            }
            long next = current + by;
            map.put(hashKey, longBytes(next));
            logData("hincrBy", key, "field=" + summarizeBytes(field) + ", delta=" + by + ", value=" + next);
            return next;
        }
    }

    public long sadd(String key, List<byte[]> values, Long ttlSeconds) {
        RedisEntry entry = requireType(key, RedisDataType.SET, true);
        synchronized (entry.getMutex()) {
            Set<BytesKey> set = castSet(entry);
            long added = 0L;
            for (byte[] value : values) {
                if (set.add(new BytesKey(value))) {
                    added++;
                }
            }
            applyTtl(entry, ttlSeconds);
            logData("sadd", key, "values=" + values.size() + ", added=" + added + ", ttl=" + ttlSeconds);
            return added;
        }
    }

    public Set<byte[]> smembers(String key) {
        RedisEntry entry = requireType(key, RedisDataType.SET, false);
        if (entry == null) {
            return Collections.emptySet();
        }
        synchronized (entry.getMutex()) {
            Set<byte[]> result = new LinkedHashSet<>();
            for (BytesKey item : castSet(entry)) {
                result.add(item.bytes());
            }
            return result;
        }
    }

    public boolean sismember(String key, byte[] value) {
        RedisEntry entry = requireType(key, RedisDataType.SET, false);
        if (entry == null) {
            return false;
        }
        synchronized (entry.getMutex()) {
            return castSet(entry).contains(new BytesKey(value));
        }
    }

    public long scard(String key) {
        RedisEntry entry = requireType(key, RedisDataType.SET, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            return castSet(entry).size();
        }
    }

    public long srem(String key, List<byte[]> values) {
        RedisEntry entry = requireType(key, RedisDataType.SET, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            long removed = 0L;
            Set<BytesKey> set = castSet(entry);
            for (byte[] value : values) {
                if (set.remove(new BytesKey(value))) {
                    removed++;
                }
            }
            logData("srem", key, "values=" + values.size() + ", removed=" + removed);
            return removed;
        }
    }

    public long rpush(String key, List<byte[]> values, Long ttlSeconds) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, true);
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            for (byte[] value : values) {
                list.add(copyBytes(value));
            }
            applyTtl(entry, ttlSeconds);
            long result = list.size();
            logData("rpush", key, "values=" + values.size() + ", size=" + result + ", ttl=" + ttlSeconds);
            return result;
        }
    }

    public long lpush(String key, List<byte[]> values, Long ttlSeconds) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, true);
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            for (byte[] value : values) {
                list.addFirst(copyBytes(value));
            }
            applyTtl(entry, ttlSeconds);
            long result = list.size();
            logData("lpush", key, "values=" + values.size() + ", size=" + result + ", ttl=" + ttlSeconds);
            return result;
        }
    }

    public List<byte[]> lrange(String key, long start, long end) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return Collections.emptyList();
        }
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            int[] range = normalizeRange(list.size(), start, end);
            if (range[0] > range[1]) {
                return Collections.emptyList();
            }
            List<byte[]> result = new ArrayList<>();
            for (int i = range[0]; i <= range[1]; i++) {
                result.add(copyBytes(list.get(i)));
            }
            return result;
        }
    }

    public long llen(String key) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            return castList(entry).size();
        }
    }

    public byte[] lindex(String key, long index) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return null;
        }
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            int normalized = normalizeIndex(list.size(), index);
            if (normalized < 0 || normalized >= list.size()) {
                return null;
            }
            return copyBytes(list.get(normalized));
        }
    }

    public boolean ltrim(String key, long start, long end) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return true;
        }
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            int[] range = normalizeRange(list.size(), start, end);
            if (range[0] > range[1]) {
                list.clear();
                return true;
            }
            List<byte[]> kept = new ArrayList<>();
            for (int i = range[0]; i <= range[1]; i++) {
                kept.add(copyBytes(list.get(i)));
            }
            list.clear();
            list.addAll(kept);
            logData("ltrim", key, "start=" + start + ", end=" + end + ", size=" + list.size());
            return true;
        }
    }

    public boolean lset(String key, long index, byte[] value) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return false;
        }
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            int normalized = normalizeIndex(list.size(), index);
            if (normalized < 0 || normalized >= list.size()) {
                return false;
            }
            list.set(normalized, copyBytes(value));
            logData("lset", key, "index=" + index + ", value=" + summarizeBytes(value));
            return true;
        }
    }

    public long lrem(String key, long count, byte[] value) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            long removed = 0L;
            if (count == 0) {
                count = Long.MAX_VALUE;
            }
            if (count > 0) {
                for (int i = 0; i < list.size() && removed < count; ) {
                    if (Arrays.equals(list.get(i), value)) {
                        list.remove(i);
                        removed++;
                    } else {
                        i++;
                    }
                }
            } else {
                long target = -count;
                for (int i = list.size() - 1; i >= 0 && removed < target; i--) {
                    if (Arrays.equals(list.get(i), value)) {
                        list.remove(i);
                        removed++;
                    }
                }
            }
            logData("lrem", key, "count=" + count + ", value=" + summarizeBytes(value) + ", removed=" + removed);
            return removed;
        }
    }

    public byte[] lpop(String key) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return null;
        }
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            if (list.isEmpty()) {
                return null;
            }
            byte[] result = copyBytes(list.removeFirst());
            logData("lpop", key, "value=" + summarizeBytes(result) + ", size=" + list.size());
            return result;
        }
    }

    public byte[] rpop(String key) {
        RedisEntry entry = requireType(key, RedisDataType.LIST, false);
        if (entry == null) {
            return null;
        }
        synchronized (entry.getMutex()) {
            LinkedList<byte[]> list = castList(entry);
            if (list.isEmpty()) {
                return null;
            }
            byte[] result = copyBytes(list.removeLast());
            logData("rpop", key, "value=" + summarizeBytes(result) + ", size=" + list.size());
            return result;
        }
    }

    public long zadd(String key, List<ZSetMember> members, Long ttlSeconds) {
        RedisEntry entry = requireType(key, RedisDataType.ZSET, true);
        synchronized (entry.getMutex()) {
            Map<BytesKey, Double> zset = castZset(entry);
            long added = 0L;
            for (ZSetMember member : members) {
                BytesKey memberKey = new BytesKey(member.getValue());
                if (!zset.containsKey(memberKey)) {
                    added++;
                }
                zset.put(memberKey, member.getScore());
            }
            applyTtl(entry, ttlSeconds);
            logData("zadd", key, "members=" + members.size() + ", added=" + added + ", ttl=" + ttlSeconds);
            return added;
        }
    }

    public long zrem(String key, List<byte[]> members) {
        RedisEntry entry = requireType(key, RedisDataType.ZSET, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            long removed = 0L;
            Map<BytesKey, Double> zset = castZset(entry);
            for (byte[] member : members) {
                if (zset.remove(new BytesKey(member)) != null) {
                    removed++;
                }
            }
            logData("zrem", key, "members=" + members.size() + ", removed=" + removed);
            return removed;
        }
    }

    public long zcard(String key) {
        RedisEntry entry = requireType(key, RedisDataType.ZSET, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            return castZset(entry).size();
        }
    }

    public long zremrangeByScore(String key, double minScore, double maxScore) {
        RedisEntry entry = requireType(key, RedisDataType.ZSET, false);
        if (entry == null) {
            return 0L;
        }
        synchronized (entry.getMutex()) {
            long removed = 0L;
            Map<BytesKey, Double> zset = castZset(entry);
            List<BytesKey> toRemove = new ArrayList<>();
            for (Map.Entry<BytesKey, Double> item : zset.entrySet()) {
                double score = item.getValue();
                if (score >= minScore && score <= maxScore) {
                    toRemove.add(item.getKey());
                }
            }
            for (BytesKey member : toRemove) {
                if (zset.remove(member) != null) {
                    removed++;
                }
            }
            logData("zremrangeByScore", key, "min=" + minScore + ", max=" + maxScore + ", removed=" + removed);
            return removed;
        }
    }

    public void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        long removed = 0L;
        for (Map.Entry<String, RedisEntry> item : store.entrySet()) {
            if (item.getValue().isExpired(now)) {
                if (store.remove(item.getKey(), item.getValue())) {
                    removed++;
                }
            }
        }
        if (removed > 0 && properties.isCleanupLogEnabled()) {
            log.info("过期键清理完成 removedKeys={}", removed);
        }
    }

    @PreDestroy
    public void shutdown() {
        cleanupExecutor.shutdownNow();
    }

    private RedisEntry requireType(String key, RedisDataType type, boolean createIfAbsent) {
        RedisEntry entry = getLiveEntry(key);
        if (entry == null && createIfAbsent) {
            RedisEntry created = new RedisEntry(type, createValue(type));
            RedisEntry previous = store.putIfAbsent(key, created);
            entry = previous == null ? created : previous;
        }
        if (entry != null) {
            ensureType(entry, type);
        }
        return entry;
    }

    private RedisEntry getLiveEntry(String key) {
        RedisEntry entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired(System.currentTimeMillis())) {
            store.remove(key, entry);
            return null;
        }
        return entry;
    }

    private void ensureType(RedisEntry entry, RedisDataType type) {
        if (entry.getType() != type) {
            throw new IllegalStateException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    private Object createValue(RedisDataType type) {
        switch (type) {
            case HASH:
                return new LinkedHashMap<BytesKey, byte[]>();
            case SET:
                return new LinkedHashSet<BytesKey>();
            case LIST:
                return new LinkedList<byte[]>();
            case ZSET:
                return new LinkedHashMap<BytesKey, Double>();
            default:
                return new byte[0];
        }
    }

    @SuppressWarnings("unchecked")
    private Map<BytesKey, byte[]> castHash(RedisEntry entry) {
        return (Map<BytesKey, byte[]>) entry.getValue();
    }

    @SuppressWarnings("unchecked")
    private Set<BytesKey> castSet(RedisEntry entry) {
        return (Set<BytesKey>) entry.getValue();
    }

    @SuppressWarnings("unchecked")
    private LinkedList<byte[]> castList(RedisEntry entry) {
        return (LinkedList<byte[]>) entry.getValue();
    }

    @SuppressWarnings("unchecked")
    private Map<BytesKey, Double> castZset(RedisEntry entry) {
        return (Map<BytesKey, Double>) entry.getValue();
    }

    private void applyTtl(RedisEntry entry, Long ttlSeconds) {
        if (ttlSeconds != null && ttlSeconds > 0) {
            entry.setExpireAtMillis(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ttlSeconds));
        }
    }

    private int normalizeIndex(int size, long index) {
        int normalized = (int) index;
        if (normalized < 0) {
            normalized = size + normalized;
        }
        return normalized;
    }

    private int[] normalizeRange(int size, long start, long end) {
        if (size == 0) {
            return new int[]{1, 0};
        }
        int normalizedStart = normalizeIndex(size, start);
        int normalizedEnd = normalizeIndex(size, end);
        if (start < 0 && normalizedStart < 0) {
            normalizedStart = 0;
        }
        if (end < 0 && normalizedEnd < 0) {
            normalizedEnd = -1;
        }
        normalizedStart = Math.max(0, normalizedStart);
        normalizedEnd = Math.min(size - 1, normalizedEnd);
        return new int[]{normalizedStart, normalizedEnd};
    }

    private long parseLong(byte[] bytes) {
        return Long.parseLong(new String(bytes, StandardCharsets.UTF_8));
    }

    private byte[] longBytes(long value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] doubleBytes(double value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    public static class ZSetMember {
        private final double score;
        private final byte[] value;

        public ZSetMember(double score, byte[] value) {
            this.score = score;
            this.value = value;
        }

        public double getScore() {
            return score;
        }

        public byte[] getValue() {
            return value;
        }
    }

    private byte[] copyBytes(byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    private void logData(String op, String key, String details) {
        if (properties.isDataLogEnabled()) {
            log.info("缓存操作 op={} key={} {}", op, key, details);
        }
    }

    private String summarizeBytes(byte[] bytes) {
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
}
// [AI:END]
