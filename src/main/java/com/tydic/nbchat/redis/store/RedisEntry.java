package com.tydic.nbchat.redis.store;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
public class RedisEntry {

    private final RedisDataType type;

    private final Object value;

    private final Object mutex = new Object();

    private volatile long expireAtMillis = -1L;

    public RedisEntry(RedisDataType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public RedisDataType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    public Object getMutex() {
        return mutex;
    }

    public long getExpireAtMillis() {
        return expireAtMillis;
    }

    public void setExpireAtMillis(long expireAtMillis) {
        this.expireAtMillis = expireAtMillis;
    }

    public boolean isExpired(long now) {
        return expireAtMillis > 0 && expireAtMillis <= now;
    }
}
// [AI:END]
