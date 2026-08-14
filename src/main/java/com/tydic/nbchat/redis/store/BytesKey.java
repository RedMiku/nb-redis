package com.tydic.nbchat.redis.store;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import java.util.Arrays;

public final class BytesKey {

    private final byte[] data;

    public BytesKey(byte[] data) {
        this.data = data == null ? new byte[0] : Arrays.copyOf(data, data.length);
    }

    public byte[] bytes() {
        return Arrays.copyOf(data, data.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BytesKey)) {
            return false;
        }
        BytesKey other = (BytesKey) obj;
        return Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
// [AI:END]
