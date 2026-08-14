package com.tydic.nbchat.redis.protocol;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class RespResponseWriter {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    private RespResponseWriter() {
    }

    public static ByteBuf simpleString(String value) {
        return prefixed('+', value == null ? "OK".getBytes(StandardCharsets.UTF_8) : value.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuf error(String message) {
        return prefixed('-', message.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuf integer(long value) {
        return prefixed(':', String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuf bulk(byte[] value) {
        if (value == null) {
            return Unpooled.copiedBuffer("$-1\r\n", StandardCharsets.UTF_8);
        }
        ByteBuf buffer = Unpooled.buffer(16 + value.length);
        buffer.writeByte('$');
        buffer.writeBytes(String.valueOf(value.length).getBytes(StandardCharsets.UTF_8));
        buffer.writeBytes(CRLF);
        buffer.writeBytes(value);
        buffer.writeBytes(CRLF);
        return buffer;
    }

    public static ByteBuf array(List<byte[]> values) {
        if (values == null) {
            return Unpooled.copiedBuffer("*-1\r\n", StandardCharsets.UTF_8);
        }
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('*');
        buffer.writeBytes(String.valueOf(values.size()).getBytes(StandardCharsets.UTF_8));
        buffer.writeBytes(CRLF);
        for (byte[] value : values) {
            buffer.writeBytes(bulk(value));
        }
        return buffer;
    }

    public static ByteBuf integerArray(List<Long> values) {
        if (values == null) {
            return Unpooled.copiedBuffer("*-1\r\n", StandardCharsets.UTF_8);
        }
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('*');
        buffer.writeBytes(String.valueOf(values.size()).getBytes(StandardCharsets.UTF_8));
        buffer.writeBytes(CRLF);
        for (Long value : values) {
            buffer.writeBytes(integer(value == null ? 0L : value));
        }
        return buffer;
    }

    public static ByteBuf arrayOfBuffers(List<ByteBuf> values) {
        if (values == null) {
            return Unpooled.copiedBuffer("*-1\r\n", StandardCharsets.UTF_8);
        }
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('*');
        buffer.writeBytes(String.valueOf(values.size()).getBytes(StandardCharsets.UTF_8));
        buffer.writeBytes(CRLF);
        for (ByteBuf value : values) {
            if (value != null) {
                buffer.writeBytes(value.copy());
            } else {
                buffer.writeBytes(bulk(null));
            }
        }
        return buffer;
    }

    public static ByteBuf arrayFromSet(Collection<byte[]> values) {
        return array(values == null ? null : values.stream().toList());
    }

    public static ByteBuf arrayFromMap(Map<byte[], byte[]> values) {
        if (values == null) {
            return Unpooled.copiedBuffer("*-1\r\n", StandardCharsets.UTF_8);
        }
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte('*');
        buffer.writeBytes(String.valueOf(values.size() * 2).getBytes(StandardCharsets.UTF_8));
        buffer.writeBytes(CRLF);
        for (Map.Entry<byte[], byte[]> entry : values.entrySet()) {
            buffer.writeBytes(bulk(entry.getKey()));
            buffer.writeBytes(bulk(entry.getValue()));
        }
        return buffer;
    }

    private static ByteBuf prefixed(char prefix, byte[] bytes) {
        ByteBuf buffer = Unpooled.buffer(3 + bytes.length);
        buffer.writeByte((byte) prefix);
        buffer.writeBytes(bytes);
        buffer.writeBytes(CRLF);
        return buffer;
    }
}
// [AI:END]
