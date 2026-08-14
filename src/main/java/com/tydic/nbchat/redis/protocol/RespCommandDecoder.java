package com.tydic.nbchat.redis.protocol;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespCommandDecoder extends ByteToMessageDecoder {

    private static final byte ARRAY_PREFIX = '*';
    private static final byte BULK_PREFIX = '$';
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        in.markReaderIndex();
        if (!in.isReadable()) {
            return;
        }
        byte first = in.readByte();
        if (first == ARRAY_PREFIX) {
            decodeArray(in, out);
            return;
        }

        in.resetReaderIndex();
        decodeInline(in, out);
    }

    private void decodeArray(ByteBuf in, List<Object> out) {
        Integer count = readLength(in);
        if (count == null) {
            in.resetReaderIndex();
            return;
        }
        List<byte[]> parts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (!in.isReadable()) {
                in.resetReaderIndex();
                return;
            }
            if (in.readByte() != BULK_PREFIX) {
                throw new IllegalStateException("仅支持RESP Bulk String参数");
            }
            Integer len = readLength(in);
            if (len == null) {
                in.resetReaderIndex();
                return;
            }
            if (len < 0) {
                parts.add(null);
                continue;
            }
            if (in.readableBytes() < len + 2) {
                in.resetReaderIndex();
                return;
            }
            byte[] bytes = new byte[len];
            in.readBytes(bytes);
            expectCrlf(in);
            parts.add(bytes);
        }
        out.add(new RespCommand(parts));
    }

    private void decodeInline(ByteBuf in, List<Object> out) {
        String line = readLine(in);
        if (line == null) {
            in.resetReaderIndex();
            return;
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        String[] items = trimmed.split("\\s+");
        List<byte[]> parts = new ArrayList<>(items.length);
        for (String item : items) {
            parts.add(item.getBytes(StandardCharsets.UTF_8));
        }
        out.add(new RespCommand(parts));
    }

    private Integer readLength(ByteBuf in) {
        String line = readLine(in);
        if (line == null) {
            return null;
        }
        return Integer.parseInt(line);
    }

    private String readLine(ByteBuf in) {
        int lfIndex = in.forEachByte(value -> value != LF);
        if (lfIndex < 0) {
            return null;
        }
        int length = lfIndex - in.readerIndex() - 1;
        if (length < 0) {
            throw new IllegalStateException("RESP报文格式非法");
        }
        byte[] lineBytes = new byte[length];
        in.readBytes(lineBytes);
        if (in.readByte() != CR || in.readByte() != LF) {
            throw new IllegalStateException("RESP行结束符非法");
        }
        return new String(lineBytes, StandardCharsets.UTF_8);
    }

    private void expectCrlf(ByteBuf in) {
        if (in.readByte() != CR || in.readByte() != LF) {
            throw new IllegalStateException("RESP参数结束符非法");
        }
    }
}
// [AI:END]
