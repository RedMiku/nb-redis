package com.tydic.nbchat.redis.protocol;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RespCommand {

    private final String name;

    private final List<byte[]> arguments;

    public RespCommand(List<byte[]> rawParts) {
        if (rawParts == null || rawParts.isEmpty()) {
            throw new IllegalArgumentException("RESP命令不能为空");
        }
        this.name = new String(rawParts.get(0), StandardCharsets.UTF_8).toUpperCase();
        this.arguments = new ArrayList<>();
        for (int i = 1; i < rawParts.size(); i++) {
            this.arguments.add(rawParts.get(i));
        }
    }

    public String getName() {
        return name;
    }

    public List<byte[]> getArguments() {
        return Collections.unmodifiableList(arguments);
    }
}
// [AI:END]
