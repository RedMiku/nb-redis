package com.tydic.nbchat.redis.config;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "nbchat.redis-server")
public class RedisServerProperties {

    private String host = "0.0.0.0";

    private int port = 6379;

    private int bossThreads = 1;

    private int workerThreads = 0;

    private long cleanupIntervalSeconds = 5L;

    private boolean connectionLogEnabled = true;

    private boolean commandLogEnabled = true;

    private boolean dataLogEnabled = true;

    private boolean cleanupLogEnabled = true;

    private int maxLoggedValueLength = 128;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBossThreads() {
        return bossThreads;
    }

    public void setBossThreads(int bossThreads) {
        this.bossThreads = bossThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public long getCleanupIntervalSeconds() {
        return cleanupIntervalSeconds;
    }

    public void setCleanupIntervalSeconds(long cleanupIntervalSeconds) {
        this.cleanupIntervalSeconds = cleanupIntervalSeconds;
    }

    public boolean isConnectionLogEnabled() {
        return connectionLogEnabled;
    }

    public void setConnectionLogEnabled(boolean connectionLogEnabled) {
        this.connectionLogEnabled = connectionLogEnabled;
    }

    public boolean isCommandLogEnabled() {
        return commandLogEnabled;
    }

    public void setCommandLogEnabled(boolean commandLogEnabled) {
        this.commandLogEnabled = commandLogEnabled;
    }

    public boolean isDataLogEnabled() {
        return dataLogEnabled;
    }

    public void setDataLogEnabled(boolean dataLogEnabled) {
        this.dataLogEnabled = dataLogEnabled;
    }

    public boolean isCleanupLogEnabled() {
        return cleanupLogEnabled;
    }

    public void setCleanupLogEnabled(boolean cleanupLogEnabled) {
        this.cleanupLogEnabled = cleanupLogEnabled;
    }

    public int getMaxLoggedValueLength() {
        return maxLoggedValueLength;
    }

    public void setMaxLoggedValueLength(int maxLoggedValueLength) {
        this.maxLoggedValueLength = maxLoggedValueLength;
    }
}
// [AI:END]
