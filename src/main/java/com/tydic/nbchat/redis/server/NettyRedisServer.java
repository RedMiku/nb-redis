package com.tydic.nbchat.redis.server;

// [AI:START] tool=trae date=2026-07-14 author=feifuzeng
import com.tydic.nbchat.redis.config.RedisServerProperties;
import com.tydic.nbchat.redis.protocol.RespCommandDecoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
public class NettyRedisServer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(NettyRedisServer.class);

    private final RedisServerProperties properties;

    private final RedisCommandHandler commandHandler;

    private volatile boolean running;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private Channel serverChannel;

    public NettyRedisServer(RedisServerProperties properties, RedisCommandHandler commandHandler) {
        this.properties = properties;
        this.commandHandler = commandHandler;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        bossGroup = new NioEventLoopGroup(properties.getBossThreads());
        workerGroup = properties.getWorkerThreads() > 0
                ? new NioEventLoopGroup(properties.getWorkerThreads())
                : new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RespCommandDecoder());
                            ch.pipeline().addLast(commandHandler);
                        }
                    });
            serverChannel = bootstrap.bind(properties.getHost(), properties.getPort()).sync().channel();
            running = true;
            log.info("nbchat-redis 已启动，监听 {}:{}", properties.getHost(), properties.getPort());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("启动 nbchat-redis 失败", e);
        }
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        try {
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
        } finally {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully().syncUninterruptibly();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully().syncUninterruptibly();
            }
            running = false;
            log.info("nbchat-redis 已停止");
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
// [AI:END]
