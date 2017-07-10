package com.alibaba.middleware.race.sync;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.sync.V2.db.DBService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 服务器类，负责push消息到client Created by wanshao on 2017/5/25.
 */
public class Server {
    static {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
    }

    // 保存channel
    private static Map<String, Channel> map = new ConcurrentHashMap<String, Channel>();
    // 接收评测程序的三个参数
    public static Map<String, Channel> getMap() {
        return map;
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Schema:"+args[0] + ", Table:" + args[1] + ", Start:" + args[2] + " End:" + args[3]);
        long start = Long.parseLong(args[2]), end = Long.parseLong(args[3]);
        Server server = new Server();
        CountDownLatch sortFinish = new CountDownLatch(1);
        server.startSort(sortFinish);
        server.startServer(5527, sortFinish, start, end);
        //dbService.sendResToClient(start, end);

        // dbService.sendResToClient(start, end);
    }

    private void startSort(final CountDownLatch latch){
        new Thread(new Runnable() {
            @Override
            public void run() {
                DBService dbService= new DBService();
                dbService.load();
                latch.countDown();
            }
        }).start();
    }

    private void startServer(final int port, final CountDownLatch latch, final long start, final long end) {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) throws Exception {
                                    ch.pipeline().addLast(new ServerDemoInHandler(latch, start, end));
                                }
                            })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            f.channel().closeFuture().sync();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}