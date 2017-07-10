package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * Created by wanshao on 2017/5/25.
 */
public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {

    private FileChannel outChannel;
    private String           outFile   = Constants.RESULT_HOME + File.separator
            + Constants.RESULT_FILE_NAME;
    public ClientDemoInHandler(){
        super();
        try {
            System.out.println("client init");
            outChannel = new FileOutputStream(outFile).getChannel();
        } catch (FileNotFoundException e) {
        }
    }

    // 接收server端的消息，并打印出来
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf)msg;
        byteBuf.readBytes(outChannel, byteBuf.readableBytes());
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channelActive");
        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        // 关闭文件
        outChannel.close();
        System.exit(0);
    }
}
