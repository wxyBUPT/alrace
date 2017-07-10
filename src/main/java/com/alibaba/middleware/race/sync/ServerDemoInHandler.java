package com.alibaba.middleware.race.sync;

import com.alibaba.middleware.race.sync.V2.db.BigTable;
import com.alibaba.middleware.race.sync.V2.db.BitMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private CountDownLatch sortFinish;
    private long start;
    private long end;
    public ServerDemoInHandler(CountDownLatch latch, long start, long end) {
        super();
        sortFinish = latch;
        this.start = start;
        this.end = end;
    }

    /**
     * 根据channel
     * 
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 保存channel
        Server.getMap().put(getIPString(ctx), ctx.channel());
        System.out.println("channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
        // 接收并打印客户端的信息
        System.out.println("client said:  " + resultStr);
        sortFinish.await();
        System.out.println("sendResult, start: " + start + ", end: " + end);

        Iterator<Channel> iter = Server.getMap().values().iterator();

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1 << 18);

        //ByteBuf byteBuf = Unpooled.buffer(1 << 20);//PooledByteBufAllocator.DEFAULT.buffer(initialCapacity)
        if (iter.hasNext()) {
            Channel channel = iter.next();
            byte[] int_str = new byte[10];
            for (int i = (int) start + 1; i < end; i++) {
                if (BitMap.INSTANCE.get(i)) {
                    if(byteBuffer.remaining() < 70){
                        byteBuffer.flip();
                        ByteBuf byteBuf = Unpooled.copiedBuffer(byteBuffer);
                        channel.writeAndFlush(byteBuf);
                        byteBuffer.clear();
                    }
                    BigTable.INSTANCE.writeRowToByteBufer(i, byteBuffer, int_str);
                }
            }
            byteBuffer.flip();
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            ChannelFuture f = channel.writeAndFlush(Unpooled.wrappedBuffer(bytes));
            byteBuffer.clear();
            f.addListener(
                    new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture channelFuture) throws Exception {
                            channelFuture.channel().close();
                            System.out.println("Close channel!");
                        }
                    }
            );
            System.out.println("finish send");
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private Object getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
        Thread.sleep(5000);

        return "message generated in ServerDemoInHandler";

    }
}
