package com.alibaba.middleware.race.sync.V2.io;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Params;
import com.alibaba.middleware.race.sync.V2.db.BlockEvent;
import com.alibaba.middleware.race.sync.V2.utils.TimeRecoder;
import com.lmax.disruptor.RingBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by xiyuanbupt on 6/23/17.
 */
public class LoaderTaskV4 implements Runnable{

    private final RingBuffer<BlockEvent> ringBuffer;

    public LoaderTaskV4(RingBuffer<BlockEvent> ringBuffer){
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void run() {
        try {
            long sequence = ringBuffer.next();
            BlockEvent blockEvent = ringBuffer.get(sequence);
            ByteBuffer buffer = blockEvent.byteBuffer;
            buffer.clear();
            TimeRecoder.FILELODER.start();
            for (int num = 1; num < 11; num++) {
                String file = Constants.DATA_HOME + File.separator + num + ".txt";
                Path path = Paths.get(file);
                FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
                int length;
                while (channel.read(buffer) > 0) {
                    length = buffer.position();
                    byte[] data = buffer.array();
                    int last_row_sep = length - 1;
                    while (data[last_row_sep] != Params.SEP_ROW) {
                        last_row_sep--;
                    }
                    long preSequence = sequence;
                    buffer.limit(last_row_sep + 1);
                    buffer.position(0);
                    sequence = ringBuffer.next();
                    buffer = ringBuffer.get(sequence).byteBuffer;
                    buffer.clear();
                    buffer.put(data, last_row_sep + 1, length - last_row_sep - 1);
                    ringBuffer.publish(preSequence);
                }
                channel.close();
                TimeRecoder.FILELODER.record("laod file: " + num);
            }
            buffer.limit(0);
            ringBuffer.publish(sequence);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
