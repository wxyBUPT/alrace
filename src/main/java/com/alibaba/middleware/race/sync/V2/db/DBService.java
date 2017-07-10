package com.alibaba.middleware.race.sync.V2.db;

import com.alibaba.middleware.race.sync.Params;
import com.alibaba.middleware.race.sync.V2.io.LoaderExecutor;
import com.alibaba.middleware.race.sync.V2.io.LoaderTaskV4;
import com.alibaba.middleware.race.sync.V2.utils.TimeRecoder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Future;

/**
 * Created by xiyuanbupt on 6/25/17.
 */
public class DBService {
    class ParseHandler implements EventHandler<BlockEvent> {
        private int concurrent_count = Params.CONCURRENT_COUNT;
        private int hash;

        public ParseHandler(int hash) {
            this.hash = hash;
        }
        @Override
        public void onEvent(BlockEvent blockEvent, long l, boolean b) throws Exception {
            if(l % concurrent_count != hash)return;
            blockEvent.parseBlock();
        }
    }

    class TableHandler implements EventHandler<BlockEvent>{
        @Override
        public void onEvent(BlockEvent blockEvent, long l, boolean b) throws Exception {
            blockEvent.mergeToTable();
        }
    }

    public void load(){
        ParseHandler[] parseHandlers = new ParseHandler[Params.CONCURRENT_COUNT];
        for(int i = 0; i<Params.CONCURRENT_COUNT; i++){
            parseHandlers[i] = new ParseHandler(i);
        }
        TimeRecoder.DIS_INIT.start();
        Disruptor<BlockEvent> disruptor = new Disruptor<BlockEvent>(
                BlockEvent.BLOCK_EVENT_EVENT_FACTORY, Params.BLOCK_COUNT,
                ComputeExecutors.INSTANCE, ProducerType.SINGLE, new BusySpinWaitStrategy()
        );
        Future<?> future = LoaderExecutor.EXECUTOR_SERVICE.submit(new LoaderTaskV4(disruptor.getRingBuffer()));

        disruptor.handleEventsWith(
                parseHandlers
        ).then(
                new TableHandler()
        );
        TimeRecoder.DIS_INIT.record("dis init");

        disruptor.start();
        try{
            future.get();
        }catch (Exception e){
            e.printStackTrace();
        }
        disruptor.shutdown();
        TimeRecoder.FILELODER.record("finish sort");
    }
}
