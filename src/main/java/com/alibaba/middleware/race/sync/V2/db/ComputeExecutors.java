package com.alibaba.middleware.race.sync.V2.db;

import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by xiyuanbupt on 6/17/17.
 */
public class ComputeExecutors {
    public static final ExecutorService INSTANCE = Executors.newFixedThreadPool(15, DaemonThreadFactory.INSTANCE);
}
