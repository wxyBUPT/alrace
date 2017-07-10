package com.alibaba.middleware.race.sync.V2.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by xiyuanbupt on 6/16/17.
 */
public class LoaderExecutor {
    public static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
}
