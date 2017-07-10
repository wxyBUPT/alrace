package com.alibaba.middleware.race.sync.V2.utils;

import java.util.concurrent.TimeUnit;

/**
 * Created by xiyuanbupt on 6/17/17.
 *
 */
public class SleepUtil {

    public static void sleepSeconds(int n){
        try {
            TimeUnit.SECONDS.sleep(n);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}
