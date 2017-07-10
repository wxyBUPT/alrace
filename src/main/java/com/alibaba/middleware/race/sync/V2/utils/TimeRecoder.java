package com.alibaba.middleware.race.sync.V2.utils;


/**
 * Created by xiyuanbupt on 6/16/17.
 */
public class TimeRecoder {

    public static TimeRecoder FILELODER = new TimeRecoder();
    public static TimeRecoder DIS_INIT = new TimeRecoder();

    private long start;
    private long record;

    public void start(){
        start = System.nanoTime();
        record = start;
    }

    public void record(String event){
        long now = System.nanoTime();
        long gap = now - record;
        record = now;
        long total = now - start;
        System.out.println(event + " use time: " + gap / 1000000 + "ms" + (gap % 1000000)/1000 + "us, total time: " + total/1000000 + "ms" + (total % 1000000)/1000 + "us");
    }
}
