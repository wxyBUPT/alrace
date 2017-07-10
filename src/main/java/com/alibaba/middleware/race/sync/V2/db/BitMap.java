package com.alibaba.middleware.race.sync.V2.db;

import com.alibaba.middleware.race.sync.Params;

/**
 * Created by xiyuanbupt on 6/17/17.
 */
public class BitMap {

    static public BitMap INSTANCE = new BitMap(Params.BIG_TABLE_ROW_COUNT);

    private final long[] data;
    private BitMap(int bits){
        this.data = new long[bits >>> 6];
    }

    public void set(int index){
        data[(index >>> 6)] |= (1L << index);
    }

    public boolean get(int index){
        return (data[(index >>> 6)] & (1L << index)) != 0;
    }

    public void unset(int index){
        data[(index >>> 6)] &= ~(1L << index);
    }
}