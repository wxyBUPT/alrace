package com.alibaba.middleware.race.sync;

/**
 * Created by xiyuanbupt on 6/6/17.
 */
public interface Params {

    int BUFFER_SIZE_TABLE = 1 << 18; // 256K
    int MAX_ROW_PER_BUFFER = 11 * (1 << 10); // max row in a buffer
    int BLOCK_COUNT = 64;

    int CONCURRENT_COUNT = Constants.dev?12:12;

    byte SEP_ROW = 10; //"/n"
    byte SEP_COL = 124; // "|"
    byte OP_INSERT = 73; // "I"
    byte OP_UPDATE = 85; // "U"
    byte MALE_FIRST = -25; // "男".getBytes()[0] == -25
    byte SEP_ANS_COL = 9; // '/t'
    byte[] MALE  = "男".getBytes();
    byte[] FEMALE = "女".getBytes();

    int BIG_TABLE_ROW_COUNT = Constants.dev?((1 << 20) * 10):((1 << 20) * 10);
}