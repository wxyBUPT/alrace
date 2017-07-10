package com.alibaba.middleware.race.sync.V2.db;


import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Params;
import com.alibaba.middleware.race.sync.V2.utils.TypeUtil;

import java.nio.ByteBuffer;

/**
 * Created by xiyuanbupt on 6/16/17.
 * 先把单线程的写出来, 多线程需要考虑是否会发生伪共享, 以及时间戳覆盖的相关问题
 * 这里是一个120M 的大table
 * |mysql-bin.000022814547989|1497439282000|middleware8|student|I|id:1:1|NULL|1|first_name:2:0|NULL|邹|last_name:2:0|NULL|明益|sex:2:0|NULL|女|score:1:0|NULL|797|score2:1:0|NULL|106271|
 */
public class BigTable {

    public static BigTable INSTANCE = new BigTable();

    private final int BIG_TABLE_ROW_COUNT = Params.BIG_TABLE_ROW_COUNT;

    private BigTable(){
        // 在内存中维护10M行数据, 3byte first_name, 6byte last_name, 1byte sex(因为目前用单线程去做, 存储数据), 2byte score, 3byte score2
        first_names = new byte[BIG_TABLE_ROW_COUNT * 3];
        last_names = new byte[BIG_TABLE_ROW_COUNT * 6];
        sexs = new boolean[BIG_TABLE_ROW_COUNT];
        scores = new byte[BIG_TABLE_ROW_COUNT * 2]; // 存储数字而非字符串
        score2s = new byte[BIG_TABLE_ROW_COUNT * 3]; // 存储数字而非字符串
    }

    private byte[] first_names;
    private byte[] last_names;
    private boolean[] sexs; // true male, false female
    private byte[] scores;
    private byte[] score2s;

    public void storeSex(boolean isMale, int row){
        sexs[row] = isMale;
    }

    public void storeFirstNameFir(byte[] data, int start, int len, int row){
        System.arraycopy(data, start, first_names, row * 3, len);
    }

    public void storeLastNameFir(byte[] data, int start, int len, int row){
        System.arraycopy(data, start, last_names, row * 6, len);
    }

    public void storeFirstName(byte[] data, int start, int len, int row){
        int i = row * 3;
        System.arraycopy(data, start, first_names, i, len);
        if(len < 3){
            first_names[i + len] = 0;
        }
    }

    public void storeLastName(byte[] data, int start, int len, int row){
        int i = row * 6;
        System.arraycopy(data, start, last_names, i, len);
        if(len < 6){
            last_names[i + len] = 0;
        }
    }

    void storeScore(byte[] score, int row){
        System.arraycopy(score, 0, scores, row * 2, 2);
    }

    void storeScore2(byte[] score2, int row){
        System.arraycopy(score2, 0, score2s, row * 3, 3);
    }

    void mv(int from, int to) {
        // sex
        sexs[to] = sexs[from];
        // firstName
        System.arraycopy(first_names, from * 3, first_names, to * 3, 3);
        // lastName
        System.arraycopy(last_names, from * 6, last_names, to * 6, 6);
        // score
        System.arraycopy(scores, from * 2, scores, to * 2, 2);
        // score2
        System.arraycopy(score2s, from * 3, score2s, to * 3, 3);
    }

    public void writeRowToByteBufer(int id, ByteBuffer byteBuffer, byte[] int_str){

        int id_len = TypeUtil.convertIntToByteStr(int_str, id);
        byteBuffer.put(int_str, 0, id_len).put(Params.SEP_ANS_COL);
        int i = id * 3;
        int j = (id + 1) * 3;
        for(;i<j&&first_names[i] != 0; i++){
            byteBuffer.put(first_names[i]);
        }
        byteBuffer.put(Params.SEP_ANS_COL);
        i = id * 6;
        j = (id + 1) * 6;
        for(;i<j&&last_names[i] != 0; i++){
            byteBuffer.put(last_names[i]);
        }
        byteBuffer.put(Params.SEP_ANS_COL);
        if(sexs[id]){
            byteBuffer.put(Params.MALE);
        }else {
            byteBuffer.put(Params.FEMALE);
        }
        byteBuffer.put(Params.SEP_ANS_COL);

        int score = TypeUtil.get2BytesInt(scores, id * 2);
        int score_len = TypeUtil.convertIntToByteStr(int_str, score);
        byteBuffer.put(int_str, 0, score_len);
        if(Constants.dev){
            byteBuffer.put(Params.SEP_ROW);
            return;
        }
        byteBuffer.put(Params.SEP_ANS_COL);
        int score2 = TypeUtil.get3BytesInt(score2s, id * 3);
        int score2_len = TypeUtil.convertIntToByteStr(int_str, score2);
        byteBuffer.put(int_str, 0, score2_len);
        byteBuffer.put(Params.SEP_ROW);
    }
}
