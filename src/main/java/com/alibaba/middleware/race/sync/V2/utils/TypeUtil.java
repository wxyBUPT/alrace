package com.alibaba.middleware.race.sync.V2.utils;

/**
 * Created by xiyuanbupt on 6/17/17.
 */
public class TypeUtil {
    public static void write2byteInt(byte[] bytes, int i, int start){
        bytes[start++] = (byte) ((i >> 8) & 0xff);
        bytes[start] = (byte)(i & 0xff);
    }

    public static void write3byteInt(byte[] bytes, int i, int start){
        bytes[start++] = (byte) ((i >> 16) & 0xff);
        bytes[start++] = (byte) ((i >> 8) & 0xff);
        bytes[start] = (byte)(i & 0xff);
    }

    public static int get2BytesInt(byte[] bytes, int start){
        return (bytes[start++] & 0xff) << 8  | (bytes[start] & 0xff);
    }

    public static int get3BytesInt(byte[] bytes, int start){
        return (bytes[start++] & 0xff) << 16 | (bytes[start++] & 0xff) << 8  | (bytes[start] & 0xff);
    }

    public static int convertIntToByteStr(byte[] int_str, int value){
        int i = 9;
        while (value != 0){
            int_str[i --] = (byte)('0' + value % 10);
            value /= 10;
        }
        int len = 9 - i;
        System.arraycopy(int_str, i + 1, int_str, 0, len);
        return len;
    }
}
