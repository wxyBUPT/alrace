package com.alibaba.middleware.race.sync.V2.utils;

/**
 * Created by xiyuanbupt on 6/16/17.
 */
public class BytesUtil {

    public static int BytesIndexToInt(byte[] bytes, int start, int end){
        int res = 0;
        while (start <= end){
            res *= 10;
            res += bytes[start++] - 48;
        }
        return res;
    }
}
