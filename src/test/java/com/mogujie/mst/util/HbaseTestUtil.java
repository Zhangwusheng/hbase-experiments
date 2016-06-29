package com.mogujie.mst.util;

import java.util.Arrays;
import java.util.UUID;

/**
 * Created by fenqi on 16/6/30.
 */
public final class HbaseTestUtil {
    public static String generateUUID(int size) {
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, size);
    }

    public static String getStrOfRepeatedChar(int len, char ch) {
        char[] chars = new char[len];
        Arrays.fill(chars, ch);
        return new String(chars);
    }

}
