package com.mogujie.mst.util;

import java.util.UUID;

/**
 * Created by fenqi on 16/6/30.
 */
public final class HbaseTestUtil {
    public static String generateUUID(int size) {
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, size);
    }

}
