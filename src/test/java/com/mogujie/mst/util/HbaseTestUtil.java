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

    public static byte[] getOneBitBiggerBytes(byte[] in) {
        byte[] out = null;
        boolean king = true;

        for (int i = 0; i < in.length; i++) {
            if (in[i] != 0xff) {
                out = in.clone();
                out[i] += 1;
                king = false;
            }
        }

        /**
         * all right, welcome the king of this len
         */
        if (king) {
            out = new byte[in.length + 1];
            System.arraycopy(in, 0, out, 0, in.length);
            out[out.length] = 0;
        }

        return out;
    }

}
