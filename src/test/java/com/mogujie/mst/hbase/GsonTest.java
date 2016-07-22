package com.mogujie.mst.hbase;

import com.google.gson.Gson;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by fenqi on 16/7/22.
 */
public class GsonTest {
    private static final Logger log = LoggerFactory.getLogger(GsonTest.class);

    @Test
    public void testMapDump() {
        Gson gson = new Gson();

        Map<String, String> map = new HashMap();
        map.put("KEY1", "VALUE");
        map.put("KEY2", "VALUE");
        map.put("KEY3", "VALUE");
        log.info(gson.toJson(map));

        map.clear();
        map.put("KEY1", "VALUE");
        log.info(gson.toJson(map));

        map.clear();
        map.put("KEY1", "VALUE");
        map.put("KEY5", "VALUE");
        log.info(gson.toJson(map));

        map.clear();
        map.put("KEY8", "VALUE");
        log.info(gson.toJson(map));
    }
}
