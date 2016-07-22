package com.mogujie.mst.hbase.filters;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mogujie.mst.util.HbaseTestUtil.generateUUID;

/**
 * Created by fenqi on 16/6/18.
 */
public class CustomFilterForRishiTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(CustomFilterForRishiTest.class);
    private static char seperator = '-';

    @BeforeClass
    public static void beforeClass() {
        log.info("change valueGenerator ...");
        valueGenerator = (seeds) -> generateUUID(8) + seperator + seeds[0];
        HbaseTestBase.beforeClass();
    }

    @Test
    public void testCustomFilterForRishi() {
        char c = seperator;
        long start = 2, end = 4;
        CustomFilterForRishi customFilterForRishi =
                new CustomFilterForRishi(
                        Bytes.toBytes(columnFamily),
                        Bytes.toBytes(qualifier),
                        c, start, end);

        scanAndCheck(customFilterForRishi, 3);
    }

}
