package com.mogujie.mst.hbase.filters;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by fenqi on 16/9/21.
 * try to answer http://stackoverflow.com/questions/39585560/scan-settimerange-get-timeout-hbase-java
 */
public class TimeRangeTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(TimeRangeTest.class);
    private static int expectNum = 5, waitInterval = 2000;
    private static long t1, t2;

    @BeforeClass
    public static void beforeClass() {
        log.info("set count to 0, do warm up myself ...");
        count = 0;
        HbaseTestBase.beforeClass();
        warmUp();
    }

    private static void warmUp() {

        try {
            Put put;

            t1 = System.currentTimeMillis();

            // following rows should be scanned out
            for (int i = 0; i < expectNum; i++) {
                put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(qualifier));

                table.put(put);
            }

            Thread.sleep(waitInterval);

            t2 = System.currentTimeMillis();

            Thread.sleep(waitInterval);

            // following rows should not be scanned out
            for (int i = 0; i < expectNum; i++) {
                put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(qualifier));

                table.put(put);
            }


        } catch (Exception e) {
            Assert.fail(e.toString());
        }

    }

    @Test
    public void testTimeRange() {

        scanAndCheck(null, expectNum);

    }
}
