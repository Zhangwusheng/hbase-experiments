package com.mogujie.mst.hbase.filter;

import com.mogujie.mst.HbaseOperator;
import com.mogujie.mst.filters.CustomFilterForRishi;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static com.mogujie.mst.util.HbaseTestUtil.generateUUID;

/**
 * Created by fenqi on 16/6/18.
 */
public class CustomFilterForRishiTest extends FilterTestBase {
    private static final Logger log = LoggerFactory.getLogger(CustomFilterForRishiTest.class);
    static private char seperator = '-';

    @BeforeClass
    public static void beforeClass() {
        log.info("change valueGenerator ...");
        valueGenerator = (seeds) -> generateUUID(8) + seperator + seeds[0];
        FilterTestBase.beforeClass();
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
