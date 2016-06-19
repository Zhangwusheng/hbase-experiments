package com.mogujie.mst;

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

/**
 * Created by fenqi on 16/6/18.
 */
public class CustomFilterForRishiTest {
    private static final Logger log = LoggerFactory.getLogger(CustomFilterForRishiTest.class);
    private static HbaseOperator operator = null;

    @BeforeClass
    public static void beforeClass() {
        operator = new HbaseOperator();
        operator.init("10.13.42.19");
    }

    @AfterClass
    public static void afterClass() {
        operator.close();
    }

    public static String generateUUID() {
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, 8);
    }

    @Test
    public void test() {

        String tableName = "t1",
                columnFamily = "cf",
                qualifier = "q";

        Table table = null;

        try {
            table = operator.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        int count = 5;
        char seperator = ':';

        for (int i = 0; i < count; i++) {
            String rowKey = "r" + i,
                    value = generateUUID() + seperator + i;
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            try {
                table.put(put);
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }

        char c = seperator;
        long start = 2, end = 4;
        CustomFilterForRishi customFilterForRishi =
                new CustomFilterForRishi(
                        Bytes.toBytes(columnFamily),
                        Bytes.toBytes(qualifier),
                        c, start, end);

        Scan scan = new Scan();
        scan.setFilter(customFilterForRishi);

        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);
            int resultNum = 0;
            for (Result result = scanner.next(); result != null; resultNum++, result = scanner.next());
            scanner.close();
            Assert.assertEquals(resultNum, 3);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

}
