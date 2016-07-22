package com.mogujie.mst.hbase.filters;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by fenqi on 16/6/18.
 */
public class ColumnPrefixFilterTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(ColumnPrefixFilterTest.class);
    private static String[] qualifiers = {"a.b.c", "a.b.d", "a.e"};

    @BeforeClass
    public static void beforeClass() {
        log.info("change qualifierGenerator ...");
        qualifierGenerator = (seeds) -> qualifiers;
        dropTable = false;
        zookeeperURI = "localhost";
        HbaseTestBase.beforeClass();
    }

    @Test
    public void testColumnPrefixFilter() {
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("a"));

        String startKey = keyGenerator.get(String.valueOf("1"), "", keyPrefix),
                endKey = keyGenerator.get(String.valueOf("1"), "", keyPrefix);

        scanAndCheck(columnPrefixFilter, 1,
                startKey, endKey,
                qualifiers.length, columnFamily);
    }

    @Test
    public void testNativeUsage() {
        byte[] columnFamily = Bytes.toBytes("0"),
                qualifierPrefix = Bytes.toBytes("a"),
                rowKey = Bytes.toBytes("row-key");

        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(qualifierPrefix);

        Scan scan = new Scan();
        scan.setFilter(columnPrefixFilter);
        scan.addFamily(columnFamily);
        scan.setStartRow(rowKey);
        scan.setStopRow(rowKey);

        Assert.assertTrue(true);
    }
}
