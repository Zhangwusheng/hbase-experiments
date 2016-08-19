package com.mogujie.mst.hbase.filters;

import com.google.gson.Gson;
import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by fenqi on 16/7/22.
 */
public class RowSuffixFilterTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(RowSuffixFilterTest.class);

    @BeforeClass
    public static void beforeClass() {
        log.info("set count to 0, do warm up myself ...");
        count = 0;
        HbaseTestBase.beforeClass();
        warmUp();
    }

    private static void warmUp() {
        try {
            Put put = null;

            put = new Put(Bytes.toBytes("07/08/2016_99"));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes("value"));
            table.put(put);

            put = new Put(Bytes.toBytes("08/08/2016_99"));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes("value"));
            table.put(put);

            put = new Put(Bytes.toBytes("09/08/2016_99"));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes("value"));
            table.put(put);

            put = new Put(Bytes.toBytes("09/08/2016_100"));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes("value"));
            table.put(put);

        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    @Test
    public void testRowSuffixFilter() {
//        String pattern = "99";
//        String expr = "^.*" + pattern + "$";

        String expr = "^.*99$";
        int flag = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;;
        RegexStringComparator.EngineType engineType = RegexStringComparator.EngineType.JAVA;

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(expr, flag, engineType));

        scanAndCheck(rowFilter, 3);
    }

}
