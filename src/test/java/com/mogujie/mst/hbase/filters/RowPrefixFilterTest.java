package com.mogujie.mst.hbase.filters;

import com.google.gson.Gson;
import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by fenqi on 16/7/22.
 */
public class RowPrefixFilterTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(RowPrefixFilterTest.class);

    @BeforeClass
    public static void beforeClass() {
        log.info("set count to 0, do warm up myself ...");
        count = 0;
        HbaseTestBase.beforeClass();
        warmUp();
    }

    private static void warmUp() {
        try {

            Gson gson = new Gson();
            Put put = null;
            Map<String, String> map = new HashMap();

            put = new Put(Bytes.toBytes("ROW1"));
            map.clear();
            map.put("KEY1", "VALUE");
            map.put("KEY2", "VALUE");
            map.put("KEY3", "VALUE");
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(gson.toJson(map)));
            table.put(put);

            put = new Put(Bytes.toBytes("ROW2"));
            map.clear();
            map.put("KEY1", "VALUE");
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(gson.toJson(map)));
            table.put(put);

            put = new Put(Bytes.toBytes("ROW3"));
            map.clear();
            map.put("KEY1", "VALUE");
            map.put("KEY5", "VALUE");
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(gson.toJson(map)));
            table.put(put);

            put = new Put(Bytes.toBytes("ROW4"));
            map.clear();
            map.put("KEY8", "VALUE");
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(gson.toJson(map)));
            table.put(put);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    @Test
    public void testRowPrefixFilter() {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        scan.setRowPrefixFilter(Bytes.toBytes("KEY8"));
        ResultScanner resultScanner = null;
        try {
            resultScanner = table.getScanner(scan);
            for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                log.info("{}", result);
            }
            resultScanner.close();
        } catch (IOException e) {
            Assert.fail(e.toString());
        } finally {
            if(null != resultScanner) {
                resultScanner.close();
            }
        }
    }

    @Test
    public void testSingleColumnValueFilter() {
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(qualifier),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("KEY8")
        );
//        Scan scan = new Scan();
//        scan.setFilter(singleColumnValueFilter);
//        ResultScanner resultScanner = table.getScanner(scan);
        scanAndCheck(singleColumnValueFilter, 1);
    }
}
