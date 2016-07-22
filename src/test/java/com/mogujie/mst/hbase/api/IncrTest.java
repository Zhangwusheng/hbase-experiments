package com.mogujie.mst.hbase.api;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by fenqi on 16/7/10.
 */
public class IncrTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(IncrTest.class);
    private static String rowKey = "rowKey";
    private static int intMaxVersions = 2, incrValue = 10, baseTimestamp = 12345678;

    @BeforeClass
    public static void beforeClass() {
        log.info("set count to 0, do warm up myself ...");
        count = 0;
        maxVersions = intMaxVersions;
        HbaseTestBase.beforeClass();
        warmUp();
    }

    private static void warmUp() {

        for (int i = 1; i <= intMaxVersions; i++) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), (long) (baseTimestamp + i), Bytes.toBytes((long) i));
            try {
                table.put(put);
            } catch (IOException e) {
                Assert.fail(e.toString());
            }
        }
    }

    @Test
    public void test() {

        List<Long> timestamps = new ArrayList<>();

        {
            Scan scan = new Scan();
            scan.setMaxVersions(intMaxVersions);

            try {
                ResultScanner scanner = table.getScanner(scan);
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    timestamps.addAll(result.listCells().stream().map(Cell::getTimestamp).collect(Collectors.toList()));
                    for (Cell cell : result.listCells()) {
                        log.info("value is {}", Bytes.toLong(cell.getValueArray(), cell.getValueOffset()));
                    }
                }
            } catch (IOException e) {
                Assert.fail(e.toString());
            }

        }

        Assert.assertEquals(intMaxVersions, timestamps.size());

        for (long timestamp : timestamps) {
            log.info(" timestamp : {}", timestamp);
        }

        long min = Collections.min(timestamps);

        try {
            Increment incr = new Increment(Bytes.toBytes(rowKey));
            incr.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), incrValue);
            incr.setTimeRange(min, min + 1);
            Result result = table.increment(incr);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            Assert.assertEquals(1, cells.size());
            Cell cell = cells.get(0);
            log.info("{}", cell);
            long l = Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
            Assert.assertEquals(incrValue + 1, l);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }
}
