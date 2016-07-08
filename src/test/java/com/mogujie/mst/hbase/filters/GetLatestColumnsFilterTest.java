package com.mogujie.mst.hbase.filters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by fenqi on 16/7/5.
 */
public class GetLatestColumnsFilterTest extends FilterTestBase {
    private static final Logger log = LoggerFactory.getLogger(GetLatestColumnsFilterTest.class);
    private static final String strRowKey = "r", strQualifier = "q", dependentColumn = "dependentColumn";
    private static final Set<Long> timestampsSet = new HashSet<>();

    @BeforeClass
    public static void beforeClass() {
        log.info("set count to 0, do warm up myself ...");
        count = 0;
        FilterTestBase.beforeClass();
        warmUp();
    }

    public static void warmUp() {
        try {
            int j = 0;

            {
                Put put = new Put(Bytes.toBytes(strRowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(dependentColumn), Bytes.toBytes(dependentColumn));

                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(strQualifier + j), Bytes.toBytes(strQualifier + j));
                j++;
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(strQualifier + j), Bytes.toBytes(strQualifier + j));
                j++;
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(strQualifier + j), Bytes.toBytes(strQualifier + j));

                table.put(put);
            }

            {
                j = 100;
                Put put = new Put(Bytes.toBytes(strRowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(dependentColumn), Bytes.toBytes(dependentColumn));

                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(strQualifier + j), Bytes.toBytes(strQualifier + j));
                j++;
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(strQualifier + j), Bytes.toBytes(strQualifier + j));
                j++;
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(strQualifier + j), Bytes.toBytes(strQualifier + j));

                table.put(put);
            }

            {
                Get get = new Get(Bytes.toBytes(strRowKey));
                Result result = table.get(get);
                log.info(result.toString());

                timestampsSet.addAll(result.listCells().stream().map(Cell::getTimestamp).collect(Collectors.toList()));

                NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = result.getNoVersionMap();
                for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> row : resultMap.entrySet()) {
                    log.info("row is {}", new String(row.getKey()));
                    for (Map.Entry<byte[], byte[]> column : row.getValue().entrySet()) {
                        log.info("column is {}, value is {}", new String(column.getKey()), new String(column.getValue()));
                    }
                }
            }
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

    }

    @Test
    public void testDependentColumnFilter() {
        DependentColumnFilter dependentColumnFilter = new DependentColumnFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(dependentColumn));
        scanAndCheck(dependentColumnFilter, 1, strRowKey, strRowKey, 7, null);
    }

    @Test
    public void testTimestampsFilter() {
        List<Long> timestamps = new ArrayList<>();
        timestamps.addAll(timestampsSet);
        TimestampsFilter timestampsFilter = new TimestampsFilter(timestamps);
        scanAndCheck(timestampsFilter, 1, strRowKey, strRowKey, 7, null);
    }

    @Test
    public void testGetLatestColumnsFilter() {
        GetLatestColumnsFilter getLatestColumnsFilter = new GetLatestColumnsFilter();
        scanAndCheck(getLatestColumnsFilter, 1, strRowKey, strRowKey, 4, null);
    }


}
