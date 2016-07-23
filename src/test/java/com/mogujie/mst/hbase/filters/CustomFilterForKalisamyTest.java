package com.mogujie.mst.hbase.filters;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by fenqi on 16/7/23.
 */
public class CustomFilterForKalisamyTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(RowPrefixFilterTest.class);
    private static String dependentQualifier = "dependentQualifier";

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

            put = new Put(Bytes.toBytes("ROW1"));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(qualifier));
            table.put(put);

            put = new Put(Bytes.toBytes("ROW2"));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(qualifier));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(dependentQualifier), Bytes.toBytes(dependentQualifier));
            table.put(put);

        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    @Test
    public void testCustomFilterForKalisamy() {
        CustomFilterForKalisamy customFilterForKalisamy = new CustomFilterForKalisamy(
                CompareFilter.CompareOp.EQUAL,
//                new BinaryComparator(Bytes.toBytes(qualifier))
                new BinaryComparator(Bytes.toBytes(dependentQualifier))
        );
        scanAndCheck(customFilterForKalisamy, 2);
    }

    @Test
    public void testValueFilter() {
        ValueFilter valueFilter = new ValueFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(qualifier))
        );
        scanAndCheck(valueFilter, 2);
    }
}
