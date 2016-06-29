package com.mogujie.mst.hbase.filter;

import com.mogujie.mst.HbaseOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by fenqi on 16/6/18.
 */
public class RowFilterTest extends FilterTestBase {
    private static final Logger log = LoggerFactory.getLogger(RowFilterTest.class);

    @Test
    public void testBinaryComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(keyPrefix + "4")));
        scanAndCheck(rowFilter, 1);
    }

    @Test
    public void testBinaryPrefixComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes(keyPrefix + "1")));
        scanAndCheck(rowFilter, 6);
    }

    @Test
    public void testSubstringComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(keyPrefix + "15"));
        scanAndCheck(rowFilter, 0);
    }

}
