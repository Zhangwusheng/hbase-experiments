package com.mogujie.mst.hbase.mapreduce;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by fenqi on 16/7/22.
 * To get table count, just run
 * 'hbase org.apache.hadoop.hbase.mapreduce.RowCounter <tableName>' in commandLine
 * this example should how to achieve that by writing code ourselves
 */
public class RowCounterTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(RowCounterTest.class);
    private static int rowCount = 100;

    @BeforeClass
    public static void beforeClass() {
        log.info("change qualifierGenerator ...");
        zookeeperURI = "localhost";
        count = rowCount;
        HbaseTestBase.beforeClass();
    }

    @Test
    public void testRowCounter() {
        Job job = null;
        try {
            job = RowCounter.createSubmittableJob(operator.getConfig(), new String[]{tableName});
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
        if (job == null) {
            Assert.fail();
        }

        try {
            boolean success = job.waitForCompletion(true);
            Assert.assertTrue(success);
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        try {
            Counters counters = job.getCounters();
            log.info("get counters: \n{}", counters.toString());
            GenericCounter genericCounter = (GenericCounter) counters.findCounter(
                    "org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters",
                    "ROWS");
            Assert.assertEquals(rowCount, genericCounter.getValue());
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }
}
