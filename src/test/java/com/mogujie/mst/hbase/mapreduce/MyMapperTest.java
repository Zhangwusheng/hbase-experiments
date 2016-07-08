package com.mogujie.mst.hbase.mapreduce;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created by fenqi on 16/7/8.
 * copy from http://sujee.net/2011/04/10/hbase-map-reduce-example/#.V38g6JN95ok
 */
public class MyMapperTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(MyMapperTest.class);
    private static String resultTable = "resultTable";

    @BeforeClass
    public static void beforeClass() {
        log.info("set count to 0, do warm up myself ...");
        count = 0;
        HbaseTestBase.beforeClass();
        warmUp();
    }

    private static void warmUp() {
        String[] pages = {"/", "/a.html", "/b.html", "/c.html"};

        int totalRecords = 10000;
        int maxID = totalRecords / 100;
        Random rand = new Random();
        log.info("importing {} records ....\n", totalRecords);
        for (int i = 0; i < totalRecords; i++) {
            int userID = rand.nextInt(maxID) + 1;
            byte[] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
            String randomPage = pages[rand.nextInt(pages.length)];
            Put put = new Put(rowkey);
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(randomPage));
            try {
                table.put(put);
            } catch (IOException e) {
                Assert.fail(e.toString());
            }
        }
    }

    @Before
    public void before() {
        log.info("{}");
        try {
            HbaseTestBase.createTable(resultTable, columnFamily, true);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    @After
    public void after() {
        log.info("{}");
        try {
            HbaseTestBase.dropTable(resultTable, false);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    @Test
    public void testMyMapper() {
        Job job = null;
        try {
            job = new Job(operator.getConfig(), "ExampleRead");
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
        job.setJarByClass(MyJob.class);     // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        try {
            TableMapReduceUtil.initTableMapperJob(
                    tableName,                              // input HBase table name
                    scan,                                   // Scan instance to control CF and attribute selection
                    MyJob.MyMapper.class,                   // mapper
                    ImmutableBytesWritable.class,           // mapper output key
                    IntWritable.class,                      // mapper output value
                    job);

            MyJob.MyReducer.columnFamily = columnFamily;
            MyJob.MyReducer.qualifier = qualifier;

            TableMapReduceUtil.initTableReducerJob(
                    resultTable,                              // output HBase table name
                    MyJob.MyReducer.class,                  // reducer
                    job
            );

        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        boolean b = false;
        try {
            b = job.waitForCompletion(true);
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        Assert.assertTrue(b);
    }

}
