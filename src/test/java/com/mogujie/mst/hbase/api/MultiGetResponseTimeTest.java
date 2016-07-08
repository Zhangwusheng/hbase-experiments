package com.mogujie.mst.hbase.api;

import com.mogujie.mst.hbase.HbaseOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by fenqi on 16/6/21.
 */
public class MultiGetResponseTimeTest {
    private static final Logger log = LoggerFactory.getLogger(MultiGetResponseTimeTest.class);
    private static HbaseOperator operator = null;
    private static final String MultiGetConfigFile = "config.yml";
    private static int count = 20, threadNum;
    private static MultiGetConfig config = null;
    private static List<String> keys = null;

    private static String tableName, columnFamily , qualifier;

    @BeforeClass
    public static void beforeClass() throws IOException {

        config = new Yaml().loadAs(new FileReader(MultiGetConfigFile), MultiGetConfig.class);

        operator = new HbaseOperator();
        operator.init(config.getZookeeperURI(), config.getTimeout());

        keys = new ArrayList<>();
        File file = new File(config.getKeysFile());
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String line;
        while ((line = br.readLine()) != null) {
            keys.add(line.trim());
        }
        count = keys.size();

        threadNum = config.getThreadNum();
        tableName = config.getTableName();
        columnFamily = config.getColumnFamily();
        qualifier = config.getQualifier();

        if (config.isNeedInsert()) {

            Table table = null;

            try {
                table = operator.getConnection().getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                Assert.fail(e.toString());
            }

            for (int i = 0; i < count; i++) {
                String rowKey = keys.get(i),
                        value = "v" + i;
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                try {
                    table.put(put);
                } catch (IOException e) {
                    Assert.fail(e.toString());
                }
            }
        }
    }

    @AfterClass
    public static void afterClass() {
        if (null != operator)
            operator.close();
    }

    private final class GetTask implements Callable<Result> {
        private String rowKey = null;

        public GetTask(String rowKey) {
            this.rowKey = rowKey;
        }

        @Override
        public Result call() throws Exception {
            Table table = null;

            try {
                table = operator.getConnection().getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                Assert.fail(e.toString());
            }

            long start = System.currentTimeMillis();
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));
            long end = System.currentTimeMillis(), cost = end - start;
            log.info("single get cost time : {}", cost);

            if (null != table) {
                table.close();
            }

            return result;
        }
    }

    @Test
    public void ConcurrentGetResponseTime() throws ExecutionException, InterruptedException, IOException {
        ExecutorService pool = Executors.newFixedThreadPool(threadNum);

        for (int i = 0; i < config.getSampleNum(); i++) {
            runConcurrentGetOnce(pool);
            if (config.isNeedInteract()) {
                log.info("will sleep {} seconds...", config.getWaitTimeSecs());
                Thread.sleep(config.getWaitTimeSecs() * 1000);
            }
        }

        pool.shutdown();
    }

    private void runConcurrentGetOnce(ExecutorService pool) throws ExecutionException, InterruptedException {
        List<Future<Result>> futures = new ArrayList<>(count);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            futures.add(pool.submit(new GetTask(keys.get(i))));
        }

        for (Future<Result> future : futures) {
            Result result = future.get();
            log.debug(result.toString());
        }
        long end = System.currentTimeMillis(), cost = end - start;
        log.info("concurrent get cost time : {}", cost);
    }


    @Test
    public void MultiGetResponseTime() throws IOException, InterruptedException {
        List<Get> gets = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            gets.add(new Get(Bytes.toBytes(keys.get(i))));
        }

        for (int i = 0; i < config.getSampleNum(); i++) {
            runMultiGetOnce(gets);
            if (config.isNeedInteract()) {
                log.info("will sleep {} seconds...", config.getWaitTimeSecs());
                Thread.sleep(config.getWaitTimeSecs() * 1000);
            }
        }
    }

    private void runMultiGetOnce(List<Get> gets) {
        Table table = null;

        try {
            table = operator.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        long start = System.currentTimeMillis();
        try {
            Result[] results = table.get(gets);
            for (Result result : results) {
                log.debug(result.toString());
            }
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        long end = System.currentTimeMillis(), cost = end - start;
        log.info("multiple get cost time : {}", cost);

        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                Assert.fail(e.toString());
            }
        }
    }

}
