package com.mogujie.mst.hbase.coprocessor.endpoint;

import com.mogujie.mst.hbase.HbaseTestBase;
import com.mogujie.mst.hbase.proto.LineCounterServer;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by fenqi on 16/7/8.
 * copy from http://paddy-w.iteye.com/blog/2191505
 */
public class LineCounterEndPointTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(LineCounterEndPointTest.class);

    @BeforeClass
    public static void beforeClass() {
        log.info("set count to 0, do warm up myself ...");
        count = 0;
        HbaseTestBase.beforeClass();
    }

    @Test
    public void testLineCounterEndPoint() {
        final LineCounterServer.CountRequest req = LineCounterServer.CountRequest.newBuilder().setAskWord("count").build();
        Map<byte[], Long> tmpRet = null;
        try {
            tmpRet = table.coprocessorService(LineCounterServer.LineCounter.class, null, null, instance -> {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<LineCounterServer.CountResponse> rpc = new BlockingRpcCallback<>();
                instance.countLine(controller, req, rpc);
                LineCounterServer.CountResponse resp = rpc.get();
                return resp.getRetWord();
            });
        } catch (Throwable throwable) {
            if (throwable.getClass().equals(AssertionError.class)) {
                throw new AssertionError(throwable);
            } else {
                log.info("{}", throwable);
                Assert.fail(throwable.toString());
            }
        }

        long ret = 0;
        for (long l : tmpRet.values()) {
            ret += l;
        }
        log.info("lines: {}", ret);
    }

    @Test
    public void testLineCounterEndPoint2() {
        final LineCounterServer.CountRequest req = LineCounterServer.CountRequest.newBuilder().setAskWord("count").build();
        final AtomicLong ret = new AtomicLong();
        try {
            table.coprocessorService(LineCounterServer.LineCounter.class, null, null, instance -> {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<LineCounterServer.CountResponse> rpc = new BlockingRpcCallback<>();
                instance.countLine(controller, req, rpc);
                LineCounterServer.CountResponse resp = rpc.get();
                Assert.assertNotNull(resp);
                return resp.getRetWord();
            }, (region, row, result) -> {
                ret.getAndAdd(result);
                log.info("{}: {}", Bytes.toString(row), result);
            });
        } catch (Throwable throwable) {
            if (throwable.getClass().equals(AssertionError.class)) {
                throw new AssertionError(throwable);
            } else {
                log.info("{}", throwable);
                Assert.fail(throwable.toString());
            }
        }
        log.info("lines: {}", ret.get());
    }
}
