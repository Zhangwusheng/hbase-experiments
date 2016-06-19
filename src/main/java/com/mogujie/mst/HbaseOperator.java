package com.mogujie.mst;

import com.mogujie.mst.commands.HbaseCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by fenqi on 16/5/11.
 */
public class HbaseOperator {
    private static final Logger log = LoggerFactory.getLogger(HbaseOperator.class);

    /** Connection to the cluster. A single connection shared by all application threads. */
    private Connection connection = null;

    protected void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void init(String zookeeperURI) {
        try {
            connection = ConnectionFactory.createConnection(initConfing(zookeeperURI));
        } catch (IOException e) {
            System.out.println("get exception when create conn");
            e.printStackTrace();
            connection = null;
        }
    }

    protected Configuration initConfing(String zookeeperURI) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperURI);

        config.set("ipc.socket.timeout", "500");
        config.set("hbase.rpc.timeout", "1000");
        config.set("hbase.client.retries.number", "1");

        return config;
    }

    protected void execute(String tableName, HbaseCommand command) {
        /** A lightweight handle to a specific table. Used from a single thread. */
        Table table = null;

        try {
            table = connection.getTable(TableName.valueOf(tableName));
            command.execute(table);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }
}
