package com.mogujie.mst;

import com.mogujie.mst.commands.HbaseCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by fenqi on 16/5/11.
 */
public class HbaseOperator {
    private static final Logger log = LoggerFactory.getLogger(HbaseOperator.class);

    /** Connection to the cluster. A single connection shared by all application threads. */
    private Connection connection = null;

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void init(String zookeeperURI) {
        init(zookeeperURI, 5000);
    }

    public void init(String zookeeperURI, int timeout) {
        try {
            connection = ConnectionFactory.createConnection(initConfing(zookeeperURI, timeout));
        } catch (IOException e) {
            System.out.println("get exception when create conn");
            e.printStackTrace();
            connection = null;
        }
    }

    protected Configuration initConfing(String zookeeperURI, int timeout) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperURI);

        config.set("ipc.socket.timeout", String.valueOf(timeout));
        config.set("hbase.rpc.timeout", String.valueOf(timeout));
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

    public static void waitForInput() throws IOException {
        Scanner reader = new Scanner(System.in);  // Reading from System.in
        System.out.println("input something: ");
        System.out.println(reader.next()); // Scans the next token of the input as an int.
        System.out.println("input anything to exit...");
        System.in.read();
    }

    public static void main(String... args) throws IOException {
        waitForInput();
    }

}
