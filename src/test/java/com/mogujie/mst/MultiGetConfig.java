package com.mogujie.mst;

/**
 * Created by fenqi on 16/6/21.
 */
public class MultiGetConfig {
    private String zookeeperURI;
    private String keysFile;
    private int threadNum;
    private boolean needInsert;
    private String tableName;
    private String columnFamily;
    private String qualifier;

    public boolean isNeedInsert() {
        return needInsert;
    }

    public void setNeedInsert(boolean needInsert) {
        this.needInsert = needInsert;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public String getZookeeperURI() {
        return zookeeperURI;
    }

    public void setZookeeperURI(String zookeeperURI) {
        this.zookeeperURI = zookeeperURI;
    }

    public String getKeysFile() {
        return keysFile;
    }

    public void setKeysFile(String keysFile) {
        this.keysFile = keysFile;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }
}
