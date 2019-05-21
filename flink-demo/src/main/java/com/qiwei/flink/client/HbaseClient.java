package com.qiwei.flink.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;

import static com.qiwei.flink.constant.HBaseConstant.HBASE_ZOOKEEPER_QUORUM;

/**
 * @author qiwei
 * @description HbaseClient
 * @date 2019/5/21 21:31
 */
public class HbaseClient implements Serializable {

    private Connection connect;

    private static HbaseClient INSTANCE;

    public static HbaseClient getInstance(org.apache.flink.configuration.Configuration parameters) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_ZOOKEEPER_QUORUM, "192.168.1.6");
        INSTANCE = new HbaseClient(ConnectionFactory.createConnection(configuration));
        return INSTANCE;
    }

    private HbaseClient(Connection connect) {
        this.connect = connect;
    }

    public void close() throws IOException {
        connect.close();
    }

    public String getStringByKey(Table table, String rowkey, String cf, String qualifier) throws IOException {
        Result result = table.get(new Get(Bytes.toBytes(rowkey)));
        return Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier)));
    }

    public Table getTable(TableName tableName) throws IOException {
        return connect.getTable(tableName);
    }
}
