package com.tan.hbase.dml;

import com.tan.hbase.HBaseConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.Arrays;
import java.util.Collections;

public class PutDML {

    public static Connection connection = HBaseConnection.connection;

    public static void main(String[] args) throws Exception {
        Table table = connection.getTable(TableName.valueOf("TEST:test_create_table"));
        Put put = new Put("1002".getBytes());
        put.addColumn("info".getBytes(), "sex".getBytes(), "man".getBytes());
        table.put(Collections.singletonList(put));
        table.close();
    }

}
