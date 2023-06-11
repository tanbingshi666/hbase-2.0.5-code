package com.tan.hbase.dql;

import com.tan.hbase.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetDQL {

    public static Connection connection = HBaseConnection.connection;

    public static void main(String[] args) throws Exception {
        Table table = connection.getTable(TableName.valueOf("TEST:test_create_table"));

        Get get = new Get(Bytes.toBytes("1001"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.readAllVersions();

        try {
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

}
