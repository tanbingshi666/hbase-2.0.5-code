package com.tan.hbase.dql;

import com.tan.hbase.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanDQL {

    public static Connection connection = HBaseConnection.connection;

    public static void main(String[] args) throws Exception {

        Table table = connection.getTable(TableName.valueOf("TEST:test_create_table"));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("1001"));
        scan.withStopRow(Bytes.toBytes("1003"));

        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new
                            String(CellUtil.cloneRow(cell)) + "\t" + new
                            String(CellUtil.cloneFamily(cell)) + "\t" + new
                            String(CellUtil.cloneQualifier(cell)) + "\t" + new
                            String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

}
