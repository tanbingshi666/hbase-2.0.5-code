package com.tan.hbase.ddl;

import com.tan.hbase.HBaseConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTableDDL {

    public static Connection connection = HBaseConnection.connection;

    public static void main(String[] args) throws Exception {

        Admin admin = connection.getAdmin();

        // create_namespace 'TEST';
        // list_namespace
        String namespace = "TEST";
        String tableName = "test_create_table";
        String columnFamily = "info";

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(namespace, tableName));

        ColumnFamilyDescriptorBuilder
                columnFamilyDescriptorBuilder =
                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
        columnFamilyDescriptorBuilder.setMaxVersions(5);

        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());

        admin.createTable(tableDescriptorBuilder.build());

        admin.close();
    }

}
