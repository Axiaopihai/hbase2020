package com.myself.hbase.option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/22
 */
public class HbaseDDLOption {

    private static Admin admin;
    private static Connection connection;

    public static void main(String[] args) throws IOException {
        HbaseDDLOption hbaseDDLOption = new HbaseDDLOption();
        //获取配置文件
        Configuration conf = hbaseDDLOption.getConf();

        //操作表
        //获取DDL操作工具
        connection = ConnectionFactory.createConnection(conf);
        admin=connection.getAdmin();
        //创建命名空间
        hbaseDDLOption.createNamespace();
        //创建表
        hbaseDDLOption.createTable("student","baseInfor","scoreInfor");
        //修改表的元信息
        hbaseDDLOption.modifyTable("student","addrInfor");
        //关闭资源
        hbaseDDLOption.doClose();
    }

    public Configuration getConf() {
        //获取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop128,hadoop129,hadoop130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }

    public void createTable(String tableName,String... columnFamilies ) throws IOException {
        //判断表是否存在
        TableName name = TableName.valueOf(tableName);
        boolean exists = admin.tableExists(name);
        if (exists) {
            System.out.println("表已经存在,覆盖原表。。。");
            //如果存在删除原表
            admin.disableTable(name);
            admin.deleteTable(name);
        }
        //创建表
        HTableDescriptor student = new HTableDescriptor(name);
        for (String columnFamily : columnFamilies) {
            student.addFamily(new HColumnDescriptor(columnFamily).setCompressionType(Compression.Algorithm.NONE));
        }
        admin.createTable(student);
    }

    public void modifyTable(String tableName,String... columnFamilies) throws IOException {
        TableName name = TableName.valueOf(tableName);
        //添加新的列族
        for (String columnFamily : columnFamilies) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            admin.addColumn(name,hColumnDescriptor);
        }
        //修改原有的列族
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
        HColumnDescriptor[] families = tableDescriptor.getColumnFamilies();
        for (HColumnDescriptor family : families) {
            family.setMaxVersions(2);
            tableDescriptor.modifyFamily(family);
        }
        admin.modifyTable(name,tableDescriptor);
    }

    //namespace
    public void createNamespace() throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("banji").build();
        admin.createNamespace(namespaceDescriptor);
    }



    public void doClose() throws IOException {
        admin.close();
        connection.close();
    }
}
