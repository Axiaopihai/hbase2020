package com.myself.hbase.option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author zxq
 * 2020/5/25
 */
public class HbaseDMLOption {

    private static Table table;
    private static Connection connection;

    public static void main(String[] args) throws IOException, InterruptedException {
        HbaseDMLOption hbaseDMLOption = new HbaseDMLOption();
        Configuration conf = hbaseDMLOption.getConf();
        //获取DML操作工具
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf("student"));
        //插入单条数据
        hbaseDMLOption.insertSingleData();
        //删除单条数据
        hbaseDMLOption.deleteSingleData();
        //查询单条数据
        String data = hbaseDMLOption.getData();
        System.out.println(data);
        //scan数据
        hbaseDMLOption.scanData();
        //批量操作
        hbaseDMLOption.batchOption();
        //关闭资源
        hbaseDMLOption.doClose();
    }

    public Configuration getConf() {
        //获取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop128,hadoop129,hadoop130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }

    public void insertSingleData() throws IOException {
        Put put = new Put(Bytes.toBytes("1003"));
        byte[] baseInfors = Bytes.toBytes("baseInfor");
        byte[] name = Bytes.toBytes("name");
        byte[] value = Bytes.toBytes("wangwu");
        put.addColumn(baseInfors, name, value);
        table.put(put);
    }

    private void deleteSingleData() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("1003"));
        byte[] baseInfors = Bytes.toBytes("baseInfor");
        byte[] name = Bytes.toBytes("name");
        //删除所有的版本
        delete.addColumns(baseInfors,name);
        //删除最新的版本
        delete.addColumn(baseInfors,name);
        table.delete(delete);
        //不同的delete标记
        //deleteColumn 指定到列，删除所有版本或比指定时间戳小的版本
        //deleted 指定到列，删除最新或指定时间戳版本
        //deleteFamlily rowkey和指定到列族，删除所有版本或比指定时间戳小的版本
    }

    private String getData() throws IOException {
        Get get = new Get(Bytes.toBytes("1002"));
        byte[] baseInfors = Bytes.toBytes("baseInfor");
        byte[] name = Bytes.toBytes("name");
        get.addColumn(baseInfors,name);
        Result result = table.get(get);
        byte[] value = result.getValue(baseInfors, name);
        return Bytes.toString(value);
    }

    private void scanData() throws IOException {
        Scan scan = new Scan();
        byte[] baseInfors = Bytes.toBytes("baseInfor");
        byte[] name = Bytes.toBytes("name");
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            System.out.println(Bytes.toString(result.getValue(baseInfors,name)));
        }
    }

    private void batchOption() throws IOException, InterruptedException {
        ArrayList<Row> rows = new ArrayList<Row>();
        for (int i = 0; i < 5; i++) {
            Put put = new Put(Bytes.toBytes("1002"+i));
            byte[] baseInfors = Bytes.toBytes("baseInfor");
            byte[] name = Bytes.toBytes("name");
            byte[] value = Bytes.toBytes("zhangsan"+i);
            put.addColumn(baseInfors,name,value);
            rows.add(put);
        }
        table.batch(rows,new Object[rows.size()]);
    }

    private void doClose() throws IOException {
        table.close();
        connection.close();
    }


}
