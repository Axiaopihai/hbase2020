package com.myself.hbase.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.List;

/**
 * @author zxq
 * 2020/5/28
 */
public class MrHbaseMapper extends TableMapper<NullWritable,Put> {

    private String family;
    private String column;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        family = configuration.get("family");
        column = configuration.get("column");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        List<Cell> columnCells = value.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(column));
        for (Cell cell : columnCells) {
            //byte[] row = cell.getRow();
            Put put = new Put(key.get());
            put.add(cell);
            context.write(NullWritable.get(),put);
        }
    }
}
