package com.myself.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author zxq
 * 2020/5/29
 * driver类的另一种写法
 */
public class DriverTool implements Tool {

    private Configuration conf;

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(DriverMain.class);

        //与hadoop的mr对比，由此关联hbase的inputformat和outputformat
        TableMapReduceUtil.initTableMapperJob(
                "student",
                new Scan(),
                MrHbaseMapper.class,
                NullWritable.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "student2",
                MrHbaseReduce.class,
                job
        );

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public void setConf(Configuration configuration) {
        conf=configuration;
        conf.set("family","baseInfor");
        conf.set("column","name");

    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop128,hadoop129,hadoop130");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        ToolRunner.run(conf,new DriverTool(),args);
    }

}
