package com.atguigu.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ComparableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        //1.设置3个类路径
        job.setJarByClass(ComparableDriver.class);
        job.setMapperClass(ComparableMapper.class);
        job.setReducerClass(ComparableReducer.class);

        //2.设置k-v类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //3.设置分区，输入输出文件格式，inputformat
        job.setPartitionerClass(WriterComparablePartitioner.class);
        job.setNumReduceTasks(5);

        //3.设置输入输出文件路径
        FileInputFormat.setInputPaths(job,new Path("a:/input"));
        FileOutputFormat.setOutputPath(job,new Path("a:/output"));

        //4.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 : 1);

    }
}
