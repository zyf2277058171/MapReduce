package com.atguigu.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class FlowDrive {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job实例
        Job job = Job.getInstance(new Configuration(), "phoneFlow");
        //2.设置类路径
        job.setJarByClass(FlowDrive.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //输入，输出文件的位置
        FileInputFormat.setInputPaths(job,new Path("A:/input"));
        FileOutputFormat.setOutputPath(job,new Path("A:/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
