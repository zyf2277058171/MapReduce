package com.atguigu.topn;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Top10Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Top10Driver.class);
        job.setMapperClass(Top10Mapper.class);
        job.setReducerClass(Top10Reducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path("a:/input"));
        FileOutputFormat.setOutputPath(job,new Path("a:/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
