package com.atguigu.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComparableMapper extends Mapper<LongWritable,Text, FlowBean,Text> {

    private FlowBean flow = new FlowBean();
    private Text phone = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        phone.set(split[0]);

        long upflow = Long.parseLong(split[1]);
        long downflow = Long.parseLong(split[2]);

        flow.set(upflow,downflow);

        context.write(flow,phone);
    }
}
