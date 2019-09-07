package com.atguigu.topn;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top10Mapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    private FlowBean flowBean = new FlowBean();
    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        flowBean.setUpFlow(Long.parseLong(split[1]));
        flowBean.setDownFlow(Long.parseLong(split[2]));
        flowBean.setSumFlow(Long.parseLong(split[3]));

        phone.set(split[0].toString());

        context.write(flowBean,phone);
    }
}
