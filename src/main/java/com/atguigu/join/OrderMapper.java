package com.atguigu.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.PrimitiveIterator;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        if (filename == "order.txt") {
            orderBean.setOrderId(split[0]);
            orderBean.setPid(split[1]);
            orderBean.setAmount(Integer.parseInt(split[2]));
            orderBean.setPname("");
        } else {
            orderBean.setOrderId("");
            orderBean.setPid(split[0]);
            orderBean.setAmount(0);
            orderBean.setPname(split[1]);
        }
        context.write(orderBean, NullWritable.get());

    }
}
