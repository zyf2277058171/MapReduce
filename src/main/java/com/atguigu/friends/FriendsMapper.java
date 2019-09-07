package com.atguigu.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendsMapper extends Mapper<LongWritable,Text,Text,Text> {

    private Text k1 = new Text();
    private Text v1 = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");

        v1.set(split[0]);

        String[] arr = split[1].split(",");
        for (String s : arr) {
            k1.set(s);
            context.write(k1,v1);
        }

    }
}
