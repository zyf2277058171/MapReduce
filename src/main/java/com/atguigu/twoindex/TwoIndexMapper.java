package com.atguigu.twoindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable,Text,Text,Text> {

    private Text word = new Text();
    private Text number = new Text();
    private String filenumber;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        word.set(split[0]);
        number.set(split[1] + "\t" + split[2]);
        context.write(word,number);
    }
}
