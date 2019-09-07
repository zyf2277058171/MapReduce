package com.atguigu.oneindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    private FileSplit fs;
    private String name;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fs = (FileSplit) context.getInputSplit();
        name = fs.getPath().getName();

        String[] split = value.toString().split(" ");
        for (String s : split) {
            word.set(s + "\t" + name);
            context.write(this.word,one);
        }

    }
}




