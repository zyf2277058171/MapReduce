package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.PrintStream;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    /**
     * Map是我们MapTask的核心逻辑
     *
     * @param key
     * @param value
     * @param context 任务本身
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.切分成单词
        String[] words = value.toString().split(" ");

        //2.将单词以（单词，1）的形式输出
        for (String word : words) {
            this.word.set(word);
            context.write(this.word, this.one);
        }


    }
}
