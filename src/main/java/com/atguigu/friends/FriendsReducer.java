package com.atguigu.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendsReducer extends Reducer<Text,Text,Text,Text> {

    private Text v1 = new Text();

    private String s1 = "";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            s1 += value.toString() + ",";
        }
        v1.set(s1);
        context.write(key,v1);
        s1 = "";
    }
}
