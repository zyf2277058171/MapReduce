package com.atguigu.friendstwo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendsTwoReducer extends Reducer<Text,Text,Text,Text> {

    private String vv = "";
    private Text v1 = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            vv += value + " ";
        }
        v1.set(vv);
        context.write(key,v1);
        vv = "";
    }
}
