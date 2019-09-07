package com.atguigu.twoindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text,Text,Text,Text> {

    private Text number = new Text();
    private String namenumber = "";


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            namenumber += value.toString() + " ";
        }
        number.set(namenumber);
        context.write(key,number);
        namenumber= "";
    }
}
