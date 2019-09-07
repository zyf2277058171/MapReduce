package com.atguigu.topn;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Top10Reducer extends Reducer<FlowBean,Text,Text,FlowBean>{

    private Counter pass;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pass = context.getCounter("TOPN", "pass");
    }

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        pass.increment(1);

        if(pass.getValue() <= 10){
            Iterator<Text> iterator = values.iterator();
            Text next = iterator.next();

            context.write(next,key);
        }

    }
}
