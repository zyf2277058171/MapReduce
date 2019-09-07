package com.atguigu.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //此处进来的已经是按重写分组规则后的规则，pid为一组进来的。
        //values是一个迭代器，key就是一个容器，valuas每迭代一次，这一组对象中的下一个对象就会进到这个key中来
        Iterator<NullWritable> iterator = values.iterator();

        iterator.next();
        //获取第一个对象的名字
        String pname = key.getPname();

        while (iterator.hasNext()){
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());

        }


    }
}
