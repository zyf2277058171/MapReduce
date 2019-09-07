package com.atguigu.flow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
//                                  偏移量，字符串，字符串，FlowBean对象
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    //输出的key值
    private Text phone = new Text();
    //输出的value值
    private FlowBean flowBean = new FlowBean();

    @Override         //偏移量，输入的字符串，上下文
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将一行字符串用“\t”切分，并将切成的块放进数组
        String[] fields = value.toString().split("\t");
        for (String field : fields) {
            System.out.println(field);
        }
        
        
        //2.取数组中下标为1的手机号，设置为phone值
        phone.set(fields[1]);//下标为1的是

        //3.将string型转为long型，将属性设置进flowBean对象
        long up = Long.parseLong(fields[fields.length - 3]);
        long down = Long.parseLong(fields[fields.length-2]);

        flowBean.set(up,down);
        //4.将k-v对写入上下文
        context.write(phone,flowBean);


    }
}
