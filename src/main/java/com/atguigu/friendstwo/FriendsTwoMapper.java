package com.atguigu.friendstwo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendsTwoMapper extends Mapper<LongWritable,Text,Text,Text> {

    private Text k1 = new Text();
    private Text v1 = new Text();

    private String kk = "";
    private char[] chars;
    private String[] split1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        //设置value值
        v1.set(split[0]);
        //将后面的值拆分
        split1 = split[1].split(",");
        chars = new char[split1.length];
        for (int i = 0; i < split1.length; i++) {
            chars[i] = split1[i].charAt(0);
        }

        Arrays.sort(chars);
        String a = "",b = "";
        //组装key值
        for (int i = 0; i < chars.length; i++) {
            for (int j = 0; j < chars.length; j++) {
                if(chars[i] < chars[j]){
                    a = String.valueOf(chars[i]);
                    b = String.valueOf(chars[j]);
                    kk = new String(a + "-" + b);
                    k1.set(kk);
                    context.write(k1,v1);
                }
            }
        }

    }
}
