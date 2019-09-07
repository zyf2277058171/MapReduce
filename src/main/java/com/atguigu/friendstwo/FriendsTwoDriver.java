package com.atguigu.friendstwo;

import com.atguigu.friends.FriendsDriver;
import com.atguigu.friends.FriendsMapper;
import com.atguigu.friends.FriendsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendsTwoDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FriendsTwoDriver.class);
        job.setMapperClass(FriendsTwoMapper.class);
        job.setReducerClass(FriendsTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("a:/input"));
        FileOutputFormat.setOutputPath(job,new Path("a:/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
