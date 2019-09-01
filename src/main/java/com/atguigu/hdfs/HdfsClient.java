package com.atguigu.hdfs;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import javax.sound.midi.Soundbank;
import java.io.File;
import java.io.IOException;
import java.net.URI;

public class HdfsClient {

    private FileSystem fileSystem;
    @Before
    public void before() throws IOException, InterruptedException {
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop104:9000"), new Configuration(), "atguigu");
    }
    @After
    public void after() throws IOException {
        fileSystem.close();
    }

    @Test
    public void get() throws IOException {
        //1.下载
        fileSystem.copyToLocalFile(new Path("/123.txt"),new Path("d:/"));
    }
    @Test
    public void put() throws IOException {
        //2.上传
        fileSystem.copyFromLocalFile(new Path("d:/InstallConfig.ini"),new Path("/"));
    }
    @Test
    public void delete() throws IOException {
        //3.删除
        boolean delete = fileSystem.delete(new Path("/input"), true);
    }
    @Test
    public void rename() throws IOException {
        //4.更名
        boolean rename = fileSystem.rename(new Path("/123.txt"), new Path("/456.txt"));
        if(rename){
            System.out.println("chenggong");
        }
    }
    @Test
    public void ls() throws IOException {
        //5.查看文件目录
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus.getAccessTime());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath());
        }
    }
}
