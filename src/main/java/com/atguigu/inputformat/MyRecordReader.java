package com.atguigu.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<Text, BytesWritable> {

    //标记我们当前文件是否读完了
    private boolean isRead = false;

    //Key Value成员变量
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    //文件流相关
    private FSDataInputStream inputStream;
    private FileSplit fs;

    /**
     * 初始化方法，框架在数据使用前调用一次
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        //通过切片获取文件的路径
        fs  = (FileSplit) split;
        Path path = fs.getPath();

        Configuration configuration = context.getConfiguration();

        //获取文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        //开流
        inputStream = fileSystem.open(path);

    }

    /**
     * 尝试读取下一组KV
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isRead) {
            return false;
        } else {

            //读这个文件的内容
            //key
            Path path = fs.getPath();
            key.set(path.toString());

            //value
            byte[] bytes = new byte[(int) fs.getLength()];
            int read = inputStream.read(bytes);

            value.set(bytes, 0, read);

            isRead = true;
            return true;
        }
    }

    /**
     * 获取当年读到的K
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前读到的V
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 进度
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    public void close() throws IOException {
        //关流
        IOUtils.closeStream(inputStream);
    }
}
