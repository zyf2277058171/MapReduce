package com.atguigu.partitioner;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WriterComparablePartitioner extends Partitioner<FlowBean,Text> {
    /**
     * 返回某条数据的具体分区
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(FlowBean flowBean,Text text, int numPartitions) {

        int result = 0;

        //根据手机号不同返回区号
        switch (text.toString().substring(0,3)){
            case "136":
                break;
            case "137":
                result = 1;
                break;
            case "138":
                result = 2;
                break;
            case "139":
                result = 3;
                break;
            default:
                result = 4;
                break;
        }
        return result;
    }
}
