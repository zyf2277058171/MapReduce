package com.atguigu.join;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private String pid;
    private int amount;
    private String pname;

    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" + amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public int compareTo(OrderBean o) {
        int compare = o.pid.compareTo(this.pid);
        if(compare == 0){
            return o.pname.compareTo(this.pname);
        }else{
            return compare;
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
    }
}
