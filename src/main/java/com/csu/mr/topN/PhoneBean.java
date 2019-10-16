package com.csu.mr.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: PhoneBean
 * @Description: TODO
 * @Author: Achilles
 * @Date: 12/10/2019  17:49
 * @Version: 1.0
 **/

public class PhoneBean implements WritableComparable<PhoneBean> {

    String phoneNum;
    Long upFlow;
    Long downFlow;
    Long sumFlow;

    public PhoneBean() {
    }

    public PhoneBean(String phoneNum, Long upFlow, Long downFlow, Long sumFlow) {
        this.phoneNum = phoneNum;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return phoneNum + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(PhoneBean bean) {

        // 按总流量进行降序排序
        int result = 0;

        if (sumFlow > bean.getSumFlow()) {
            result = -1;
        } else if (sumFlow < bean.getSumFlow()) {
            result = 1;
        }else{
            result = 0;
        }

        return result;
    }

    // 序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(phoneNum);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    // 反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {

        phoneNum = dataInput.readUTF();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

}
