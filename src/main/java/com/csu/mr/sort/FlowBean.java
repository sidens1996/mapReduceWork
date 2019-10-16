package com.csu.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: FlowBean
 * @Description: 实现writablecomparbale接口
 * @Author: Achilles
 * @Date: 08/10/2019  16:23
 * @Version: 1.0
 **/

public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;// 上行流量
    private long downFlow;// 下行流量
    private long sumFlow;// 总流量

    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow, long sumFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return  upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    // 比较
    @Override
    public int compareTo(FlowBean bean) {

        // 核心比较条件判断
        // 总流量大小倒序排列
        int result = 0;
        if (sumFlow > bean.getSumFlow()){
            result = -1;
        } else if (sumFlow < bean.getSumFlow()) {
            result = 1;
        }
        return result;
    }

    // 序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    // 反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }
}
