package com.csu.mr.findcommon2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: KeyBean
 * @Description: TODO
 * @Author: Achilles
 * @Date: 14/10/2019  17:38
 * @Version: 1.0
 **/

public class KeyBean implements WritableComparable<KeyBean> {

    String first;
    String icon = "<->";
    String second;

    public KeyBean(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public KeyBean() {
    }

    @Override
    public String toString() {
        return first + icon + second ;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    @Override
    public int compareTo(KeyBean bean) {
        // 进行比较
        int result = 0;
        if (first.compareTo(bean.getFirst()) > 0) {//A<B
            result = 1;//升序
        } else if (first.compareTo(bean.getFirst()) < 0) {
            result = -1;//降序
        }else {
            if (second.compareTo(bean.getSecond()) > 0) {
                result = 1;
            } else if (second.compareTo(bean.getSecond()) < 0) {
                result = -1;
            } else {
                result = 0;
            }
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(first);
        dataOutput.writeUTF(icon);
        dataOutput.writeUTF(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        first = dataInput.readUTF();
        icon = dataInput.readUTF();
        second = dataInput.readUTF();
    }
}
