package com.csu.mr.index;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: IndexBean
 * @Description: TODO
 * @Author: Achilles
 * @Date: 11/10/2019  16:00
 * @Version: 1.0
 **/

public class IndexBean implements WritableComparable<IndexBean> {

    private String word;
    private String fileName;

    public IndexBean() {
    }

    public IndexBean(String word, String fileame) {
        this.word = word;
        this.fileName = fileame;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public String toString() {
        return word + '\t' +fileName;
    }

    @Override
    public int compareTo(IndexBean bean) {

        // 先按word升序排序，再按fileName降序排序
        int result = 0;

        if (word.compareTo(bean.getWord()) > 0) {
            result = 1;
        } else if (word.compareTo(bean.getWord()) < 0) {
            result = -1;
        }else{
            if (fileName.compareTo(bean.getFileName()) > 0) {
                result = -1;
            } else if (fileName.compareTo(bean.getFileName()) < 0) {
                result = 1;
            }else{
                result = 0;
            }
        }

        return result;
    }

    // 序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(word);
        dataOutput.writeUTF(fileName);
    }

    // 反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {

        word = dataInput.readUTF();
        fileName = dataInput.readUTF();
    }
}
