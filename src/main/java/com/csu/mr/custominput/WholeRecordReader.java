package com.csu.mr.custominput;

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

/**
 * @ClassName: WholeRecordReader
 * @Description: TODO
 * @Author: Achilles
 * @Date: 28/09/2019  15:54
 * @Version: 1.0
 **/

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private Configuration configuration;
    private FileSplit split;

    private boolean isProgress = true;
    private BytesWritable value = new BytesWritable();
    private Text k = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgress) {

            // 1 定义缓存区
            byte[] contents = new byte[(int) split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                // 2 获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);

                // 3 读取数据
                fis = fs.open(path);

                // 4 读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);

                // 5 输出文件内容
                value.set(contents, 0, contents.length);

                // 6 获取文件路径及名称
                String name = split.getPath().getName();

                // 7 设置输出的key值
                k.set(name);
            } catch (Exception e) {
            }finally {
                IOUtils.closeStream(fis);
            }

            isProgress = false;

            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}
