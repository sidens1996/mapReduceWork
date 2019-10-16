package com.csu.mr.findcommon2;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @ClassName: CustomRecordWriter
 * @Description: TODO
 * @Author: Achilles
 * @Date: 14/10/2019  19:55
 * @Version: 1.0
 **/

public class CustomRecordWriter extends RecordWriter<KeyBean, Text> {

    FSDataOutputStream fsDataOutputStream = null;

    public CustomRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {

        // 1 创建文件系统
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());

        // 2 创建输出文件路径
        Path  path = new Path("E:\\code_in_IDEA\\wordcount" +
                "\\output\\output_common_friends_custom_ouput\\commonfriends.txt");

        // 3 创建输出流
        fsDataOutputStream = fileSystem.create(path);
    }

    @Override
    public void write(KeyBean bean, Text text) throws IOException, InterruptedException {

        // 输出到文件
        fsDataOutputStream.write(bean.toString().getBytes());
        fsDataOutputStream.write('\t');
        fsDataOutputStream.write(text.toString().getBytes());
        fsDataOutputStream.write('\n');
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(fsDataOutputStream);
    }
}
