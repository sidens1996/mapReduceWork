package com.csu.mr.findcommon2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: CustomOutputFormatStep2
 * @Description: TODO
 * @Author: Achilles
 * @Date: 14/10/2019  19:44
 * @Version: 1.0
 **/

public class CustomOutputFormatStep2 extends FileOutputFormat<KeyBean, Text> {

    @Override
    public RecordWriter<KeyBean, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        return new CustomRecordWriter(taskAttemptContext);
    }
}
