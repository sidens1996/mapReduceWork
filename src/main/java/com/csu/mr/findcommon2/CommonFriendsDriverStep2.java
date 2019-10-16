package com.csu.mr.findcommon2;

import com.csu.mr.findcommon.CommonFriendsReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: CommonFriendsDriverStep2
 * @Description: TODO
 * @Author: Achilles
 * @Date: 13/10/2019  21:49
 * @Version: 1.0
 **/

public class CommonFriendsDriverStep2 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //  1 获取配置信息
        Configuration conf = new Configuration();
        // 设置分隔符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"\t");

        // 2 获取job对象
        Job job = Job.getInstance(conf);

        // 设置输入处理类型为KV
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 3 设置jar存储路径
        job.setJarByClass(CommonFriendsDriverStep2.class);

        // 4 设置mapper和reducer类
        job.setMapperClass(CommonFriendsMapperStep2.class);
        job.setReducerClass(CommonFriendsReducerStep2.class);

        // 5 设置mapper输出的key和value
        job.setMapOutputKeyClass(KeyBean.class);
        job.setMapOutputValueClass(Text.class);

        // 6 设置最终输出的key和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输出类
        job.setOutputFormatClass(CustomOutputFormatStep2.class);

        // 7 设置输入输出路径
        args = new String[]{"E:\\code_in_IDEA\\wordcount\\output\\output_common_friends_step1\\part-r-00000"
                ,"E:\\code_in_IDEA\\wordcount\\output\\output_common_friends_step2"};
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 上面这个路径是输出success文件的，在输出文件成功写完后创建

        // 8 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
