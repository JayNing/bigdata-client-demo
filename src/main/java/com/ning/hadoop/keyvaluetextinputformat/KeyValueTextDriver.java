package com.ning.hadoop.keyvaluetextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: KeyValueTextDriver
 * Description:
 * date: 2020/12/14 16:41
 *
 * @author ningjianjian
 */
public class KeyValueTextDriver {
    public static void main(String[] args) throws Exception {
        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\in\\kvtext.txt";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\kvtext_op\\";

        args = new String[]{inputPath, outputPath};
        doHadoop(args);
    }

    private static void doHadoop(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        Job job = Job.getInstance(conf);

        job.setJarByClass(KeyValueTextDriver.class);

        job.setMapperClass(KeyValueTextMapper.class);
        job.setReducerClass(KeyValueTextReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 6 设置输出数据路径
        Path path = new Path(args[1]);
        if (path.getFileSystem(conf).exists(path)){
            path.getFileSystem(conf).delete(path,true);
        }

        FileOutputFormat.setOutputPath(job, path);

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
