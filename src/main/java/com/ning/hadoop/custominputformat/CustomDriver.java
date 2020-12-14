package com.ning.hadoop.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * ClassName: CustomDriver
 * Description:
 * date: 2020/12/14 19:10
 *
 * @author ningjianjian
 */
public class CustomDriver {
    public static void main(String[] args) throws Exception {

        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\in\\custominputformat\\";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\custom_op\\";

        args = new String[]{inputPath, outputPath};

        doHadoop(args);
    }

    private static void doHadoop(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomDriver.class);

        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        //设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path output = new Path(args[1]);
        if (output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
