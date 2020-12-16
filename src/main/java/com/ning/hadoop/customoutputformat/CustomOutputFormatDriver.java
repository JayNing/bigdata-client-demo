package com.ning.hadoop.customoutputformat;

import com.ning.hadoop.custominputformat.CustomDriver;
import com.ning.hadoop.custominputformat.CustomMapper;
import com.ning.hadoop.custominputformat.CustomReducer;
import com.ning.hadoop.custominputformat.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * ClassName: CustomOutputFormatDriver
 * Description:
 * date: 2020/12/16 22:02
 *
 * @author ningjianjian
 */
public class CustomOutputFormatDriver {
    public static void main(String[] args) throws Exception {

        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\in\\customoutputformat_log.txt";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\customoutputformat_op\\";

        args = new String[]{inputPath, outputPath};

        doHadoop(args);
    }

    private static void doHadoop(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomOutputFormatDriver.class);

        job.setMapperClass(CustomOutputFormatMapper.class);
        job.setReducerClass(CustomOutputFormatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置输出的outputFormat
        job.setOutputFormatClass(CustomFileOutputFormat.class);

        Path output = new Path(args[1]);
        if (output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
