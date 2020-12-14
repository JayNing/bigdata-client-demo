package com.ning.hadoop.nlineinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: NLineDriver
 * Description:
 * date: 2020/12/14 17:16
 *
 * @author ningjianjian
 */
public class NLineDriver {

    public static void main(String[] args) throws Exception {

        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\in\\nline.txt";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\nline_op\\";

        args = new String[]{inputPath, outputPath};
        doHadoop(args);

    }

    private static void doHadoop(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(NLineDriver.class);

        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置每个切片InputSplit中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        //使用NLineInputFormat处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        Path output = new Path(args[1]);
        if (output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output, true);
        }

        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
