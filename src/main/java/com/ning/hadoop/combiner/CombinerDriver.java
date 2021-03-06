package com.ning.hadoop.combiner;

import com.ning.hadoop.wc01.WordCountDriver;
import com.ning.hadoop.wc01.WordCountMapper;
import com.ning.hadoop.wc01.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: CombinerDriver
 * Description:
 * date: 2020/12/15 17:01
 *
 * @author ningjianjian
 */
public class CombinerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\in\\combiner.txt";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\combiner_op\\";

        args = new String[]{inputPath, outputPath};

        doHadoop(args);

    }

    private static void doHadoop(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(CombinerDriver.class);

        // 设置汇总combiner
        job.setCombinerClass(WcCombiner.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 7 hadoop输出目录不能是已存在，如果已存在，则删除
        Path output = new Path(args[1]);
        if (output.getFileSystem(configuration).exists(output)) {
            output.getFileSystem(configuration).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);

        // 8 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
