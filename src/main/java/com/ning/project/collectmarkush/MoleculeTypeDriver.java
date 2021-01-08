package com.ning.project.collectmarkush;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: MoleculeTypeDriver
 * Description:
 * date: 2021/01/08 16:10
 * 统计markush文件中的MoleculeType类型
 * @author ningjianjian
 */
public class MoleculeTypeDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputPath = "D:\\Project\\test\\CAS\\long_text_2021-01-08-17-55-53.txt";
        String outputPath = "D:\\Project\\test\\CAS\\noregistry\\";

        args = new String[]{inputPath, outputPath};

       doHadoop(args);

    }

    private static void doHadoop(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(MoleculeTypeDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(MoleculeTypeMapper.class);
        job.setReducerClass(MoleculeTypeReducer.class);

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
