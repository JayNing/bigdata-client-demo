package com.ning.hadoop.serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ClassName: Phone01Driver
 * Description:
 * date: 2020/12/10 16:47
 *
 * @author ningjianjian
 */
public class Phone01Driver {

    public static void main(String[] args) throws Exception{

        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\in\\phone_data.txt";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\phone01_op\\";
        args = new String[]{inputPath, outputPath};

        doHadoop(args);
    }

    private static void doHadoop(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(Phone01Driver.class);

        job.setMapperClass(Phone01Mapper.class);
        job.setReducerClass(Phone01Reducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(PhoneMetaBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneMetaBean.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

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
