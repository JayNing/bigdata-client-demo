package com.ning.hadoop.partition;

import com.ning.hadoop.serializable.Phone01Mapper;
import com.ning.hadoop.serializable.Phone01Reducer;
import com.ning.hadoop.serializable.PhoneMetaBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ClassName: PartitionDriver
 * Description:
 * date: 2020/12/15 16:11
 *
 * @author ningjianjian
 */
public class PartitionDriver {
    public static void main(String[] args) {
        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\in\\phone_data.txt";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\phone01_op\\";
        args = new String[]{inputPath, outputPath};

        doHadoop(args);
    }

    private static void doHadoop(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(PartitionDriver.class);

            job.setMapperClass(Phone01Mapper.class);
            job.setReducerClass(Phone01Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PhoneMetaBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PhoneMetaBean.class);

            //设置自定义分片
            job.setPartitionerClass(MyPartition.class);
            job.setNumReduceTasks(5);

            FileInputFormat.setInputPaths(job, new Path(args[0]));

            Path output = new Path(args[1]);
            if (output.getFileSystem(conf).exists(output)){
                output.getFileSystem(conf).delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
