package com.ning.hadoop.writablecomparable;

import com.ning.hadoop.partition.MyPartition;
import com.ning.hadoop.partition.PartitionDriver;
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
 * ClassName: WritableComparableDriver
 * Description:
 * date: 2020/12/15 16:40
 *
 * @author ningjianjian
 */
public class WritableComparableDriver {
    public static void main(String[] args) {
        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\phone01_op\\";
        String outputPath = "C:\\Users\\ningjianjian\\Desktop\\wc\\phone01_op2\\";
        args = new String[]{inputPath, outputPath};

        doHadoop(args);
    }

    private static void doHadoop(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(WritableComparableDriver.class);

            job.setMapperClass(WritableComparableMapper.class);
            job.setReducerClass(WritableComparableReducer.class);

            job.setMapOutputKeyClass(PhoneMetaV2Bean.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PhoneMetaBean.class);

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
