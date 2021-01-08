package com.ning.project.collectmarkush;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: WordCountMapper
 * Description:
 * date: 2020/12/10 16:01
 *
 * @author ningjianjian
 */
public class MoleculeTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text redKey = new Text();
    private IntWritable redValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(" ");
        String moleculeType = values[6];
        String registryNumer = moleculeType.split("_")[0];
        redKey.set(registryNumer);
        redValue.set(1);
        context.write(redKey,redValue);
    }
}
