package com.ning.hadoop.wc01;

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
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text redKey = new Text();
    private IntWritable redValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        if (words.length > 0){
            for (String word : words) {
                if (StringUtils.isNotBlank(word)){
                    redKey.set(word);
                    redValue.set(1);
                    context.write(redKey,redValue);
                }
            }
        }
    }
}
