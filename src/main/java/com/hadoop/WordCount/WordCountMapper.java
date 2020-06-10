package com.hadoop.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text word  = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer result = new StringTokenizer(value.toString());
        while(result.hasMoreTokens()) {
            word.set(result.nextToken());
            context.write(word, ONE);

        }
    }
}
