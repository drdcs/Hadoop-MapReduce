package com.hadoop.Players;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMaxMapper extends Mapper <LongWritable, Text, Text, PlayerDetails> {

    private PlayerDetails playerDetails = new PlayerDetails();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] player = value.toString().split(",");
        playerDetails.setPlayerName( new Text(player[0]));
        playerDetails.setScore(new IntWritable(Integer.parseInt(player[1])));
        playerDetails.setOpposition(new Text(player[2]));
        playerDetails.setTimestamps(new LongWritable(Long.parseLong(player[3])));
        playerDetails.setBallsTaken(new IntWritable(Integer.parseInt(player[4])));
        playerDetails.setFours(new IntWritable(Integer.parseInt(player[5])));
        playerDetails.setSix(new IntWritable(Integer.parseInt(player[6])));

        context.write(playerDetails.getPlayerName(), playerDetails);
    }

}
