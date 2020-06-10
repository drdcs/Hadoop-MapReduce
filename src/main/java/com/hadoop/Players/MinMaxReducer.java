package com.hadoop.Players;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxReducer extends Reducer<Text, PlayerDetails, Text, PlayerReport> {

    PlayerReport playerReport = new PlayerReport();

    @Override
    protected void reduce(Text key, Iterable<PlayerDetails> values, Context context) throws IOException, InterruptedException {

        playerReport.setPlayerName(key);
        playerReport.setMaxScore(new IntWritable(0));
        playerReport.setMaxScore(new IntWritable(0));

        for(PlayerDetails playerDetails : values) {
            int score =  playerDetails.getScore().get();
            if (score > playerReport.getMaxScore().get()) {
                playerReport.setMaxScore(new IntWritable(score));
                playerReport.setMaxScoreopposition(playerDetails.getOpposition());
            }

            if (score < playerReport.getMaxScore().get()) {
                playerReport.setMinScore(new IntWritable(score));
                playerReport.setMinScoreopposition(playerDetails.getOpposition());
            }

            context.write(key, playerReport);
        }
    }
}
