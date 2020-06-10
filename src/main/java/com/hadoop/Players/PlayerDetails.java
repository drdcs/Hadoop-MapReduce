package com.hadoop.Players;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*  Input :

    This is a custom writable class being used to save extra effort
    in splitting data at the reducer side and avoiding unnecessary problems that can occur from the delimiter.

    Data Format:

    playerName score opposition timestamps ballsTaken fours six
     'SomeName', 100, 'SomeOpposition', 20200310, 102, 4, 10

 */
public class PlayerDetails implements Writable {

    private Text playerName;
    private IntWritable score;
    private Text opposition;
    private LongWritable timestamps;
    private IntWritable ballsTaken;
    private IntWritable fours;
    private IntWritable six;

    public void readFields(DataInput dataInput) throws IOException {

        playerName.readFields(dataInput);
        score.readFields(dataInput);
        opposition.readFields(dataInput);
        timestamps.readFields(dataInput);
        ballsTaken.readFields(dataInput);
        fours.readFields(dataInput);
        six.readFields(dataInput);

    }
    public void write(DataOutput dataOutput) throws IOException {

        playerName.write(dataOutput);
        score.write(dataOutput);
        opposition.write(dataOutput);
        timestamps.write(dataOutput);
        ballsTaken.write(dataOutput);
        fours.write(dataOutput);
        six.write(dataOutput);
    }

    public Text getPlayerName() {
        return playerName;
    }

    public void setPlayerName(Text playerName) {
        this.playerName = playerName;
    }

    public IntWritable getScore() {
        return score;
    }

    public void setScore(IntWritable score) {
        this.score = score;
    }

    public Text getOpposition() {
        return opposition;
    }

    public void setOpposition(Text opposition) {
        this.opposition = opposition;
    }

    public LongWritable getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(LongWritable timestamps) {
        this.timestamps = timestamps;
    }

    public IntWritable getBallsTaken() {
        return ballsTaken;
    }

    public void setBallsTaken(IntWritable ballsTaken) {
        this.ballsTaken = ballsTaken;
    }

    public IntWritable getFours() {
        return fours;
    }

    public void setFours(IntWritable fours) {
        this.fours = fours;
    }

    public IntWritable getSix() {
        return six;
    }

    public void setSix(IntWritable six) {
        this.six = six;
    }

    @Override
    public String toString() {
        return "PlayerDetails{" +
                "playerName=" + playerName +
                ", score=" + score +
                ", opposition=" + opposition +
                ", timestamps=" + timestamps +
                ", ballsTaken=" + ballsTaken +
                ", fours=" + fours +
                ", six=" + six +
                '}';
    }
}
