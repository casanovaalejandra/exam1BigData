package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class WordCounterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        // key is the word matched from the cases
        // values is a list of 1s, one for each time the word was found

        // setup a counter
        String id = null;
        // iterator over list of 1s, to count them (no size() or length() method available)
        for (Text value : values ){
            id+=" "+value.toString();
        }
        // emit key-pair: key, count
        // key is the word
        // count is the number of times that word appeared on the tweets
        Logger logger = LogManager.getRootLogger();
        //logger.trace("Red: " + key.toString());

        // DEBUG
        context.write(key, new Text(id));
    }
}
