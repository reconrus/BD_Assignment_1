package main.java.Query;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryTfIdfReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String str = conf.get("query");
        HashMap<String, Integer> wordsInQuery = getHashMapFromString(str);
        if (wordsInQuery.containsKey(key.toString())) {
            int idf = values.iterator().next().get();
            context.write(key, new DoubleWritable(1D * wordsInQuery.get(key.toString()) / (idf * idf)));
        }
    }

    private HashMap<String, Integer> getHashMapFromString(String str) {
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        String[] array = str.split(" ", 0);
        for (String record : array) {
            String[] splitted = record.split(":", 2);
            result.put(splitted[0], Integer.parseInt(splitted[1]));
        }
        return result;
    }

}
