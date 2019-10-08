package main.java.Query;

import java.io.*;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.Hash;

public class RelevanceMapper extends Mapper<Text, MapWritable, Text, DoubleWritable> {

    public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        HashMap<String, Double> queryWords = wordsInQuery(conf, context);

        double distance = 0D;
        for (Map.Entry<Writable, Writable> extractData: value.entrySet()) {
            String word = extractData.getKey().toString();
            if (queryWords.containsKey(word)) {
                Double docIdf = Double.parseDouble(extractData.getValue().toString());
                distance += queryWords.get(word) * docIdf;
            }
        }

        context.write(key, new DoubleWritable(distance));

    }

    private HashMap<String, Double> wordsInQuery(Configuration conf, Context context) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        URI[] files= context.getCacheFiles();
        URI path = files[0];

        InputStream in = fs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        HashMap<String, Double> result = new HashMap<String, Double>();
        while (reader.ready()) {
            String line = reader.readLine().trim();
            String[] arr = line.split("\t", 2);
            result.put(arr[0], Double.parseDouble(arr[1]));
        }

        return result;

    }
}
