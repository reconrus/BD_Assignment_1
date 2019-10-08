package main.java.Query;

import java.io.*;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class RelevanceMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        HashMap<String, Double> queryWords = wordsInQuery(conf);

        double distance = 0D;
        String [] separated = value.toString().split("\t", 2);
        String docid = separated[0];
        String temp = separated[1].substring(1, separated[1].length() - 1);
        String[] pairs = temp.split(", ");
        for (String pair : pairs) {
            String[] arr = pair.split("=");
            String word = arr[0];
            if (queryWords.containsKey(word)) {
                Double coeff = Double.parseDouble(arr[1]);
                distance += coeff * queryWords.get(word);
            }
        }
        if (distance != 0D) {
            context.write(new DoubleWritable(-1 * distance), new Text(docid));
        }
    }

    private HashMap<String, Double> wordsInQuery(Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get( conf);
        String query = conf.get("query_tf_idf");

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
                new Path(query), true);
        HashMap<String, Double> result = new HashMap<String, Double>();
        while (fileStatusListIterator.hasNext()) {
            //open stream for file
            FSDataInputStream stream = fileSystem.open(fileStatusListIterator.next().getPath());
            Scanner scanner = new Scanner(stream);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                String[] arr = line.split("\t",2);
                result.put(arr[0], Double.parseDouble(arr[1]));
            }
        }

        return result;

    }
}
