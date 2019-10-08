package main.java.Query;

import java.io.*;

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

public class RelevanceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        HashMap<String, Double> queryWords = wordsInQuery(conf);

        double distance = 0D;
        String [] shit = value.toString().split("\t", 2);
        String docid = shit[0];
        String temp = shit[1].substring(1, shit[1].length() - 1);
        String[] words = temp.split(", ");
        for (String word : words) {
            if (queryWords.containsKey(word)) {
                String[] arr = word.split("=");
                String w = arr[0];
                Double coeff = Double.parseDouble(arr[1]);
                distance += coeff * queryWords.get(word);
            }
        }
        context.write(new Text(docid), new DoubleWritable(-1 * distance));
    }

    private HashMap<String, Double> wordsInQuery(Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.getLocal(conf);
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
                result.put(arr[0], Double.parseDouble(arr[1].toString()));
            }
        }

        return result;

    }
}