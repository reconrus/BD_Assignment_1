package main.java.Query;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QueryTfIdfMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
        while (documents.hasMoreTokens()) {
            String line = documents.nextToken();
            String[] array = line.split("\t", 2);
            String word = array[0];
            String idf = array[1];
            context.write(new Text(word), new IntWritable(Integer.parseInt(idf)));
        }
    }

}
