package main.java;

import main.java.Indexer.IdfMapper;
import main.java.Indexer.IdfReducer;
import main.java.Indexer.TfIdfMapper;

import main.java.Query.QueryTfIdfMapper;
import main.java.Query.QueryTfIdfReducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;


public class SearchEngine extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new SearchEngine(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length > 0) {
            if (args.length < 2) {
                System.exit(1);
            } else if (args[0].equals("Indexer")) { // Indexing task
                boolean isCompleted = runIdf(args[1], "idf_output");
                if (!isCompleted) {
                    return 1;
                }
                isCompleted = runTfIdf(args[1], "tf_idf_output", "idf_output");
                if (!isCompleted) {
                    return 1;
                }
            } else if (args[0].equals("Query") && args.length > 2){ // Query task
                try {
                    Integer count = Integer.parseInt(args[1]);
                } catch (NumberFormatException e) {
                    return 1;
                }
                String query = args[2];
                HashMap<String, Integer> words = countFreqsInQuery(query);
                String wordsString = hashMapToString(words);
                boolean isCompleted = runQueryTfIdf(wordsString, "idf_output", "words_tf_idf_output");
                if (!isCompleted) {
                    return 1;
                }
            }
        } else {
            return 1;
        }

        return 0;

    }

    public boolean runIdf(String inputFolder, String idfOutput) throws
            IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(idfOutput);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        conf.set("idf_output", idfOutput);

        //create IDF counting job
        Job job = Job.getInstance(conf, "Word IDF");

        //configure job
        job.setJarByClass(SearchEngine.class);
        job.setMapperClass(IdfMapper.class);
        job.setCombinerClass(IdfReducer.class);
        job.setReducerClass(IdfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job, new Path(idfOutput));

        return job.waitForCompletion(true);

    }

    public boolean runTfIdf(String inputFolder, String tfIdfOutput, String idfOutput) throws
            IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(tfIdfOutput);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        conf.set("tf_idf_output", tfIdfOutput);
        conf.set("idf_output", idfOutput);

        Job job = Job.getInstance(conf, "TF/IDF");

        //configure
        job.setJarByClass(SearchEngine.class);
        job.setMapperClass(TfIdfMapper.class);
        job.setNumReduceTasks(100);

        //set output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job, new Path(tfIdfOutput));

        return job.waitForCompletion(true);

    }

    public boolean runQueryTfIdf(String wordsInQuery, String idfInput, String output) throws
            IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("query", wordsInQuery);
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        Job job = Job.getInstance(conf, "Words TF/IDF");

        job.setJarByClass(SearchEngine.class);
        job.setMapperClass(QueryTfIdfMapper.class);
        job.setReducerClass(QueryTfIdfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(idfInput));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);

    }

    private HashMap<String, Integer> countFreqsInQuery(String query) {
        String[] words = query.toLowerCase()
                .split("([ \n\t\r'\"!@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~]|\\n)+");
        HashMap<String, Integer> wordCounter = new HashMap<String, Integer>();
        for (String word : words) {
            if (!wordCounter.containsKey(word)) {
                wordCounter.put(word, 1);
            }
            else {
                int count = wordCounter.get(word);
                wordCounter.put(word, count + 1);
            }
        }

        return wordCounter;
    }

    private String hashMapToString(HashMap<String, Integer> words) {
        Iterator it = words.entrySet().iterator();
        StringBuilder result = new StringBuilder();
        while (it.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)it.next();
            result.append(pair.getKey()).append(":").append(pair.getValue().toString()).append(" ");
            System.out.println(pair.getKey() + " = " + pair.getValue());
            it.remove(); // avoids a ConcurrentModificationException
        }

        return result.toString().trim();

    }

}
