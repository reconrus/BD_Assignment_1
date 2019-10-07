import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Indexer {

    public static class IdfMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        //google json parse
        private Gson g = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //new document starts from new line
            StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
            //map will contain the existence of the words
            Map<String, Boolean> wordExists = new HashMap<String, Boolean>();
            while (documents.hasMoreTokens()) {
                //parse json
                Document document = g.fromJson(documents.nextToken(), Document.class);
                //split into words
                String[] words = document.getText().toLowerCase()
                        .split("([ \n\t\r'\"!@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~]|\\n)+");
                //for each word
                for (String word : words) {
                    boolean bad = false;
                    //check if word consists of letters
                    for (Character ch : word.toCharArray()) {
                        if (!(ch >= 'a' && ch <= 'z')) {
                            bad = true;
                            break;
                        }
                    }
                    if (bad) continue;
                    //if word does not exist, add it to hash map
                    if (!wordExists.containsKey(word)) {
                        wordExists.put(word, true);
                        context.write(new Text(word), new IntWritable(1));
                    }
                }
                //clear hash map for new document
                wordExists.clear();
            }
        }
    }

    public static class IdfReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            //since mapper returns only ones for words existing in the document
            //we can sum up to find out the number of documents, containing this word
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        //read input
        System.out.println("\nINPUT:");
        String input_folder = args[0];
        System.out.println("\tinput folder: " + input_folder);
        String idf_output = args[1];
        System.out.println("\tidf output folder: " + idf_output);

        //create configuration and save paths
        Configuration conf = new Configuration();
        conf.set("idf_output", idf_output);

        //create IDF counting job
        Job job = Job.getInstance(conf, "Word IDF");

        //configure job
        job.setJarByClass(Indexer.class);
        job.setMapperClass(IdfMapper.class);
        job.setCombinerClass(IdfReducer.class);
        job.setReducerClass(IdfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input_folder));
        FileOutputFormat.setOutputPath(job, new Path(idf_output));

        //wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}