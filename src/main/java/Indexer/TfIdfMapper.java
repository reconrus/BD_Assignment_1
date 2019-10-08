package main.java.Indexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import main.java.Indexer.Document;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TfIdfMapper extends Mapper<Object, Text, Text, MapWritable> {

    private static Gson g = new Gson();

    //this hash map will contain IDF of documents
    private static HashMap<String, Integer> wordsIdf;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(wordsIdf == null){
            wordsIdf = new HashMap<String, Integer>();

            //new configuration
            Configuration conf = context.getConfiguration();
            String idf_output = conf.get("idf_output");
            //Open file systemimport main.java.Indexer.Document;

            FileSystem fileSystem = FileSystem.getLocal(conf);
            //iterator for files
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
                    new Path(idf_output), true);
            //for all files in folder
            while (fileStatusListIterator.hasNext()) {
                //open stream for file
                FSDataInputStream stream = fileSystem.open(fileStatusListIterator.next().getPath());
                Scanner scanner = new Scanner(stream);
                //add number to map
                while (scanner.hasNextLine()) {
                    String inputLine = scanner.nextLine();
                    String[] input = inputLine.split("[ \t]+");
                    wordsIdf.put(input[0], Integer.parseInt(input[1]));
                }
            }
            fileSystem.close();
        }

        //split file by new line
        StringTokenizer documents = new StringTokenizer(value.toString(), "\n");
        //map will contain how much each word was in document
        Map<String, Integer> wordsCount = new HashMap<String, Integer>();
        while (documents.hasMoreTokens()) {
            //parse Json
            Document document = g.fromJson(documents.nextToken(), Document.class);
            //get all words
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
                if (wordsCount.containsKey(word)) {
                    //if word is in the map increase its value by one
                    wordsCount.put(word, wordsCount.get(word) + 1);
                } else {
                    //if word is not in the map, set the value to one
                    wordsCount.put(word, 1);
                }
            }
            //divide tf by idf squared to avoid computing tf/idf for query, so that query tf is enough
            MapWritable tfIdf = new MapWritable();
            for (String word : wordsCount.keySet()) {
                Integer tf = wordsCount.get(word);
                Integer idf = wordsIdf.get(word);
                tfIdf.put(new Text(word), new DoubleWritable(1D * tf / (idf * idf)));
            }
            context.write(new Text(document.getId()), tfIdf);
            wordsCount.clear();
        }
    }
}