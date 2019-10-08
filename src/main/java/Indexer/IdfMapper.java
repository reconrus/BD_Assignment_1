package main.java.Indexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import main.java.Indexer.Document;

import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IdfMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    //google json parse
    private static Gson g = new Gson();

    @Override
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
                boolean isBad = false;
                //check if word consists of letters
                for (Character ch : word.toCharArray()) {
                    if (!(ch >= 'a' && ch <= 'z')) {
                        isBad = true;
                        break;
                    }
                }
                if (isBad) continue;
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
