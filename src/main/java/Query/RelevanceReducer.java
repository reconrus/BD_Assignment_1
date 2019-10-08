package main.java.Query;

import org.apache.commons.text.diff.EditScript;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelevanceReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    static int count;

    @Override
    public void setup(Context context)
    {
        Configuration conf = context.getConfiguration();
        count = conf.getInt("count", 10);
    }

    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException
    {
        Double distance = (-1) * key.get();

        for (Text val : values)
        {
            if (count == 0){
                break;
            }
            context.write(val, new DoubleWritable(distance));
            count--;
        }
    }
}
