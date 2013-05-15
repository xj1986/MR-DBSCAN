package ccstats;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import connectedcomponents.MainNew;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class StatsReducer extends Reducer<IntWritable, IntWritable, Text, Text> {


    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
            context.getCounter(MainNew.Stats.STATS).increment(1);
            IntWritable dummy = new IntWritable();
            int i=0;
            while (values.iterator().hasNext()) {
                dummy = values.iterator().next();
                i++;
            }
            context.write(new Text(key.get()+":"+i), new Text());

    }}