/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ccpreprocessing2;

import connectedcomponentsdataformat.PreEdgeKeySource;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class PreEdgeReducerSource extends Reducer<PreEdgeKeySource, IntWritable, Text, Text> {


    @Override
    protected void reduce(PreEdgeKeySource key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {


        IntWritable value = new IntWritable();

        IntWritable smallestDest;
        IntWritable src = new IntWritable(key.source);

        value = values.iterator().next();
        smallestDest = new IntWritable(value.get());
        if(smallestDest.get()<0){
            while (values.iterator().hasNext()) {
                value = values.iterator().next();
                context.write(new Text(value+","+smallestDest.get()), new Text());
            }
        }else{
            context.write(new Text(smallestDest.get()+","+src.get()), new Text());
            while (values.iterator().hasNext()) {
                value = values.iterator().next();
                context.write(new Text(value.get()+","+src.get()), new Text());
            }
        }
    }}
