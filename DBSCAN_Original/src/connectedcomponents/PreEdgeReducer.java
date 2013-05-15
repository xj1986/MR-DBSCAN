/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package connectedcomponents;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class PreEdgeReducer extends Reducer<IntWritable, IntWritable, Text, Text> {


    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

            int minPts = context.getConfiguration().getInt(MainOriginal.PROPERTY.MINPTS.name(), 0);
            ArrayList<IntWritable> lst = new ArrayList<IntWritable>();
            IntWritable value = new IntWritable();
            
            int i=0;
            while (values.iterator().hasNext()) {
                value = values.iterator().next();
                // This if statement is added to handel the single points
                if(key.get() != value.get())
                    i++;

                if(i<minPts){
                    lst.add(new IntWritable(value.get()));
                }else{
                    context.write(new Text(key.get()+","+value.get()), new Text());
                }
            }

            if( ++i < minPts){
                lst.clear();
                context.write(new Text(-1*key.get() + "," + key.get()), new Text());
            }else{
                for(int j=0; j<lst.size(); j++){
                    context.write(new Text(key.get()+","+lst.get(j).get()), new Text());
                }
                lst.clear();
            }
    }
}

