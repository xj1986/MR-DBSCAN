/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ccpreprocessing1;

import connectedcomponents.MainNew;
import connectedcomponentsdataformat.PreEdgeKeySource;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jiang
 */
public class PreCorePtReducer extends Reducer<PreEdgeKeySource, IntWritable, Text, Text> {
    @Override
    protected void reduce(PreEdgeKeySource key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
//distinguish core and non-core pts. for core pts, remain the edges. non-core pts do not emit edge but [-x,x]
            int minPts = context.getConfiguration().getInt(MainNew.PROPERTY.MINPTS.name(), 0);
            int tempValue=0;
            IntWritable value = new IntWritable();
            value = values.iterator().next();
//            	value = values.iterator().next();
            	while(value.get()<0 && values.iterator().hasNext()){
            		tempValue+=value.get();
            		value=values.iterator().next();
            	}
                
                tempValue = 0-tempValue;
            	if (tempValue<minPts-1){
            		context.write(new Text(-key.source+","+key.source), new Text());
            	}
            	else{
            		context.write(new Text(key.source+","+value), new Text());
            		while(values.iterator().hasNext()){
            			value=values.iterator().next();
            			context.write(new Text(key.source+","+value), new Text());
            		}
            	}
}
}

