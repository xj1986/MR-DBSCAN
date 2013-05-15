package similarityjoin;

import connectedcomponents.MainOriginal;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import similarityjoindataformat.KeyWritable2;
import similarityjoindataformat.ValueWritable;

public class SimilarityReducer extends Reducer<KeyWritable2,ValueWritable,IntWritable,IntWritable> {
	@Override
    protected void reduce(KeyWritable2 key, Iterable<ValueWritable> values, Context context)
                        throws IOException, InterruptedException {
		
    	int maxcode= 1<<(key.arrayList.size()-1);
    	ArrayList<ValueWritable> bufferValues = new ArrayList<ValueWritable>();
        Configuration conf = context.getConfiguration();
    	float epsilon = conf.getFloat(MainOriginal.PROPERTY.EPSILON.name(),0);
//        int minPts = context.getConfiguration().getInt(MainOriginal.PROPERTY.MINPTS.name(), 0);
        
    	for(ValueWritable value: values){
            for (ValueWritable bufferValue: bufferValues){
                if( (value.bitcode & bufferValue.bitcode)==0 && minDist(value.arrayList, bufferValue.arrayList)<=epsilon*epsilon){
                    context.write(new IntWritable(value.key), new IntWritable(bufferValue.key));
                    context.write(new IntWritable(bufferValue.key), new IntWritable(value.key));  				
                }
            }
            if (value.bitcode < maxcode){
                bufferValues.add(WritableUtils.clone(value, conf));
            }
        }
        }
                        
    public float minDist(ArrayList<Double> d1, ArrayList<Double> d2  ){
    	int size = d1.size();
    	double dis=0;
    	for (int i=0;i<size;i++){
//    		dis+=Math.pow((d1.get(i)-d2.get(i)), 2);
            dis+=(d1.get(i)-d2.get(i))*(d1.get(i)-d2.get(i));
    	}
//    	return (float) Math.sqrt(dis);
        return (float) dis;
    }
    
}
