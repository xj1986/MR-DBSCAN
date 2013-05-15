package similarityjoin;

import connectedcomponents.MainOriginal;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import similarityjoindataformat.ArrayListDoubleWritable;
import similarityjoindataformat.ArrayListIntegerWritable;
import similarityjoindataformat.KeyWritable2;
import similarityjoindataformat.ValueWritable;

public class SimilarityMapper extends Mapper<IntWritable,ArrayListDoubleWritable,KeyWritable2,ValueWritable> {

    @Override
    protected void map(IntWritable key, ArrayListDoubleWritable value, Context context)
                    throws IOException, InterruptedException {
    	float epsilon = context.getConfiguration().getFloat(MainOriginal.PROPERTY.EPSILON.name(), 0);

    	int dim= value.arrayList.size();
//    	int div=(int) Math.pow(2, dim);
        int div= dim*dim;
    	
    	for (int bitcode = 0; bitcode < div; bitcode++){
    		ArrayListIntegerWritable newKey = new ArrayListIntegerWritable();
    		ArrayList<Integer> tempArrayList= new ArrayList<Integer>();
    		for (int i=0; i<dim; i++){
    			tempArrayList.add((int)(value.arrayList.get(i)/epsilon)+((bitcode>>i)&1));
    		}
    		for (int j=0; j<dim; j++){
    			newKey.arrayList.add(tempArrayList.get(j));
    		}
    		context.write(new KeyWritable2(newKey.arrayList, bitcode), new ValueWritable(key.get(),value.arrayList,bitcode));
    		
    	}
    }
}
