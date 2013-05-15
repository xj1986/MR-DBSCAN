package similarityjoindbscan;

import similarityjoindataformat.DataObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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
	System.out.println("Star reducers");	
    	int maxcode= 1<<(key.arrayList.size()-1);
    	ArrayList<ValueWritable> bufferValues = new ArrayList<ValueWritable>();
    	Configuration conf = context.getConfiguration();
    	int minPts=4;
        float epsilon = 1.0f;
    	HashMap<Integer, ArrayList<Integer>> adjHashMap = new HashMap<Integer, ArrayList<Integer>>();
    	Map<Integer, DataObject> recordMap = new HashMap<Integer, DataObject>();

//    	create an adjacency list for each node.
    	for(ValueWritable value: values){
    		for (ValueWritable bufferValue: bufferValues){
    			if( (value.bitcode & bufferValue.bitcode)==0 && minDist(value.arrayList, bufferValue.arrayList)<=epsilon){
//    			    context.write(new IntWritable(value.key), new IntWritable(bufferValue.key));
//    				context.write(new IntWritable(bufferValue.key), new IntWritable(value.key));
    				
//    				instead of outputting the key, store the adjacent list of each point.
//    				value.key as key; buffervalue.key as add value into adjArrayList
//    				one direction
    				if (!adjHashMap.containsKey(value.key)){
    					ArrayList<Integer> adjArrayList = new ArrayList<Integer>();
    					adjArrayList.add(bufferValue.key);
    					adjHashMap.put(value.key, adjArrayList);  //add a entire new adjacency
    				}
    				else{
    					if (!adjHashMap.get(value.key).contains(bufferValue.key));{
    						adjHashMap.get(value.key).add(bufferValue.key); // add new element to the adjacency
    					}    					
    				}
//    				the other direction
    				if (!adjHashMap.containsKey(bufferValue.key)){
    					ArrayList<Integer> adjArrayList = new ArrayList<Integer>();
    					adjArrayList.add(value.key);
    					adjHashMap.put(bufferValue.key, adjArrayList);  //add a entire new adjacency
    				}
    				else{
    					if (!adjHashMap.get(bufferValue.key).contains(value.key));{
    						adjHashMap.get(bufferValue.key).add(value.key); // add new element to the adjacency
    					}    					
    				}
    			}
    		} 
    		if (value.bitcode < maxcode){
    			bufferValues.add(WritableUtils.clone(value, conf));
    		}
    	}
    	
//    	cluster find logic here....
    	for ( Entry<Integer, ArrayList<Integer>> entry: adjHashMap.entrySet()) {
			recordMap.put(entry.getKey(),new DataObject(entry.getValue().size(),false) );
			context.write(new IntWritable(entry.getKey()), new IntWritable(-entry.getValue().size()));
//			System.out.println(entry.getValue().toString());
		}
    	
    	Iterator<Map.Entry<Integer, ArrayList<Integer>>> entries = adjHashMap.entrySet().iterator();
    	while (entries.hasNext()) {
   		 Map.Entry<Integer, ArrayList<Integer>> entry = entries.next();
   		 ArrayList<Integer> neighbors = entry.getValue();
   		 int tempKey = entry.getKey();
   		 //core point operations: 1.remove edges between connected core points 2.redirect non core point to core point
   		 if (recordMap.get(tempKey).getVisted()==false && recordMap.get(tempKey).size>=minPts-1){
   			 recordMap.get(tempKey).setVisited(true);
   			 for (int i=0; i<neighbors.size();i++) {
       			 int neighbor= neighbors.get(i);
       			 if (recordMap.get(neighbor).getVisted() == false && recordMap.get(neighbor).size>=minPts-1) {
       				 recordMap.get(neighbor).setVisited(true);
       				 ArrayList<Integer> ofNeighbors=adjHashMap.get(neighbor);
       				 for (int j=0; j<ofNeighbors.size();j++){
       					 int ofNeighbor=ofNeighbors.get(j);
       					 if ((recordMap.get(ofNeighbor).getVisted() == false && recordMap.get(ofNeighbor).size>=minPts-1)||(recordMap.get(ofNeighbor).size<minPts-1)){
       						 if (!adjHashMap.get(tempKey).contains(ofNeighbor)){
       							 adjHashMap.get(tempKey).add(ofNeighbor);
       							 adjHashMap.get(ofNeighbor).add(tempKey);
       						 }
       						 adjHashMap.get(neighbor).remove(new Integer(ofNeighbor));
       						 adjHashMap.get(ofNeighbor).remove(new Integer(neighbor));
       						 j--;
       			         }
   		             }
       			 }
   	     	}	
   		 }
   		 //non core point operations: if two non core points shares a or more identical core pts, then remove the edge between these two non-core points.
   		 else if(recordMap.get(tempKey).size<minPts-1){
   			 for (int i=0; i<neighbors.size();i++) {
   				 int neighbor= neighbors.get(i);
   				 if(recordMap.get(neighbor).size<minPts-1){
   					 ArrayList<Integer> ofNeighbors=adjHashMap.get(neighbor);
       				 for (int j=0; j<ofNeighbors.size();j++){
       					 int ofNeighbor=ofNeighbors.get(j);
       					 if (recordMap.get(ofNeighbor).size>=minPts-1 && adjHashMap.get(tempKey).contains(ofNeighbor)){
       						 adjHashMap.get(tempKey).remove(new Integer(neighbor));
       						 adjHashMap.get(neighbor).remove(new Integer(tempKey));
       						 i--;
       					 }
       				 }
   				 }
   			 }
   		  }
  	         else if(entries.hasNext()){
			     entries.next();
	        }

   	    }
    	for (Entry<Integer, ArrayList<Integer>> entry : adjHashMap.entrySet()) {
			for (Integer item : entry.getValue()) {
				context.write(new IntWritable(entry.getKey()) , new IntWritable(item));
			}
		}
    	
        System.out.println("End reducers");
    	
	}
    	
    public float minDist(ArrayList<Double> d1, ArrayList<Double> d2  ){
    	int size = d1.size();
    	double dis=0;
    	for (int i=0;i<size;i++){
    		dis+=Math.pow((d1.get(i)-d2.get(i)), 2);
    	}
    	return (float) Math.sqrt(dis);
    }
    
}
