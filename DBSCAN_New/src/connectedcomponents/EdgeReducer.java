/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package connectedcomponents;

import connectedcomponentsdataformat.EdgeKey;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class EdgeReducer extends Reducer<EdgeKey, IntWritable, Text, Text> {

    @Override
    protected void reduce(EdgeKey key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

        /*
         * Rules:
         * 1) If the vertix key is the smallest, do nothing
         * 2) If it is the largest, then loose connection to ALL neighbors and
         * connect them to smallest vertix bidirectionally 
         * 3) Otherwise, loose you connections to your neighbors except the smallest
         * and connect them to smallest
         * NOTES: when considering smallest/largest ignore (negative)      
         */

//        System.out.println("++++++++"+ key.source );

        IntWritable value = new IntWritable();

        IntWritable src, smallestDest;

        value = values.iterator().next();
        smallestDest = new IntWritable(value.get());
        src = new IntWritable(key.source);

        int largestVertix = key.source;

//
//        if(smallestDest.get()<0){
//            return;
//        }

//        System.out.println(value.get());

        // Case 1. Note that if the input key,value pair contains only one tuple, then
        // we emit nothing

        int previousValue = smallestDest.get();

        // the second condition is added to cover a core point whose all neighbors are noncore points
        if (src.get() <= smallestDest.get() || smallestDest.get()<0) {
            //context.write(src, smallestDest);
            context.write(new Text(src +","+ smallestDest),new Text());
            while (values.iterator().hasNext()) {
                value = values.iterator().next();                
                //context.write(src, value);
                if(value.get()!=previousValue){
//                    System.out.println(value.get());
                    context.write(new Text(src +","+ value),new Text());
                    previousValue = value.get();
                }
                
            }

        } else {
            boolean more = false;
            // Case 2 and 3
            while (values.iterator().hasNext()) {
                
                value = values.iterator().next();                
                if(value.get()!=previousValue){
//                    System.out.println(value.get());
                    more = true;
                    previousValue = value.get();
                // connecting bidirectionally to smallest
                if (value.get() >= 0) {
                    //context.write(value, smallestDest);
                    //context.write(smallestDest, value);
                    //cnt = counters.findCounter(CONVERGENCE_CNT.CONVERGENCE);
                    context.getCounter(MainNew.CONVERGENCE_CNT.CONVERGENCE).increment(1);
                    context.write(new Text(value +","+ smallestDest),new Text());
                    context.write(new Text(smallestDest +","+ value),new Text());
                    largestVertix = value.get();                    
                    // connecting unidirectioanlly the noncore vertix to the smallest    
                }else {
                    //context.write(smallestDest, value);
                    context.write(new Text(smallestDest +","+ value),new Text());
                }
                }
            }
            // to distinguish case 2 and 3 (case 3: we keep the connection to smalles)
            if (src.get() < largestVertix && more==true) {
                //context.write(src, smallestDest);
                context.write(new Text(src +","+ smallestDest),new Text());
            }
        }
    }
}
