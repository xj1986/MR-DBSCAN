/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ccpreprocessing1;

import connectedcomponentsdataformat.PreEdgeKeySource;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jiang
 */
public class PreCorePtMapper  extends Mapper<IntWritable, IntWritable, PreEdgeKeySource, IntWritable> {

    @Override
    protected void map(IntWritable src, IntWritable dest,
            Context context) throws IOException, InterruptedException {
        PreEdgeKeySource key = new PreEdgeKeySource(src.get(), dest.get());
        context.write(key, dest);
    }
}