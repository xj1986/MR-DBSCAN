/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package connectedcomponents;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.IntWritable;


public class PreEdgeMapperSource extends Mapper<IntWritable, IntWritable, PreEdgeKeySource, IntWritable> {

    @Override
    protected void map(IntWritable src, IntWritable dest,
            Context context) throws IOException, InterruptedException {
        PreEdgeKeySource key = new PreEdgeKeySource(dest.get(), src.get());
        context.write(key, src);
    }
}
