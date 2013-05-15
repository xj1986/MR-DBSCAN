/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


package connectedcomponents;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import connectedcomponentsdataformat.EdgeKey;
import org.apache.hadoop.io.IntWritable;


public class EdgeMapper extends Mapper<IntWritable, IntWritable, EdgeKey, IntWritable> {

    @Override
    protected void map(IntWritable src, IntWritable dest,
            Context context) throws IOException, InterruptedException {
        EdgeKey key = new EdgeKey(src.get(), dest.get());
//        System.out.println("("+ key.toString()+", "+ dest.get()+")");
        context.write(key, dest);
    }
}
