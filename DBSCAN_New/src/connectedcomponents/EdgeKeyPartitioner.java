/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */



package connectedcomponents;

import connectedcomponentsdataformat.EdgeKey;
import org.apache.hadoop.io.IntWritable;

public class EdgeKeyPartitioner extends org.apache.hadoop.mapreduce.Partitioner<EdgeKey, IntWritable>

        {

    @Override
    public int getPartition(EdgeKey key, IntWritable value, int numPartitions) {
        return   key.source % numPartitions;
    }

}
