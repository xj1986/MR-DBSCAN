/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package connectedcomponents;

import org.apache.hadoop.io.IntWritable;

public class PreEdgeKeyPartionerSource extends org.apache.hadoop.mapreduce.Partitioner<PreEdgeKeySource, IntWritable>

        {

    @Override
    public int getPartition(PreEdgeKeySource key, IntWritable value, int numPartitions) {
        return   key.source % numPartitions;
    }

}

