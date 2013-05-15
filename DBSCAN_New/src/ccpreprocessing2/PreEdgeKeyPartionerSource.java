/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ccpreprocessing2;

import connectedcomponentsdataformat.PreEdgeKeySource;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class PreEdgeKeyPartionerSource extends Partitioner<PreEdgeKeySource,IntWritable>
        implements Configurable {

     private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        initPartitioner();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    private void initPartitioner() {
        // TODO any initialization goes here
    }

    @Override
    public int getPartition(PreEdgeKeySource key, IntWritable value, int numReduceTasks) {
        return key.source % numReduceTasks;
    }

}

