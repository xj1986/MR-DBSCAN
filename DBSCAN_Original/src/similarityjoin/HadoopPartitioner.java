package similarityjoin;

/*
 * HadoopPartitioner.java
 *
 * Created on 12.12.2011, 13:29:05
 */

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import similarityjoindataformat.KeyWritable2;
import similarityjoindataformat.ValueWritable;

/**
 *Partitioner controls the partitioning of the keys of the intermediate map-outputs. 
 *The key (or a subset of the key) is used to derive the partition, typically by a hash function. 
 *The total number of partitions is the same as the number of reduce tasks for the job. 
 *Hence this controls which of the m reduce tasks the intermediate key (and hence the record) is sent for reduction.
 * @author jiang
 */
public class HadoopPartitioner
        extends Partitioner<KeyWritable2,ValueWritable>
        implements Configurable {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("HadoopPartitioner");

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
    public int getPartition(KeyWritable2 key, ValueWritable value, int numReduceTasks) {
        // TODO return a partition in [0, numReduceTasks)
    	// equally assign the key-value pairs to the amount of reduces
//        return key.arrayList.hashCode() % numReduceTasks;
        return (key.arrayList.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
