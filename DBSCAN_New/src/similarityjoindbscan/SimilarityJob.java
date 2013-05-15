package similarityjoindbscan;

import similarityjoindataformat.HadoopInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author jiang
 */
public class SimilarityJob {
    public static void main(String [] args) throws  Exception{
        Configuration conf= new Configuration();
        long totalRunTimeInMs = 0;
        long tempStartTimeinMs =0;
        long tempFinishTimeinMs=0;
        Job job= new Job(conf, "Similairty Join");
        job.setJarByClass(SimilarityJob.class);
        job.setInputFormatClass(HadoopInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        
        job.setMapperClass(SimilarityMapper.class);
        job.setMapOutputKeyClass(similarityjoindataformat.KeyWritable2.class);
        job.setMapOutputValueClass(similarityjoindataformat.ValueWritable.class);
        
        job.setPartitionerClass(HadoopPartitioner.class);
        
        job.setSortComparatorClass(HadoopKeyComparator.class);
        job.setGroupingComparatorClass(HadoopGroupComparator.class);
        
        job.setReducerClass(SimilarityReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(org.apache.hadoop.io.IntWritable.class);
        job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args [1]));
        tempStartTimeinMs = System.currentTimeMillis();
        job.waitForCompletion(true);
        tempFinishTimeinMs = System.currentTimeMillis();
        
        totalRunTimeInMs += (tempFinishTimeinMs - tempStartTimeinMs);
        System.out.println("DBSCAN Parameter fixed is submitted");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
