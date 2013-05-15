/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package connectedcomponents;

import similarityjoindataformat.HadoopInputFormat;
import connectedcomponentsdataformat.PreEdgeKeySource;
import ccpreprocessing2.PreEdgeKeyPartionerSource;
import ccpreprocessing2.PreSortingComparatorSource;
import ccpreprocessing2.PreGroupingComparatorSource;
import ccpreprocessing2.PreEdgeMapperSource;
import ccpreprocessing2.PreEdgeReducerSource;
import ccstats.StatsReducer;
import ccstats.StatsMapper;
import ccpreprocessing1.PreCoreKeyComparator;
import ccpreprocessing1.PreCorePtMapper;
import ccpreprocessing1.PreCorePtReducer;
import ccpreprocessing1.PreCoreGroupComparator;
import connectedcomponentsdataformat.EdgeFileInputFormat;
import connectedcomponentsdataformat.EdgeKey;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import similarityjoindbscan.*;


public class MainNew {

    public static String getLogFilePath(Configuration con, String pathStr) {


        //args[2] + "/_logs/history/*"
        String logFilePath = "";

        try {
            Path filePathLog = null;

            FileSystem fileSysLog = FileSystem.get(con);
            FileStatus[] fStatusLog = null;
            fileSysLog = FileSystem.get(con);
            fStatusLog = fileSysLog.globStatus(new Path(pathStr));

            for (int j = 0; j < fStatusLog.length; j++) {
                //look for the files only out of the output directoty
                filePathLog = fStatusLog[j].getPath();
                if (fileSysLog.isFile(filePathLog)) {
                    String s = filePathLog.toString();
                    if (!s.endsWith(".xml")) {
                        logFilePath = s;
                        break;
                    }

                }

            }
        } catch (IOException ex) {
            ex.toString();
        }
        return logFilePath;
    }

    public static int collectMeasure(Map<String, JobHistory.Task> tasks) {
        //Measure currentMeasure = new Measure();

        Map<String, JobHistory.Task> allTasks = tasks;
        Map<JobHistory.Keys, String> keysMap;

        int i = 0;
        int mapId = 0;

        String taskType = "";
        for (String str : allTasks.keySet()) {
            JobHistory.Task task = allTasks.get(str);
            taskType = task.get(JobHistory.Keys.TASK_TYPE);
            if (taskType.equals("REDUCE")) {
                mapId++;
            }
        }
        return mapId;
        //return currentStat;
        //System.out.println("stat list size: " + statList.size());
    }

    public static void writeStats(String outFolder, int numOfIterations, long runTimeInMs, long ssjTimeInMs, long preProcessTimeInMs, long fccTimeInMs, int minPts, float epsilon, long nrOfClusters, long mapOutputB, long reduceInputR, long reduceShuffleB, int nrReducers, int numOfReduceTasks) {
        System.out.println("yahoooooooooooooooooooooooooooooooooooooooooooooo");

        try {
            // Create file
            FileWriter fstream = new FileWriter(outFolder + "/stats" + "_" + "DBSCAN" +"_"+"New" + "_" + minPts + "_" + epsilon + "_" + nrReducers + ".txt");
            BufferedWriter out = new BufferedWriter(fstream);
            out.write("MinPts: " + minPts);
            out.write("\n");
            out.write("Epsilon: " + epsilon);
            out.write("\n");
//            out.write("Reduce_Tasks: " + numOfReduceTasks);
//            out.write("\n");
            out.write("NumOfReducers: " + nrReducers);
            out.write("\n");

            out.write("Total RunTime: " + runTimeInMs);
            out.write("\n");
            
            out.write("Similarity Join RunTime: " + ssjTimeInMs);
            out.write("\n");
            
            out.write("Pre Processing RunTime: " + preProcessTimeInMs);
            out.write("\n");
            
            out.write("Finding Connected Components RunTime: " + fccTimeInMs);
            out.write("\n");
            
            out.write("NumOfIterations: " + numOfIterations);
            out.write("\n");
            out.write("NumOfCusters: " + nrOfClusters);
            out.write("\n");

            out.write("MapOutputBytes: " + mapOutputB);
            out.write("\n");

            out.write("ReduceInputRecords: " + reduceInputR);
            out.write("\n");

            out.write("ReduceShuffleBytes: " + reduceShuffleB);
            out.write("\n");

//            System.out.println("*******************!!!!!!!!!!!!!!!!!!!!!");

            out.close();
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }



    }

    /**
     * @param args the command line arguments
     */
    public static enum CONVERGENCE_CNT {

        CONVERGENCE;
    };

    public static enum Stats {

        STATS;
    };

    public static enum PROPERTY {

        EPSILON,
        MINPTS;
    };

    public static void main(String[] args) throws InterruptedException, IOException {
        // TODO code application logic here


        long totalRunTimeInMs = 0;
        long tempStartTimeinMs = 0;
        long tempFinishTimeinMs = 0;
        long ssjTimeinMs=0;
        long fccTimeinMs=0;
        long preProcessTimeinMs=0;
        Counters counters;
        Counter cnt;
        int i = 0;
        boolean converged = false;
        int nrOfIterations = 0;
        long mapOutputBytes = 0;
        long reduceShuffleBytes = 0;
        long reduceInputRecords = 0;


        Path filePath = null;
        FileSystem fileSys = null;

        FileStatus[] fStatus = null;


        int numOfReducers = Integer.parseInt(args[4].trim());
        float epsilon = Float.parseFloat(args[3].trim());
        int minPts = Integer.parseInt(args[2].trim());
// Similarity Join 
        Configuration confPre0 = new Configuration();
        Job jobSS;
        confPre0.setInt(MainNew.PROPERTY.MINPTS.name(), minPts);
        confPre0.setFloat(MainNew.PROPERTY.EPSILON.name(), epsilon);
        // Prepreocessing run settings
///*
        if (Integer.parseInt(args[2].trim()) != 1) {
//Similarity Join
            jobSS = new Job(confPre0, "DBSCAN Similarity Join");
//            confPre0 = jobSS.getConfiguration();
            jobSS.setJarByClass(MainNew.class);

            jobSS.setInputFormatClass(HadoopInputFormat.class);
            FileInputFormat.setInputPaths(jobSS, new Path(args[0]));
            FileOutputFormat.setOutputPath(jobSS, new Path(args[1] + "/0"));
            jobSS.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);

            jobSS.setMapperClass(SimilarityMapperPara.class);
            jobSS.setMapOutputKeyClass(similarityjoindataformat.KeyWritable2.class);
            jobSS.setMapOutputValueClass(similarityjoindataformat.ValueWritable.class);

            jobSS.setPartitionerClass(HadoopPartitioner.class);
            jobSS.setSortComparatorClass(HadoopKeyComparator.class);
            jobSS.setGroupingComparatorClass(HadoopGroupComparator.class);

            jobSS.setReducerClass(SimilarityReducerPara.class);
            jobSS.setNumReduceTasks(numOfReducers);
            jobSS.setOutputKeyClass(org.apache.hadoop.io.IntWritable.class);
            jobSS.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);

            try {
                System.out.println("no.0 submitting ...");
                tempStartTimeinMs = System.currentTimeMillis();
                jobSS.waitForCompletion(true);
                tempFinishTimeinMs = System.currentTimeMillis();
                ssjTimeinMs +=(tempFinishTimeinMs - tempStartTimeinMs);
                totalRunTimeInMs += (tempFinishTimeinMs - tempStartTimeinMs);
                counters = jobSS.getCounters();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_BYTES");
                mapOutputBytes += cnt.getValue();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_SHUFFLE_BYTES");
                reduceShuffleBytes += cnt.getValue();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_RECORDS");
                reduceInputRecords += cnt.getValue();
                System.out.println("no.0 submitted.");

            } catch (ClassNotFoundException ex) {
                System.out.println("ERROR");
                Logger.getLogger(MainNew.class.getName()).log(Level.SEVERE, null, ex);
            }

            // jobpre1: outputs the noncore object (-x,x) in the same file
            int k = 0;
            Configuration confPre1 = new Configuration();
            Job jobpre1;
            confPre1.setInt(MainNew.PROPERTY.MINPTS.name(), minPts);
            jobpre1 = new Job(confPre1, "Distinguish core and non-core vertices");
//            confPre1 = jobpre1.getConfiguration();
            jobpre1.setJarByClass(MainNew.class);
            jobpre1.setInputFormatClass(EdgeFileInputFormat.class);
            FileInputFormat.setInputPaths(jobpre1, new Path(args[1] + "/0" + "/part*"));
//            try {
////                fileSys = FileSystem.get(confPre0);
//                fileSys = FileSystem.get(confPre1);
//                fStatus = fileSys.globStatus(new Path(args[1] + "/0" + "/part*"));
//
//                for (int j = 0; j < fStatus.length; j++) {
//                    //look for the files only out of the output directoty
//                    filePath = fStatus[j].getPath();
//                    if (fileSys.isFile(filePath)) {
//                        FileInputFormat.setInputPaths(jobpre1, filePath);
//                        System.out.println("the input to the Pre processing job 1 is " + filePath.toString());
//                    }
//                }
//            } catch (IOException ex) {
//                ex.toString();
//            }
//            FileInputFormat.setInputPaths(jobpre1, new Path(args[1] + "/0" + "/part*"));
            FileOutputFormat.setOutputPath(jobpre1, new Path(args[1] + "/1"));
//
            // Partitioning settings
            jobpre1.setPartitionerClass(PreEdgeKeyPartionerSource.class);
            jobpre1.setSortComparatorClass(PreCoreKeyComparator.class);
            jobpre1.setGroupingComparatorClass(PreCoreGroupComparator.class);

//            // Mapper settings
            jobpre1.setMapperClass(PreCorePtMapper.class);
            jobpre1.setMapOutputKeyClass(PreEdgeKeySource.class);
            jobpre1.setMapOutputValueClass(IntWritable.class);
//
//            // Reducer settings
            jobpre1.setReducerClass(PreCorePtReducer.class);
            jobpre1.setNumReduceTasks(numOfReducers);
            jobpre1.setOutputKeyClass(TextOutputFormat.class);
            jobpre1.setOutputValueClass(TextOutputFormat.class);
//
            try {
                System.out.println("no.1 submitting ...");

                tempStartTimeinMs = System.currentTimeMillis();
                jobpre1.waitForCompletion(true);
                tempFinishTimeinMs = System.currentTimeMillis();
                preProcessTimeinMs += (tempFinishTimeinMs - tempStartTimeinMs);
                totalRunTimeInMs += (tempFinishTimeinMs - tempStartTimeinMs);
                counters = jobpre1.getCounters();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_BYTES");
                mapOutputBytes += cnt.getValue();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_SHUFFLE_BYTES");
                reduceShuffleBytes += cnt.getValue();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_RECORDS");
                reduceInputRecords += cnt.getValue();
                System.out.println("no.1 submitted.");
            } catch (ClassNotFoundException ex) {
                System.out.println("ERROR");
                Logger.getLogger(MainNew.class.getName()).log(Level.SEVERE, null, ex);
            }

            // second job
            Configuration confPre2 = new Configuration();
            Job jobpre2;

            jobpre2 = new Job(confPre2, "Distinguish core and non-core vertices");
            confPre2 = jobpre2.getConfiguration();
            jobpre2.setJarByClass(MainNew.class);

            // InOut settings
            jobpre2.setInputFormatClass(EdgeFileInputFormat.class);
            //FileInputFormat.addInputPath(job, new Path(args[0]));

            // Partitioning settings
            jobpre2.setPartitionerClass(PreEdgeKeyPartionerSource.class);
            jobpre2.setGroupingComparatorClass(PreGroupingComparatorSource.class);
            jobpre2.setSortComparatorClass(PreSortingComparatorSource.class);

            // Mapper settings
            jobpre2.setMapperClass(PreEdgeMapperSource.class);
            jobpre2.setMapOutputKeyClass(PreEdgeKeySource.class);
            jobpre2.setMapOutputValueClass(IntWritable.class);

            // Reducer settings
            jobpre2.setReducerClass(PreEdgeReducerSource.class);
            jobpre2.setNumReduceTasks(numOfReducers);
            jobpre2.setOutputKeyClass(TextOutputFormat.class);
            jobpre2.setOutputValueClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(jobpre2, new Path(args[1] + "/1" + "/part*"));
//            try {
////                fileSys = FileSystem.get(confPre1);
//                fileSys = FileSystem.get(confPre2);
//                fStatus = fileSys.globStatus(new Path(args[1] + "/1" + "/part*"));
//
//                for (int j = 0; j < fStatus.length; j++) {
//                    //look for the files only out of the output directoty
//                    filePath = fStatus[j].getPath();
//                    if (fileSys.isFile(filePath)) {
//                        FileInputFormat.setInputPaths(jobpre2, filePath);
////                        System.out.println("the input to the main task is " + filePath.toString());
//                    }
//
//                }
//            } catch (IOException ex) {
//                ex.toString();
//            }
            FileOutputFormat.setOutputPath(jobpre2, new Path(args[1] + "/2"));

            try {
                System.out.println("no.2 submitting ...");

                tempStartTimeinMs = System.currentTimeMillis();
//               why here is jobpre1??? jobpre1.waitForCompletion(true);
                jobpre2.waitForCompletion(true);
                tempFinishTimeinMs = System.currentTimeMillis();
                preProcessTimeinMs+=(tempFinishTimeinMs - tempStartTimeinMs);
                totalRunTimeInMs += (tempFinishTimeinMs - tempStartTimeinMs);
                counters = jobpre2.getCounters();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_BYTES");
                mapOutputBytes += cnt.getValue();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_SHUFFLE_BYTES");
                reduceShuffleBytes += cnt.getValue();
                cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_RECORDS");
                reduceInputRecords += cnt.getValue();
                System.out.println("no.2 submitted.");
            } catch (ClassNotFoundException ex) {
                System.out.println("ERROR");
                Logger.getLogger(MainNew.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
//*/
        while (!converged) {
            nrOfIterations++;
            System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \nIteration: " + (nrOfIterations));
            Configuration conf = new Configuration();
            Job jobCC;

            jobCC = new Job(conf, "DBSCAN Connected Componenets");
            conf = jobCC.getConfiguration();
            jobCC.setJarByClass(MainNew.class);

            // InOut settings
            jobCC.setInputFormatClass(EdgeFileInputFormat.class);
            //FileInputFormat.addInputPath(job, new Path(args[0]));

            if (i == 0) {

//TODO
                if (Integer.parseInt(args[2].trim()) == 1) {
                    FileInputFormat.setInputPaths(jobCC, new Path(args[0]));
                } else {

                    FileInputFormat.setInputPaths(jobCC, new Path(args[1] + "/2" + "/part*"));
                }

                //FileInputFormat.setInputPaths(job, new Path(args[1] + "/1"));
            } else {

                //System.out.println("----------------------------------");

                filePath = null;
                fileSys = null;
                fStatus = null;

                try {
                    fileSys = FileSystem.get(conf);
                    fStatus = fileSys.globStatus(new Path(args[1] + "/3/" + (i - 1) + "/part*"));

                    for (int j = 0; j < fStatus.length; j++) {
                        //look for the files only out of the output directoty
                        filePath = fStatus[j].getPath();
                        if (fileSys.isFile(filePath)) {
                            FileInputFormat.addInputPath(jobCC, filePath);
//                            FileInputFormat.setInputPaths(jobCC, filePath);
//                            System.out.println("the input to the main task is " + filePath.toString());
                        }
                    }
                } catch (IOException ex) {
                    ex.toString();
                }
            }
            FileOutputFormat.setOutputPath(jobCC, new Path(args[1] + "/3/" + i));

            // Mapper settings
            jobCC.setMapperClass(EdgeMapper.class);
            jobCC.setMapOutputKeyClass(EdgeKey.class);
            jobCC.setMapOutputValueClass(IntWritable.class);

            // Partitioning settings
            jobCC.setPartitionerClass(EdgeKeyPartitioner.class);
            jobCC.setGroupingComparatorClass(GroupingComparator.class);
            jobCC.setSortComparatorClass(SortingComparator.class);

            // Reducer settings
            jobCC.setReducerClass(EdgeReducer.class);
            jobCC.setNumReduceTasks(numOfReducers);
            jobCC.setOutputKeyClass(TextOutputFormat.class);
            jobCC.setOutputValueClass(TextOutputFormat.class);

            try {
                System.out.println("no.3 submitting ...");
                System.out.println("number of reducers: " + numOfReducers);

                tempStartTimeinMs = System.currentTimeMillis();
                jobCC.waitForCompletion(true);
                tempFinishTimeinMs = System.currentTimeMillis();
                fccTimeinMs+=(tempFinishTimeinMs - tempStartTimeinMs);
                totalRunTimeInMs += (tempFinishTimeinMs - tempStartTimeinMs);

//                job.waitForCompletion(true);
                System.out.println("no.3 submitted.");
            } catch (ClassNotFoundException ex) {
                System.out.println("ERROR");
                Logger.getLogger(MainNew.class.getName()).log(Level.SEVERE, null, ex);
            }

            // Check for convergence
            counters = jobCC.getCounters();
            cnt = counters.findCounter(MainNew.CONVERGENCE_CNT.CONVERGENCE);

            System.out.println("counter value:" + cnt.getValue());

            if (cnt.getValue() == 0) {
                converged = true;
            }
            i++;

            // for extra stats
            counters = jobCC.getCounters();
            cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_BYTES");
            mapOutputBytes += cnt.getValue();
            cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_SHUFFLE_BYTES");
            reduceShuffleBytes += cnt.getValue();
            cnt = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_RECORDS");
            reduceInputRecords += cnt.getValue();
        }

// collect statistics regarding each cluster and how many nodes it has

        Configuration confStats = new Configuration();
        Job jobStats;

        jobStats = new Job(confStats, "Stats");
        confStats = jobStats.getConfiguration();
        jobStats.setJarByClass(MainNew.class);

        // InOut settings
        jobStats.setInputFormatClass(EdgeFileInputFormat.class);
        //FileInputFormat.addInputPath(job, new Path(args[0]));

        //FileOutputFormat.setInputPaths(jobStats, new Path(args[1] + "/2/" + (i-1)));
        FileInputFormat.setInputPaths(jobStats, new Path(args[1] + "/3/" + (i - 1) + "/part*"));
        FileOutputFormat.setOutputPath(jobStats, new Path(args[1] + "/3_Stats"));

        // Mapper settings
        jobStats.setMapperClass(StatsMapper.class);
        jobStats.setMapOutputKeyClass(IntWritable.class);
        jobStats.setMapOutputValueClass(IntWritable.class);

        // Reducer settings
        jobStats.setReducerClass(StatsReducer.class);
        jobStats.setNumReduceTasks(numOfReducers);
        jobStats.setOutputKeyClass(TextOutputFormat.class);
        jobStats.setOutputValueClass(TextOutputFormat.class);

        try {
            System.out.println("submitting ...");
            jobStats.waitForCompletion(true);
            System.out.println("submitted.");
        } catch (ClassNotFoundException ex) {
            System.out.println("ERROR");
            Logger.getLogger(MainNew.class.getName()).log(Level.SEVERE, null, ex);
        }

        counters = jobStats.getCounters();
        cnt = counters.findCounter(MainNew.Stats.STATS);



        JobHistory.JobInfo jobInfo = new JobHistory.JobInfo("2");

//        DefaultJobHistoryParser.parseJobTasks(getLogFilePath(confStats, args[1] + "/2_Stats" + "/_logs/history/*"), jobInfo, FileSystem.get(confStats));
//        int numTasks = collectMeasure(jobInfo.getAllTasks());
//        System.out.println("");
        int numTasks = 1000000;

        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&:" + args[5]);

        writeStats(args[5], nrOfIterations, totalRunTimeInMs, ssjTimeinMs, preProcessTimeinMs, fccTimeinMs, minPts, epsilon, cnt.getValue(), mapOutputBytes, reduceInputRecords, reduceShuffleBytes, numOfReducers, numTasks);
//        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&:" + args[5]);
    }
}
