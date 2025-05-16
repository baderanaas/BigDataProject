package outcomeanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OutcomeAnalysisDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // Validate input arguments
        if (args.length != 2) {
            System.err.println("Usage: OutcomeAnalysisDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create job configuration
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Inspection Outcome Analysis by Category");
        job.setJarByClass(OutcomeAnalysisDriver.class);

        // Set mapper and reducer classes
        job.setMapperClass(OutcomeAnalysisMapper.class);
        job.setCombinerClass(OutcomeAnalysisReducer.class);
        job.setReducerClass(OutcomeAnalysisReducer.class);

        // Define output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Ensure output is different from input
        if (inputPath.equals(outputPath)) {
            System.err.println("Error: Output path must be different from input path!");
            System.exit(-1);
        }

        // Delete output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Deleted existing output directory: " + outputPath);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Execute job
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OutcomeAnalysisDriver(), args);
        System.exit(res);
    }
}