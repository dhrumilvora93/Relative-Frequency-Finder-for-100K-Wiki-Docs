import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindRelativeFreqDriver
{
	 public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
    	Configuration conf = new Configuration();
    	Job job1 = Job.getInstance(conf);
    	job1.setJarByClass(FindRelativeFreqDriver.class);
        job1.setJobName("finding_relative_frequency");

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_o"));

        job1.setMapperClass(FreqMapper.class);
        job1.setReducerClass(FreqReducer.class);
        job1.setCombinerClass(NextWordReducer.class);
        job1.setPartitionerClass(NextWordPartitioner.class);
        job1.setNumReduceTasks(3);

        job1.setOutputKeyClass(NextWord.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.waitForCompletion(true);

        Configuration conf1 = new Configuration();
    	  Job job2 = Job.getInstance(conf1);
    	  job2.setJarByClass(FindRelativeFreqDriver.class);
    	  job2.setJobName("finding_relative_frequency");

    	  job2.setSortComparatorClass(IntermediateComparator.class);
        FileInputFormat.addInputPath(job2, new Path("temp_o"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));


        job2.setMapperClass(FreqMapper2.class);
        job2.setReducerClass(FreqReducer2.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(NextWord.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}