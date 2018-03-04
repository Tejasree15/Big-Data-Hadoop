package training.collabera.distributedCache;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver{
	
	public static void main(String[] args) throws Exception{
			
		if (args.length != 3) {
			System.err.printf("need three arguments");
			
		}
	
		//Initialize the Hadoop job and set the jar as well as the name of the Job
		Job job = new Job();
		job.setJarByClass(Driver.class);
		job.setJobName("Word Counter With Stop Words Removal");
		
		//Add input and output file paths to job based on the arguments passed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the MapClass and ReduceClass in the job
		job.setMapperClass(DCMapper.class);
		job.setReducerClass(DCReducer.class);
		//job.setNumReduceTasks(0);
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
	
		//Wait for the job to complete and print if the job was successful or not
		System.exit(job.waitForCompletion(true) ? 0:1);
		
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}
		
	}
}
