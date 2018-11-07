/**
 * class to call mapper and reducer for alphabet count
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author skonduri
 *
 */
public class AlphaDriver{

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		//set mapper and reducer to run
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"myalphacount");
		
		job.setJarByClass(AlphaDriver.class);
		job.setMapperClass(AlphaMapper.class);
		//job.setCombinerClass(AlphaReducer.class);
		job.setReducerClass(AlphaReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		
		//Defining the output value class for the mapper
		
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		Path outputPath = new Path(args[1]);
			
	    //Configuring the input/output path from the filesystem into the job
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
		//deleting the output path automatically from hdfs so that we don't have delete it explicitly
			
		outputPath.getFileSystem(conf).delete(outputPath);
		//exiting the job only if the flag value becomes false
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
