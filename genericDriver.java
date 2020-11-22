import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class genericDriver {

	public static void main(String []args) throws Exception
	{
	Configuration conf = new Configuration();
	
	/* here we set our own custom parameter  myValue with 
	 * default value 10. We will overwrite this value in CLI
	 * at runtime.
	 * Remember that both parameters are Strings and we 
	 * have convert them to numeric values when required.
	 */
	
	conf.set("myValue", args[2].toString());
	
	String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	
	//if less than two paths provided will show error
	if(otherArgs.length<2)
	{
		System.err.println("Error: please provide two paths");
		System.exit(2);
	}
	
	Job job = Job.getInstance(conf,"top_10 program_2");
	job.setJarByClass(genericDriver.class);
	
	job.setMapperClass(top_n_Mapper.class);
	job.setReducerClass(top_n_Reducer.class);
	
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);

	job.setNumReduceTasks(1);
	
	FileInputFormat.addInputPath(job, new Path (otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	System.exit(job.waitForCompletion(true)? 0:1);
	}
}



class top_n_Mapper extends Mapper<Object,Text,LongWritable,Text>{
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
	String []tokens = value.toString().split("\t");
		
		String movie_name = tokens[0];
		long  no_of_views = Long.parseLong(tokens[1]);
		
		no_of_views = (-1)*no_of_views;
		
		
		context.write(new LongWritable(no_of_views), new Text(movie_name));
	}
}


class top_n_Reducer extends Reducer<LongWritable,Text,LongWritable,Text>{

	static int count;
	

	@Override
	public void setup(Context context) throws IOException,InterruptedException
	{	
		
		Configuration conf = context.getConfiguration();
		 
		String  param = conf.get("myValue");           
		
		
		//converting the String value to integer
		 count = Integer.parseInt(param);
	
	}
	
	
	
	@Override
	public void reduce(LongWritable key, Iterable<Text>values, Context context)throws IOException,InterruptedException
	{
		
		
		 long no_of_views = (-1) * key.get();  
		 String movie_name = null;
		 
		 for(Text val:values)
		 {
			 movie_name = val.toString();
		 }
		 
	
		
		 
		 if(count > 0)
		 {
			 context.write(new LongWritable(no_of_views), new Text(movie_name));
			 count--;
		 }
	}
}