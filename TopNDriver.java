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

public class TopNDriver {

	public static void main(String []args) throws Exception
	{
		Configuration conf = new Configuration();
		
		conf.set("n", args[2].toString());
		
		String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		//if less than two paths provided will show error
		if(otherArgs.length<2)
		{
			System.err.println("Error: please provide two paths");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf,"top_10 program_2");
		job.setJarByClass(TopNDriver.class);
		
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
		
		String word = tokens[0];
		long  count = Long.parseLong(tokens[1]);
		
		count = (-1)*count;
		
		
		context.write(new LongWritable(count), new Text(word));
	}
}


class top_n_Reducer extends Reducer<LongWritable,Text,LongWritable,Text>{

	static int n;
	@Override
	public void setup(Context context) throws IOException,InterruptedException
	{	
		Configuration conf = context.getConfiguration();
		String n = conf.get("n");           
		count = Integer.parseInt(n);
	}
	
	
	
	@Override
	public void reduce(LongWritable key, Iterable<Text>values, Context context)throws IOException,InterruptedException
	{
		 long count = (-1) * key.get();  
		 String word = null;
		 
		 for(Text val:values)
		 {
			 word = val.toString();
		 }
		 
		 if(n > 0)
		 {
			 context.write(new LongWritable(count), new Text(word));
			 n--;
		 }
	}
}