import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 

public class TopN {
	public static class TopNMapper extends Mapper<Object, Text, Text, LongWritable> {
		// Our output key and value Writables
		private TreeMap<Long, String> tmap = new TreeMap<Long, String>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String word = tokens[0];
			long count = Long.parseLong(tokens[1]);
			int n = Integer.parseInt(context.getConfiguration().get("n"));

			tmap.put(count, word);

			if (tmap.size() > n) {
				tmap.remove(tmap.firstKey());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<Long, String> entry : tmap.entrySet())  
	        { 
	  
	            long count = entry.getKey(); 
	            String name = entry.getValue(); 
	  
	            context.write(new Text(name), new LongWritable(count)); 
	        } 
		}
	}

	public static class TopNReducer extends Reducer<Text, LongWritable, LongWritable, Text> {

		private TreeMap<Long, String> tmap = new TreeMap<Long, String>();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			String word = key.toString();
			long count = 0;
			int n = Integer.parseInt(context.getConfiguration().get("n"));

			for (LongWritable val : values) {
				count = val.get();
			}

			tmap.put(count, word);

			if(tmap.size() > n) {
				tmap.remove(tmap.firstKey());
			}
		}

		@Override 
		public void cleanup(Context context) throws IOException, InterruptedException 
	    { 
	  
	        for (Map.Entry<Long, String> entry : tmap.entrySet())  
	        { 
	  
	            long count = entry.getKey(); 
	            String name = entry.getValue(); 
	            context.write(new LongWritable(count), new Text(name)); 
	        } 
	    } 
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Error: please provide two paths and a number");
			System.exit(2);
		}

		int n = Integer.parseInt(otherArgs[2]);
		conf.setInt("n", n);

		Job job = Job.getInstance(conf, "Top N"); 
  
        job.setMapperClass(TopNMapper.class); 
        job.setReducerClass(TopNReducer.class); 
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
}