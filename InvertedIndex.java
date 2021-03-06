import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex {
	public class InvertedMap extends Mapper<LongWritable,Text,Text,Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
		{
			String file = ((FileSplit) context.getInputSplit()).getPath().getName();
			String line = value.toString();
			String words[] = line.split(" ");

			for(String word : words){
				context.write(new Text(word), new Text(file));
			}
		}
	}

	public static class InvertedReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap fileMap = new HashMap();
			int count = 0;

			for(Text currWord : values) {
				String wordStr = currWord.toString();

				if(fileMap != null && fileMap.get(wordStr) != null) {
					count = (int)fileMap.get(wordStr);
					count++;
					fileMap.put(wordStr, count);
				}
				else {
					fileMap.put(wordStr, 1);
				}
			}
			context.write(key, new Text(fileMap.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "InvertedIndex");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedMap.class);
		job.setReducerClass(InvertedReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}