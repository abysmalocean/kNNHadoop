package kNN2.kNNrun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.core.Instances;
import weka.core.converters.ArffLoader;

public class run {
	public static int[] predictions(Instances data)
	{
		System.out.println("data instace is " + data.numInstances());
		//for(int i = 0 ; i <data.numInstances();i++)
		for(int i = 0 ; i <2;i++)
		{
			double smallestDistance = Double.MAX_VALUE ;
			int smallestDistanceClass;
			//for(int j = 0;  j <data.numInstances(); j++) // target each other instance
			for(int j = 0;  j < 2; j++) // target each other instance
	        {
				if(j == i) continue;
				double distance = 0 ;
				for(int k = 0; k < data.numAttributes() - 1 ; k++)
				{
					//System.out.println("(data.get(i).index(k))----->" + (data.get(i).value(k)) + "\n"  
					//		+ "(data.get(i).index(k))----->" + (data.get(j).value(k)));
					double diff = data.get(i).value(k) - data.get(j).value(k); 
					distance += diff * diff ; 
					//System.out.println("Distance is [ " + distance + " ]");
				}
				distance = Math.sqrt(distance);
				if(distance < smallestDistance)
				{
					smallestDistance = distance;
				}
			
	        }
		}
		return null;
		
	}
	
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private IntWritable testSumWriteable;
		private double testSum = 0.0000;
		private int k = 10;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			try
			{
				Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if(localFiles != null && localFiles.length > 0) {
					System.out.println("Liang Xu");
				}

			}catch(IOException ex) {
				System.err.println("Exception in mapper setup: " + ex.getMessage());
			}
			
			//String stringValue = FileUtils.readFileToString(new File("/home/liangxu/workspace/kNNHadoop/train"));
		}
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//StringTokenizer itr = new StringTokenizer(value.toString());
			
			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			String temp = null;
			while (itr.hasMoreTokens()) {
				temp = itr.nextToken();
				testSum =testSum + Double.parseDouble(temp);
				//System.out.println(word + " " + (int)testSum);
			}
			one.set( (int) testSum );
			word.set(temp);
			//testSumWriteable.set(testSum);
			context.write(word, one);
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			
			for (IntWritable val : values) 
			{
				//System.out.println("Liang Xu");
				System.out.print(values);
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}

	
	public static void main(String[] args) throws Exception 
	{
		/*
		System.out.println(args[0]);
		if(args.length != 2)
		{
			System.out.print("Usage: kNNrun datasets/small.arff output");
		}
		long startTime = System.currentTimeMillis();
		
		Instances data = null;
		try{
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
			data = new Instances(reader);
			reader.close();
		}catch (NumberFormatException e) {
			System.out.println("Can not load the data file\n");
        }
		//System.out.println(data.get(1) );
		
		predictions(data);

		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("total time mill second spend is " + totalTime);
		System.out.print("LiangXu\n");
		*/
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(run.class);
		
		// Test data separation
		DistributedCache.addCacheFile(new Path("/home/liangxu/workspace/kNNHadoop/train").toUri(),job.getConfiguration());
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		
		
		// Setup the Key Value type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Input file and out put file foler
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");	
		}
	}

}
