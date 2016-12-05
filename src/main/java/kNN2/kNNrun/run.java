package kNN2.kNNrun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
	
	public static void main(String[] args) throws Exception 
	{
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
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "kNN");
		job.setJarByClass(run.class);
		
		predictions(data);

		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("total time mill second spend is " + totalTime);
		
		System.out.print("LiangXu\n");
	}

}
