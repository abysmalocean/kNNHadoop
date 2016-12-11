package kNN2.kNNrun;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;

import weka.core.Instances;
import weka.core.converters.ArffLoader;

public class run {
	
	public static class DistanceClass implements WritableComparable< DistanceClass >
	{
		private Double distance =0.00;
		private String category = null;
		
		public void set(Double dis, String cate)
		{
			this.distance = dis;
			this.category = cate;
		}
		public Double getDis()
		{
			return this.distance;
		}
		
		public String getCate()
		{
			return this.category;
		}
		
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			distance = arg0.readDouble();
			category = arg0.readUTF();
			
		}

		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeDouble(distance);
			arg0.writeUTF(category);
		}

		public int compareTo(DistanceClass o) {
			// TODO Auto-generated method stub
			return (this.category).compareTo(o.category);
		}
		public int hashCode() {
			return Integer.parseInt(category);
			
		}
		
		@Override
		public String toString() {
		       return "Class --- > " + this.category;
		    }
		
	}
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DistanceClass>{

		private final static DoubleWritable one = new DoubleWritable(0.000);
		private Text word = new Text();
		private double testSum = 0.0000;
		ArrayList<String> TestCaselist = new ArrayList<String>();
		DistanceClass distanceAndModel = new DistanceClass();
		
		
		private int k = 10;
		//TreeMap<Double, String> Kmap = new TreeMap<Double, String >();
		ArrayList< TreeMap<Double, String> > Kmaplist = new ArrayList< TreeMap<Double, String> >() ;
		DistanceClass DistanceClass = new DistanceClass();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			//System.out.println("Liang Xu in setup");
			// suppose load the test date set
			try
			{
				Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				//System.out.println("Local file is (Liang Xu)" + localFiles);
				//System.out.println("Local file length is (Liang Xu)" + localFiles.length);
				if(localFiles != null && localFiles.length > 0) {
					String testFileString = FileUtils.readFileToString(new File(System.getProperty("user.dir")+"/test/Test.arff"));
					//System.out.println("Liang Xu in the mapper and the file length is " + testFileString.length());
					StringTokenizer testValues = new StringTokenizer(testFileString, "\n");
					String temp = null;
					
					while (testValues.hasMoreTokens()) {
						temp = testValues.nextToken();
						TestCaselist.add(temp);
					}
				}

			}catch(IOException ex) {
				System.err.println("Exception in mapper setup: " + ex.getMessage());
			}
			
			for (int i = 0; i < TestCaselist.size(); i++)
			{
				TreeMap<Double, String> Kmap = new TreeMap<Double, String >();
				Kmaplist.add(Kmap);
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			for (int i = 0; i < TestCaselist.size(); i++)
			{
				
				StringTokenizer itr = new StringTokenizer(value.toString(),",");
				StringTokenizer testitr = new StringTokenizer(TestCaselist.get(i).toString(),",");
				String trainValueString = null;
				double trainValueDouble = 0.00;
				double testValueDouble = 0.00;
				while (itr.hasMoreTokens()) 
				{
					trainValueString = itr.nextToken();
					trainValueDouble = Double.parseDouble(trainValueString);
					if(itr.hasMoreTokens())
					{
						//make sure this token is not the last one
						testValueDouble =  Double.parseDouble(testitr.nextToken());
						testSum =testSum + Math.pow(trainValueDouble-testValueDouble, 2);
					}
					//System.out.println(word + " " + (int)testSum);
				}
				
				//System.out.format("Distance between test list [ " + i +  " ]to the test is [ %10.1f ] -------->"+"[ " + trainValueString.toString() + " ] \n" ,testSum);
				Kmaplist.get(i).put( testSum,trainValueString.toString());
				
				if(Kmaplist.get(i).size()>k)
				{
					//System.out.println("new distance is " + testSum + "get remove " + Kmaplist.get(i).get(Kmaplist.get(i).lastKey()));
					Kmaplist.get(i).remove(Kmaplist.get(i).lastKey());
				}
				//context.write(word, one);
				testSum = 0.00;
				trainValueString = null;
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			for (int i = 0; i < TestCaselist.size(); i++)
			{
				for(Map.Entry<Double, String> entry : Kmaplist.get(i).entrySet() )
				{
					//DistanceClass distanceAndModelout = new DistanceClass();
					distanceAndModel.set(entry.getKey(),entry.getValue());
					word.set(Integer.toString(i));
					//System.out.println("Mapper update the value key is "+ word + " value is " + distanceAndModelout.getDis() + " Class is " + distanceAndModelout.getCate());
					context.write(word, distanceAndModel);		
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,DistanceClass,Text,DistanceClass> {
		private DistanceClass result = new DistanceClass();
		TreeMap<Double, String> KnnInReduce = new TreeMap<Double, String >();
		ArrayList< TreeMap<Double, String> > Kmaplist = new ArrayList< TreeMap<Double, String> >() ;
		private Text wordvalue = new Text();
		int K = 10;
		
		public void reduce(Text key, Iterable<DistanceClass> values, Context context) throws IOException, InterruptedException 
		{
			//System.out.println("Key is " + key);

			for(DistanceClass val : values)
			{
				//System.out.println("Liang Xu Dis is --->"+val.getDis() + " cat is----->" + val.getCate());
				KnnInReduce.put(val.getDis(),val.getCate());
				
				if(KnnInReduce.size() > K)
				{
					KnnInReduce.remove(KnnInReduce.lastEntry());
					//System.out.println("LiangXu");
				}
				
			}
				
			DistanceClass DistanceClasstemp = new DistanceClass();
			
			ArrayList<String> knnList = new ArrayList<String>(KnnInReduce.values());
			
			Map<String, Integer> freqMap = new HashMap<String, Integer>();
			
			//System.out.println("knnList Size ------>" + knnList.size());
			
			for(int i = 0; i < knnList.size();i++)
			{
				//System.out.println("Key is [ " + key + " ] knnList get [ " + i + " ]  ------>" + knnList.size());
				Integer frequency = freqMap.get(knnList.get(i));
		        if(frequency == null)
		        {
		            freqMap.put(knnList.get(i), 1);
		        } else
		        {
		            freqMap.put(knnList.get(i), frequency+1);
		        }
			}
			
		    String mostCommonModel = null;
		    int maxFrequency = -1;
		    
		    for(Map.Entry<String, Integer> entry: freqMap.entrySet())
		    {
		        if(entry.getValue() > maxFrequency)
		        {
		            mostCommonModel = entry.getKey();
		            maxFrequency = entry.getValue();
		        }
		    }
			result.set((double)Integer.parseInt(mostCommonModel),"finish");
			//System.out.println("Test " + key + " is blong to " + mostCommonModel);
			wordvalue.set(mostCommonModel);
			result.set(0.00,mostCommonModel);
			String temp = key.toString();
			
			//System.out.println("temp is" + temp);
			temp = temp + " case";
			//System.out.println("temp is" + temp);
			key.set(temp);
			context.write(key, result);
			KnnInReduce.clear();
		}
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
		
		/*
		try{
			BufferedReader reader = new BufferedReader(new FileReader(args[0]));
			data = new Instances(reader);
			reader.close();
		}catch (NumberFormatException e) {
			System.out.println("Can not load the data file\n");
        }
		//System.out.println(data.get(1) );
		
		predictions(data);

		*/
		
		Configuration conf = new Configuration();
		//JobConf job = new JobConf(conf,run.class);
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(run.class);
		
		// Test data separation
		DistributedCache.addCacheFile(new Path(System.getProperty("user.dir")+"/test/Test.arff").toUri(),job.getConfiguration());
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		
		// Setup the Key Value type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DistanceClass.class);
		
		// Input file and out put file foler
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if(job.waitForCompletion(true))
		{
			long endTime   = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			//totalTime = TimeUnit.MILLISECONDS.toSeconds(totalTime);
			System.out.println("total time mill second spend is " + totalTime);
			System.exit(1);
		}
	}

}
