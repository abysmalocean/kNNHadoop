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
			
		}

		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

		public int compareTo(DistanceClass o) {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
	
	/*
	public static void bubbleSort(TreeMap<Double, String > Treemap) 
	{
	    int n = Treemap.size();
	    Entry<Double, String> temp = Treemap.firstEntry();

	    for (int i = 0; i < n; i++) {
	        for (int j = 1; j < (n - i); j++) {

	            if (Treemap.values().toArray()[j - 1]  Treemap[j])  {
	                temp = numArray[j - 1];
	                numArray[j - 1] = numArray[j];
	                numArray[j] = temp;
	            }

	        }
	    }
	}
	*/
	
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
		
		
		   public static String getAttemptId(Configuration conf) throws IllegalArgumentException
		   {
			   // This whole function used for chech the mapper ID
		       if (conf == null) {
		           throw new NullPointerException("conf is null");
		       }

		       String taskId = conf.get("mapred.task.id");
		       if (taskId == null) {
		           throw new IllegalArgumentException("Configutaion does not contain the property mapred.task.id");
		       }
		       //System.out.println(taskId);
		       String[] parts = taskId.split("_");
		       if (parts.length != 6 ||
		               !parts[0].equals("attempt") ||
		               (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
		           throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
		       }

		       return parts[4] + "-" + parts[5];
		   }
		
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
					String testFileString = FileUtils.readFileToString(new File(System.getProperty("user.dir")+"/test/smallTest1.arff"));
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
			//StringTokenizer itr = new StringTokenizer(value.toString());
			//System.out.println(getAttemptId(context.getConfiguration()));
			/*
			for (int i = 0; i < TestCaselist.size(); i++)
			{
				System.out.println(TestCaselist.get(i));
			}
			*/
			
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
				one.set( testSum );
				word.set(trainValueString.toString());
				//testSumWriteable.set(testSum);
				
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
					Double distance    = entry.getKey();
					String classbelong = entry.getValue()  ;
					DistanceClass.set(distance,classbelong);
					word.set(Integer.toString(i));
					//System.out.println("Mapper update the value key is "+ word + "value is " + distance + "Class is " + classbelong);
					context.write(word, DistanceClass);		
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,DistanceClass,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		TreeMap<Double, String> KnnInReduce = new TreeMap<Double, String >();
		int K = 10;
		
		public void reduce(Text key, Iterable<DistanceClass> values, Context context) throws IOException, InterruptedException 
		{
			
			for(DistanceClass val : values)
			{
				val.getCate();
				val.getDis();
				KnnInReduce.put(val.getDis(),val.getCate());
				
				if(KnnInReduce.size() > K)
				{
					KnnInReduce.remove(KnnInReduce.lastEntry());
					System.out.println("LiangXu");
				}
				
			}
			for(int i = 0 ; i < K ;i++)
			{
				//System.out.println("value is " + KnnInReduce.);
			}
			//System.out.println("Liang Xu Dis is --->"+values.getDis() + "cat is----->" + values.getCate());
			/*
			
			for (DistanceClass val) 
			{
				System.out.println("Liang Xu Dis is --->"+val.getDis() + "cat is----->" + val.getCate());
				
				//KnnInReduce.put(val.getCate(),val.getDis());
				
				
				*/
			
			DistanceClass DistanceClasstemp = new DistanceClass();
			
			ArrayList<String> knnList = new ArrayList<String>(KnnInReduce.values());
			
			Map<String, Integer> freqMap = new HashMap<String, Integer>();
			for(int i = 0; i < knnList.size();i++)
			{
				System.out.println("Liang Xu");
			}
			
			result.set(K);
			//context.write(key, result);
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
		DistributedCache.addCacheFile(new Path(System.getProperty("user.dir")+"/test/smallTest1.arff").toUri(),job.getConfiguration());
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		
		
		// Setup the Key Value type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DistanceClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
