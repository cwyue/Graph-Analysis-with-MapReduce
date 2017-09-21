package edu.gatech.cse6242;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	  private IntWritable weight = new IntWritable();
	  private Text target = new Text();

	  public void map(Object key, Text value, Context context
	                  ) throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
	    if(itr.hasMoreTokens()){
	    	String str=itr.nextToken();
	    	if(itr.hasMoreTokens()){
	    		target.set(itr.nextToken()); //use the second string as the target name
	    	}
	    }	    
	    if (itr.hasMoreTokens()) {
	      int w=java.lang.Integer.parseInt(itr.nextToken());
	      weight.set(w);
	      context.write(target, weight);
	    }
	  }
	}

	public static class MaxReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		  int max = 0;
		  for (IntWritable val : values) {
		  	int temp=val.get();
		  	if (temp>max){
		  		max=temp;
		  	}
		  }
		  result.set(max);
		  context.write(key, result);
		}
	  
	}

	public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf, "Q1");

	  /* TODO: Needs to be implemented */
	  job.setJarByClass(Q1.class);
	  job.setMapperClass(TokenizerMapper.class);
	  job.setCombinerClass(MaxReducer.class);
	  job.setReducerClass(MaxReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);

	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}



}

