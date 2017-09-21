package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;

public class Q4 {

  public static void main(String[] args) throws Exception {
    String str=args[1];
    int index=str.indexOf("windows.net");
    String OUTPUT_PATH=str.substring(0,index)+"windows.net/temp"+str.substring(index+12);

    Configuration conf = new Configuration();    
    Job job1 = Job.getInstance(conf, "Job1");

    job1.setJarByClass(Q4.class);
    job1.setMapperClass(Mapper1.class);
    job1.setCombinerClass(Reducer1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    //job1.setOutputValueClass(Text.class);    
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    //FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));
    //System.exit(job1.waitForCompletion(true) ? 0 : 1);
    job1.waitForCompletion(true); 
       
    Job job2 = Job.getInstance(conf, "Job2");
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(Mapper2.class);
    job2.setCombinerClass(Reducer1.class);
    job2.setReducerClass(Reducer1.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class); 
    //FileInputFormat.addInputPath(job2, new Path(args[0])); 
    FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1); 
    
  }

  public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
  	private final static IntWritable one = new IntWritable(1);
  	private Text node = new Text();
   	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   		StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
     
 	    while (itr.hasMoreTokens()) {
 	      node.set(itr.nextToken());
 	      context.write(node, one);
 	    }
 	}

  }

  public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {
  	IntWritable result = new IntWritable();
  	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
  	  int sum = 0;
  	  for (IntWritable val : values) {
  	    sum += val.get();
  	  }
  	  result.set(sum);
  	  context.write(key, result);
  	}    
  }

  public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>{
  	private final static IntWritable one = new IntWritable(1);
  	private Text degree = new Text();
   	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   		String[] valueItems = value.toString().split("\t");
   		degree.set(valueItems[1]);
   		context.write(degree, one);
 	}
  }

}

