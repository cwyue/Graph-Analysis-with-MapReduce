package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Q2 {

	def main(args: Array[String]) {
    	val sc = new SparkContext(new SparkConf().setAppName("Q2"))
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
		import org.apache.spark.sql.Row;	
		import org.apache.spark.sql.types.{StructType,StructField,StringType}

    	// read the file and create dataFrame
    	
    	val file = sc.textFile("hdfs://localhost:8020" + args(0))
	    val DF = file.map{line =>
	    	     val col = line split '\t'
	    	     (col(0), col(1), col(2).toInt)}
	    	     .toDF("source","target","weight")

		//create two subsets recording the incoming and outgoing weights of each node
		val frame1=DF.filter("weight!=1").groupBy("source").agg(sum("weight")).withColumnRenamed("sum(weight)","out")
		val frame2=DF.filter("weight!=1").groupBy("target").agg(sum("weight")).withColumnRenamed("sum(weight)","in")
		
		//full outjoin between two frames
		val myUDF = udf[String,String,String]((source: String, target: String) => {
		  if (target == null) source
		  else target
		})
		val frame=frame1.join(frame2, $"source" === $"target", "outer").na.fill(Map("in" -> 0, "out" -> 0)).withColumn("node", myUDF($"source", $"target"))		
		val frame_new=frame.select(frame("node"), frame("in") - frame("out"))

		// store output on given HDFS path.
		val expr = concat_ws("\t", frame_new.columns.map(col): _*)
		frame_new.select(expr).map(_.getString(0)).saveAsTextFile("hdfs://localhost:8020" + args(1))
		
  	}
}
