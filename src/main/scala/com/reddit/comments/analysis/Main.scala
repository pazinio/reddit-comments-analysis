package com.reddit.comments.analysis

import org.apache.hadoop.hdfs.server.namenode.SafeMode
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

case class AuthorBodySubReddit(author: String, body: String, subreddit: String)
case class SubredditTotal(subreddit: String, total: Int)


object Main {
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

//    println("args length:" + args.length)
//    println("args:" + args.mkString("#"))

    // params
//    val master = "local[*]"
    if (args.size < 1) {
      println("Please provide requested params")
      return
    }

    val innerPath = args(0)
    val terms = args.tail

    val path ="*.json"//s"adl://mydatalakestore77.azuredatalakestore.net/reddit/${innerPath}/*"
    println("args:" + args.mkString(","))
    println("path:" + path)
    println("terms:" + terms.mkString(","))

    val spark = SparkSession
      .builder
      .appName("reddit-comments-analysis")
      .config("spark.master", "local[*]")
      .getOrCreate()


    import spark.implicits._

    /**
      * reading reddit comments data
      */
    println("Read data as data frame ...")
    val df = spark.time {
      spark.read
        .option("header", "true")
        .json(path).toDF
    }

    println("Convert dataframe to specific relevant data set columns ...")
    val ds = spark.time {
        df.select("author", "body", "subreddit")
          .as[AuthorBodySubReddit]
    }

    val relevantDS = spark.time {
      ds.filter(d => terms.exists(d.body.contains(_)))
    }

    /**
     * for a given term filter relevant comments
     */
    terms.foreach(term => {
      spark.time { val result = relevantDS
        .filter(d => d.body.contains(term))
        .map(d => (d.subreddit, 1)).rdd
        .reduceByKey(_ + _).toDS
        .map(x => SubredditTotal(subreddit = x._1, total = x._2))
        .orderBy($"total".desc)
        .limit(10)

      println(s"Given a single-word term: ||'${term}'||, determine what sub-reddit it appears in and how many times (Case-insensitive):")
      val outputPath = term//s"adl://mydatalakestore77.azuredatalakestore.net/output/${term}"
      println("outputPath: " + outputPath)
      result.coalesce(1).write.mode("overwrite").csv(outputPath)
    }}
  )


  }
}
