package com.reddit.comments.analysis

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

case class AuthorBodySubReddit(author: String, body: String, subreddit: String)
case class SubredditTotal(subreddit: String, total: Int)


object Main {
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("args length:" + args.length)
    println("args:" + args.mkString("#"))

    // params
    val master = "local[*]"
    val path = "*.json" //org.apache.spark.SparkFiles.get("input.json")
    val term = args.head.toLowerCase // "love".toLowerCase

    val spark = SparkSession
      .builder
      .appName("reddit-comments-analysis")
      .config("spark.master", master)
      .getOrCreate()


    import spark.implicits._

    /**
      * reading reddit comments data
      */
    println("read data as data frame")
    val df = spark.read
      .option("header", "true")
      .json(path).toDF
    df.show(2)

    println("convert dataframe to specific relevant data set columns")
    val ds =
      df.select("author", "body", "subreddit")
      .as[AuthorBodySubReddit]
    ds.show(10)

  /**
    * for a given term filter relevant comments
    */
  val result = ds
    .filter(d => d.body.contains(term))
    .map(d => (d.subreddit, 1)).rdd
    .reduceByKey(_+_).toDS
    .map(x => SubredditTotal(subreddit = x._1, total = x._2))
    .orderBy($"total".desc)

    println(s"Given a single-word term: ${term}, determine what sub-reddit it appears in and how many times (Case-insensitive):")
    result.show()
  }
}
