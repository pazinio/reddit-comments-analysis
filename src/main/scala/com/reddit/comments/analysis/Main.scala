package com.reddit.comments.analysis

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

case class AuthorBodySubReddit(author: String, body: String, subreddit: String)
case class SubredditTotal(subreddit: String, total: Int)


object Main {
  def main(args: Array[String]): Unit = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd hh-mm-ssZ")
    val now = format.format(new java.util.Date())

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // params
    //    val master = "local[*]"
    if (args.size < 3) {
      println("Please provide requested params")
      return
    }

    val innerPath = args(0)
    val whichPart = if (args(1) == "all") '*' else args(1)


    val terms = args.tail.tail

    val path = s"adl://mydatalakestore77.azuredatalakestore.net/reddit/${innerPath}/${whichPart}"
    println("args:" + args.mkString(","))
    println("path:" + path)
    println("terms:" + terms.mkString(","))

    val spark = SparkSession
      .builder
      .appName("reddit-comments-analysis")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")

    import spark.implicits._

    spark.time {
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

      relevantDS.cache()

      /**
        * for a given term filter relevant comments
        */
      terms.foreach(term => {
        val lowerCaseTerm = term.toLowerCase
        spark.time {
          val result = relevantDS
            .flatMap(d => {
              val arr = d.body.trim.split(" ")
              val res = arr.map(a => (d.subreddit, a.toLowerCase))
              res
            })
            .filter(d => d._2 == lowerCaseTerm)
            .map(d => (d._1, 1)).rdd
            .reduceByKey(_ + _).toDS
            .map(x => SubredditTotal(subreddit = x._1, total = x._2))
            .orderBy($"total".desc)
            .limit(10)

          println(s"Given a single-word term: ||'${term}'||, determine what sub-reddit it appears in and how many times (Case-insensitive):")
          val outputPath =  s"adl://mydatalakestore77.azuredatalakestore.net/output/${now}/${term}"
          println("outputPath: " + outputPath)
          result.coalesce(1).write.mode("overwrite").csv(outputPath)
        }
      }
      )


    }
  }
}
