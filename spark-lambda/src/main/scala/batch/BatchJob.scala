package batch

import java.lang.management.ManagementFactory

import domain.Activity
import utils.SparkUtils._
import org.apache.spark.sql.{SQLContext, SaveMode}

object BatchJob {
    def main(args : Array[String]) : Unit = {
        // setup spark context
        val sc = getSparkContext("Lambda with Spark")
        // sql context
        implicit val sqlContext = new SQLContext(sc)

        // import implicit conversion functions
        import org.apache.spark.sql.functions._
        import sqlContext.implicits._

        //val sourceFile = "file:///Users/asishbiswas/VirtualBox VMs/Vagrant/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"  // to run from IDE
        val sourceFile = "file:///vagrant/data.tsv"     // to run in vm cluster
        val inputRDD = sc.textFile(sourceFile)

        // prepare rdd of activity
        val rawActivityRDD = inputRDD.flatMap(line => {
            val record = line.split("\\t")
            val MS_IN_HOUR = 60 * 60 * 1000
            if (record.length == 7)
                Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
            else
                None
        })

        /*
            RDD Transformation
         */

//        val productActivityMapRDD = rawActivityRDD.keyBy(a => (a.timestamp_hour, a.product)).cache()
//
//        // find visitors by product (how many visitor visited a product)
//        val visitorByProductRDD = productActivityMapRDD.mapValues(a => a.visitor)
//                .distinct() /* distinct of all the values per key */
//                .countByKey()
//        //visitorByProductRDD.foreach(println)
//
//        // same functionality with reduceByKey
//        //val visitorByProductRDD = productActivityMapRDD.mapValues(a => (a.visitor, 1))
//        //        .reduceByKey((t1, t2) => (t1._1, t1._2 + t2._2))
//
//
//        // lets find activity by product (product - (page_view | add_to_cart | purchase))
//        val activityByProductRDD = productActivityMapRDD.mapValues{a =>
//            a.action match {
//                case "purchase"     => (1, 0, 0)
//                case "add_to_cart"  => (0, 1, 0)
//                case "page_view"    => (0, 0, 1)
//            }
//        }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
//        activityByProductRDD.foreach(println)


        /*
            DataFrame Operation with SQL
         */

        val rawActivityDF = rawActivityRDD.toDF()

        // find visitors by product (how many visitor visited a product)
        val activityDF = rawActivityDF.select(
            add_months(from_unixtime(rawActivityDF("timestamp_hour") / 1000), 1).as("timestamp_hour"),
            rawActivityDF("referrer"),
            rawActivityDF("action"),
            rawActivityDF("prevPage"),
            rawActivityDF("page"),
            rawActivityDF("visitor"),
            rawActivityDF("product")
        ).cache()

        activityDF.registerTempTable("activity")
        val visitorByProductDF = sqlContext.sql(
            """SELECT timestamp_hour, product, COUNT(DISTINCT visitor)
              |FROM activity
              |GROUP BY timestamp_hour, product
            """.stripMargin)
        visitorByProductDF.printSchema()
        visitorByProductDF.foreach(println)

        val activityByProductDF = sqlContext.sql(
            """SELECT
              |timestamp_hour,
              |product,
              |sum(case when action = 'purchase'    then 1 else 0 end) as purchase_count,
              |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
              |sum(case when action = 'page_view'   then 1 else 0 end) as page_view_count
              |FROM activity
              |GROUP BY timestamp_hour, product
            """.stripMargin).cache()
        activityByProductDF.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")   // action
        activityByProductDF.foreach(println)    // action

    }

}
