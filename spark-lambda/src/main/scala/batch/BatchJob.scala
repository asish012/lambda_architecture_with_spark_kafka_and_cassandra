package batch

import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.{SparkConf, SparkContext}

object BatchJob {
    def main(args : Array[String]) : Unit = {
        // get spark configuration
        val conf = new SparkConf().setAppName("Lambda architecture with Spark")

        // Check if running from IDE
        if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
            conf.setMaster("local[*]")
        }

        // setup spark context
        val sc = new  SparkContext(conf)

        val sourceFile = "file:////Users/asishbiswas/VirtualBox VMs/Vagrant/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
        val inputRDD = sc.textFile(sourceFile)

        val rawActivityRDD = inputRDD.flatMap(line => {
            val record = line.split("\\t")
            val MS_IN_HOUR = 60 * 60 * 1000
            if (record.length == 7)
                Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
            else
                None
        })

        val productActivityMapRDD = rawActivityRDD.keyBy(a => (a.timestamp_hour, a.product)).cache()

        // find visitors by product (how many visitor visited a product)
        val visitorByProductRDD = productActivityMapRDD.mapValues(a => a.visitor)
                .distinct() /* distinct of all the values per key */
                .countByKey()
        //visitorByProductRDD.foreach(println)

        // same functionality with reduceByKey
        //val visitorByProductRDD = productActivityMapRDD.mapValues(a => (a.visitor, 1))
        //        .reduceByKey((t1, t2) => (t1._1, t1._2 + t2._2))


        // lets find activity by product (product - (page_view | add_to_cart | purchase))
        val activityByProductRDD = productActivityMapRDD.mapValues{a =>
            a.action match {
                case "purchase"     => (1, 0, 0)
                case "add_to_cart"  => (0, 1, 0)
                case "page_view"    => (0, 0, 1)
            }
        }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
        activityByProductRDD.foreach(println)

    }

}
