package batch

import java.lang.management.ManagementFactory

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

        // simple action. since inputRDD is a rdd of string, foreach expects a function that takes a string and returns Unit (aka void).
        inputRDD.foreach(println)

    }

}
