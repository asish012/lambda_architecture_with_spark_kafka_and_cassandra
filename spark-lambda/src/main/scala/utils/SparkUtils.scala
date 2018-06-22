package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SparkUtils {

    val isIDE = {
        ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }

    def getSparkContext(appName : String) = {
        // checkpoint directory
        var checkpointDirectory = ""

        // get spark configuration
        val conf = new SparkConf().setAppName(appName)

        // Check if running from IDE
        if (isIDE) {
            conf.setMaster("local[*]")
            checkpointDirectory = "file:///Users/asishbiswas/VirtualBox VMs/Vagrant/spark-kafka-cassandra-applying-lambda-architecture/vagrant/checkpoint"
        } else {
            checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
        }

        // setup spark context
        val sc = SparkContext.getOrCreate(conf)
        sc.setCheckpointDir(checkpointDirectory)
        sc
    }

    def getSparkSqlContext(sc: SparkContext) = {
        // get sql context
        implicit val sqlContext = SQLContext.getOrCreate(sc)
        sqlContext
    }
}
