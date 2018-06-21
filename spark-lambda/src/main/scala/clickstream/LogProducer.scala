package clickstream

import java.io.FileWriter

import config.Settings

import scala.util.Random

object LogProducer extends App {
    // If a class extends from App class, the class becomes executable without having the main function
    // We don't need to have the main function with parameters, because we are going to have all our parameters from application.conf file using typesafe lib

    val weblogConfig = Settings.WebLogGen

    val Products  = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
    val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
    val Visitors = (0 to weblogConfig.visitors).map("Visitor-" + _)
    val Pages = (0 to weblogConfig.pages).map(i => s"Page-$i")

    val filePath = weblogConfig.filePath
    val fileWritter = new FileWriter(filePath, true)

    // introduce some randomness to time increments
    val rnd = new Random()
    val incrementTimeEvery = rnd.nextInt(weblogConfig.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to weblogConfig.records) {
        adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * weblogConfig.timeMultiplier)
        timestamp = System.currentTimeMillis()
        var action = iteration % (rnd.nextInt(200) + 1) match {
            case 0 => "purchase"
            case 1 => "add_to_cart"
            case _ => "page_view"
        }

        val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
        val prevPage = referrer match {
            case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
            case _ => ""
        }

        val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
        val page = Pages(rnd.nextInt(Pages.length - 1))
        val product = Products(rnd.nextInt(Products.length - 1))

        val logLine = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
        fileWritter.write(logLine)

        if (iteration % incrementTimeEvery == 0) {
            // random wait
            println(s"Sent $iteration messages!")
            val sleeping = rnd.nextInt(incrementTimeEvery * 60)
            println(s"Sleeping for $sleeping ms")
            Thread sleep sleeping
        }
    }
    fileWritter.close()

}
