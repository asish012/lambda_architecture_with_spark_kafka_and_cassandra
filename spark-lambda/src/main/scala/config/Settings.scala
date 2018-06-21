package config

import com.typesafe.config.ConfigFactory

object Settings {
    // typesafe expects resources/application.conf or resources/resource.conf for default configuration
    private val config = ConfigFactory.load()   // loads configuration from application.conf

    object WebLogGen {
        private val clickstream = config.getConfig("clickstream")

        lazy val records        = clickstream.getInt("records")
        lazy val timeMultiplier = clickstream.getInt("time_multiplier")
        lazy val pages          = clickstream.getInt("pages")
        lazy val visitors       = clickstream.getInt("visitors")
        lazy val filePath       = clickstream.getString("file_path")
        // lazy val: the value of the variable will not be evaluated until they are actually used.
    }

}
