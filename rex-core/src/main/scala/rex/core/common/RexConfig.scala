package rex.core.common

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by Shubham Gupta on 12/6/2017.
  */

/*
* utility for application configuration object
*/
trait RexConfig {

  implicit lazy val config: Config = RexConfig.config
}

// private object initializing one time application configurations
private[common] object RexConfig {
  lazy val config: Config = ConfigFactory.load("rex-core-local.conf")
}