package org.neoflex

import com.sun.org.slf4j.internal.{Logger, LoggerFactory}

trait Logging {
  @transient val Log: Logger = LoggerFactory.getLogger(this.getClass)
}
