/**
 * Copyright (C) 2020 Christophe Botek or Sparkles contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

package ca.botekchristophe.sparkes.core.datasources

/**
 * A DataStream is generaly representing a queue in a System like Apache Kafka.
 */
trait DataStream extends DataSource {

  /**
   * A topic is a unique name defining where the data stream will be sitting in the message queue.
   */
  val topic: String
}
