package org.kafkastreams.keyvalue

/**
  * Created by 11245 on 2018/1/19.
  */
import org.apache.kafka.streams.KeyValue

import scala.language.implicitConversions

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}
