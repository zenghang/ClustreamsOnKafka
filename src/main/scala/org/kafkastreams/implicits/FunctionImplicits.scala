package org.kafkastreams.implicits

/**
  * Created by 11245 on 2018/1/25.
  */
import org.apache.kafka.streams.kstream.Reducer

import scala.language.implicitConversions
object FunctionImplicits {
  implicit def BinaryFunctionToReducer[V](f: ((V, V) => V)): Reducer[V] = (l: V, r: V) => f(l, r)
}
