package com.ariskk.flink4s

import cats.{Monoid, Semigroup}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.datastream.{DataStream as JavaStream, KeyedStream as JavaKeyedStream}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

final case class KeyedStream[T, K](stream: JavaKeyedStream[T, K])(using
    typeInfo: TypeInformation[T],
    keyInfo: TypeInformation[K]
) {

  def combine(using semi: Semigroup[T]): DataStream[T] = reduce(semi.combine)

  def reduce(f: (T, T) => T): DataStream[T] = {
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T): T = f(v1, v2)
    }
    DataStream(stream.reduce(reducer))
  }

  def connect[T2, K2](otherKeyedStream: KeyedStream[T2, K2])(using
      tTypeInfo: TypeInformation[T2],
      kTypeInfo: TypeInformation[K2]
  ): ConnectedStreams[T, T2] =
    ConnectedStreams(stream.connect(otherKeyedStream.stream))

  def countWindow(size: Long): WindowedStream[T, K, GlobalWindow] =
    WindowedStream(stream.countWindow(size))

  def countWindow(size: Long, slide: Long): WindowedStream[T, K, GlobalWindow] =
    WindowedStream(stream.countWindow(size, slide))

}
