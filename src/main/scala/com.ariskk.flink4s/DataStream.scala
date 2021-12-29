package com.ariskk.flink4s

import scala.jdk.CollectionConverters._

import org.apache.flink.api.common.functions.{
  FilterFunction,
  FlatMapFunction,
  MapFunction,
  Partitioner
}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{
  DataStream => JavaStream,
  KeyedStream => JavaKeyedStream,
  SingleOutputStreamOperator
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.datastream.DataStreamSink

final case class DataStream[T](stream: JavaStream[T])(using typeInfo: TypeInformation[T]):

  def map[R](f: T => R)(using typeInfo: TypeInformation[R]): DataStream[R] =
    val mapper = new MapFunction[T, R] with ResultTypeQueryable[R]:
      def map(in: T): R                                = f(in)
      override def getProducedType: TypeInformation[R] = typeInfo
    DataStream(stream.map(mapper, typeInfo))

  def flatMap[R](f: T => IterableOnce[R])(using typeInfo: TypeInformation[R]): DataStream[R] =
    val flatMapper = new FlatMapFunction[T, R] with ResultTypeQueryable[R]:
      def flatMap(in: T, out: Collector[R])            = f(in).foreach(out.collect)
      override def getProducedType: TypeInformation[R] = typeInfo
    DataStream(stream.flatMap(flatMapper))

  def filter(f: T => Boolean): DataStream[T] =
    val filter = new FilterFunction[T]:
      def filter(t: T): Boolean = f(t)
    DataStream(stream.filter(filter))

  def filterNot(f: T => Boolean): DataStream[T] = filter(!f(_))

  def collect[R](pf: PartialFunction[T, R])(using typeInfo: TypeInformation[R]): DataStream[R] =
    filter(pf.isDefinedAt _).map(pf)

  def keyBy[K](f: T => K)(using keyTypeInfo: TypeInformation[K]): KeyedStream[T, K] =
    val keyExtractor = new KeySelector[T, K] with ResultTypeQueryable[K]:
      def getKey(in: T)                                = f(in)
      override def getProducedType: TypeInformation[K] = keyTypeInfo
    KeyedStream(new JavaKeyedStream(stream, keyExtractor, keyTypeInfo))

  def union(dataStreams: DataStream[T]*): DataStream[T] =
    DataStream(stream.union(dataStreams.map(_.stream): _*))

  def runAndCollect: List[T] = stream.executeAndCollect().asScala.toList

  def print(): DataStreamSink[T] = stream.print()

end DataStream

object DataStream:
  def apply[T: TypeInformation](stream: SingleOutputStreamOperator[T]): DataStream[T] =
    new DataStream[T](stream.asInstanceOf[JavaStream[T]])
