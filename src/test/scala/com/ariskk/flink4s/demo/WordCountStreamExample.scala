package com.ariskk.flink4s.demo
import com.ariskk.flink4s.StreamExecutionEnvironment
import org.apache.flink.api.common.typeinfo.TypeInformation
import com.ariskk.flink4s.TypeInfo.stringTypeInfo
import com.ariskk.flink4s.TypeInfo.intTypeInfo

given typeInfoTuple: TypeInformation[(String, Int)] = TypeInformation.of(classOf[(String, Int)])
final case class Counter(id: String, count: Int)

object Counter:
  given typeInfo: TypeInformation[Counter] = TypeInformation.of(classOf[Counter])

  @main def main(): Unit = {
    println("Hello, World!")
    val items = (1 to 100).map(x => s"item-${x % 10}")
    val senv  = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = senv
      .fromCollection(items)
      .map(x => Counter(x, 1))
      .keyBy(_.id)
      .reduce((acc, v) => acc.copy(count = acc.count + v.count))
      .print()
    senv.execute

    val streamingLines = Seq(
      "To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,"
    )
    val textStreaming = senv.fromCollection(streamingLines)
    val countsStreaming = textStreaming
      .flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .keyBy(_._1)
      .reduce((acc, v) => acc.copy(_2 = acc._2 + v._2))
      .print()
    senv.execute
    println("Completed execution of example")

  }
