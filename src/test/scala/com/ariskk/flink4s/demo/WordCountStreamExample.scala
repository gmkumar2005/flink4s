package com.ariskk.flink4s.demo
import cats.Semigroup
import com.ariskk.flink4s.StreamExecutionEnvironment
import com.ariskk.flink4s.TypeInfo.{intTypeInfo, stringTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation

given typeInfoTuple: TypeInformation[(String, Int)] = TypeInformation.of(classOf[(String, Int)])
final case class Counter(id: String, count: Int)

object Counter:
  given typeInfo: TypeInformation[Counter] = TypeInformation.of(classOf[Counter])

given semigroup: Semigroup[Counter] with
  def combine(x: Counter, y: Counter): Counter = Counter(x.id, x.count + y.count)

  @main def main(): Unit = {
    countUsingCats()

  }

def countUsingCats(): Unit = {
  val senv  = StreamExecutionEnvironment.getExecutionEnvironment
  val items = (1 to 10).map(x => s"item-${x % 10}")
  val stream = senv
    .fromCollection(items)
    .map(x => Counter(x, 1))
    .keyBy(_.id)
    .combine
    .runAndCollect

  pprint.pprintln("Results : " + stream)
}
def countWords(): Unit = {
  val senv = StreamExecutionEnvironment.getExecutionEnvironment
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
def countItems(): Unit = {
  println("Hello, World! countItems")
  val items = (1 to 10).map(x => s"item-${x % 10}")
  val senv  = StreamExecutionEnvironment.getExecutionEnvironment
  val stream = senv
    .fromCollection(items)
    .map(x => Counter(x, 1))
    .keyBy(_.id)
    .reduce((acc, v) => acc.copy(count = acc.count + v.count))
    .runAndCollect
  val expectedValues = List(
    Counter("item-0", 1),
    Counter("item-3", 1),
    Counter("item-5", 1),
    Counter("item-6", 1),
    Counter("item-2", 1),
    Counter("item-7", 1),
    Counter("item-1", 1),
    Counter("item-4", 1),
    Counter("item-9", 1),
    Counter("item-8", 1)
  )
  pprint.pprintln("Results : " + stream)
  pprint.pprintln("Expected results : " + expectedValues)
}
