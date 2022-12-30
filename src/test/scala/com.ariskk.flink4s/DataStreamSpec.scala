package com.ariskk.flink4s

import cats.Semigroup
import com.ariskk.flink4s.TypeInfo.{intTypeInfo, stringTypeInfo}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable.Buffer as MutableBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

given semigroup: Semigroup[Counter] with
  def combine(x: Counter, y: Counter): Counter = Counter(x.id, x.count + y.count)

final case class Counter(id: String, count: Int)

object Counter:
  given typeInfo: TypeInformation[Counter] = TypeInformation.of(classOf[Counter])
given typeInfoTuple: TypeInformation[(String, Int)] =
  TypeInformation.of(classOf[(String, Int)])

final class DataStreamSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    FlinkExecutor.startCluster()
  }

  override def afterAll(): Unit = {
    FlinkExecutor.stopCluster()
  }

  describe("DataStreamSpec") {
    it("should map data") {
      val env     = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val range   = (1 to 5).toList
      val results = env.fromCollection(range).map(_ + 1).runAndCollect
      results should contain theSameElementsAs (2 to 6).toList
    }

    it("should flatMap data") {
      val env   = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val range = (1 to 10).toList
      val results =
        env.fromCollection(range).flatMap(x => Option.when(x > 5)(x)).runAndCollect
      results should contain theSameElementsAs (6 to 10).toList
    }

    it("should filter data") {
      val env     = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val range   = (1 to 10).toList
      val results = env.fromCollection(range).filter(_ > 5).runAndCollect
      results should contain theSameElementsAs (6 to 10).toList
    }

    it("should filter out data") {
      val env     = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val range   = (1 to 10).toList
      val results = env.fromCollection(range).filterNot(_ > 5).runAndCollect
      results should contain theSameElementsAs (1 to 5).toList
    }

    it("should collect data") {
      val env     = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val range   = (1 to 10).toList
      val results = env.fromCollection(range).collect { case x if x > 5 => s"$x" }.runAndCollect
      results should contain theSameElementsAs (6 to 10).map(_.toString).toList
    }

    it("should union homogeneous streams") {
      val env     = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val stream1 = env.fromCollection((1 to 10).toList)
      val stream2 = env.fromCollection((11 to 20).toList)
      val stream3 = env.fromCollection((21 to 30).toList)
      val results = stream1.union(stream2, stream3).runAndCollect
      results should contain theSameElementsAs (1 to 30).toList
    }

    it("should be able to accept sinks") {
      val env      = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val elements = (1 to 10).toList
      val stream   = env.fromCollection(elements)
      stream.addSink(DataStreamSpec.intCollector)
      env.execute
      DataStreamSpec.values.toList should contain theSameElementsAs elements
    }

    it("should execute async functions and return the results in order") {
      val env      = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val elements = (1 to 20).toList
      val results = env
        .fromCollection(elements)
        .orderedMapAsync(x => Future(x + 1))
        .runAndCollect

      results should contain theSameElementsAs (2 to 21).toList

    }

    it("should execute async functions and return the results in any order") {
      val env      = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val elements = (1 to 20).toList
      val results = env
        .fromCollection(elements)
        .unorderedMapAsync(x => Future(x + 1))
        .runAndCollect
      results should contain theSameElementsAs (2 to 21).toList

    }

    it("should execute a simple item count") {

      val env   = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val items = (1 to 10).map(x => s"item-${x % 10}")
      val results = env
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

      results should contain theSameElementsAs expectedValues

    }
  }
  it("should be able to accept print into sinks") {
    val env      = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
    val elements = (1 to 10).toList
    val stream   = env.fromCollection(elements)
    stream.addSink(DataStreamSpec.intCollector)
    stream.print()
    DataStreamSpec.values.toList should contain theSameElementsAs elements
  }

  it("should execute a simple item count using cats combine") {
    val env   = StreamExecutionEnvironment.getExecutionEnvironment
    val items = (1 to 10).map(x => s"item-${x % 10}")
    val results = env
      .fromCollection(items)
      .map(x => Counter(x, 1))
      .keyBy(_.id)
      .combine
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
    results should contain theSameElementsAs expectedValues
  }

}

object DataStreamSpec {
  val values: MutableBuffer[Int] = MutableBuffer()
  def intCollector: SinkFunction[Int] = new SinkFunction[Int] {
    override def invoke(value: Int, context: Context): Unit =
      synchronized(values.addOne(value))
  }

}
