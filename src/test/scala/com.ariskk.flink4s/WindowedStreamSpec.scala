package com.ariskk.flink4s

import com.ariskk.flink4s.TypeInfo.{intTypeInfo, stringTypeInfo}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable.Buffer as MutableBuffer

final class WindowedStreamSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    FlinkExecutor.startCluster()
  }

  override def afterAll(): Unit = {
    FlinkExecutor.stopCluster()
  }

  describe("WindowedStream") {
    it("should apply reducers to count windows") {
      val env    = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val stream = env.fromCollection((1 to 1000).toList)
      val results = stream
        .keyBy(_ % 2)
        .countWindow(250)
        .reduce(_ + _)
        .runAndCollect

      val firstOdds   = (1 to 499 by 2).sum
      val secondOdds  = (501 to 999 by 2).sum
      val firstEvens  = (2 to 500 by 2).sum
      val secondEvens = (502 to 1000 by 2).sum

      results should contain theSameElementsAs Seq(firstOdds, secondOdds, firstEvens, secondEvens)

    }

    it("should apply reducers to count windows with slide") {
      val env    = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      val stream = env.fromCollection((1 to 200).toList.map(_ => 1))
      val results = stream
        .keyBy(identity)
        .countWindow(100, 50)
        .reduce(_ + _)
        .runAndCollect

      results.size should equal(4)
      results should contain theSameElementsAs List(50, 100, 100, 100)
    }
  }

}
