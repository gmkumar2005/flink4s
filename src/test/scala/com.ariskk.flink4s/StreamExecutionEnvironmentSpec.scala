package com.ariskk.flink4s

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

final class StreamExecutionEnvironmentSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    FlinkExecutor.startCluster()
  }

  override def afterAll(): Unit = {
    FlinkExecutor.stopCluster()
  }

  describe("StreamExecutionEnvironment") {
    it("should have an option to enable checkpointing") {
      val env = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      env.enableCheckpointing(10.seconds, CheckpointingMode.AT_LEAST_ONCE)
      val cm = env.getCheckpointConfig.getCheckpointingMode
      cm should equal(CheckpointingMode.AT_LEAST_ONCE)
    }

    it("should have an option to set restart strategies") {
      val env = FlinkExecutor.newEnv(parallelism = FlinkExecutor.parallelismValue)
      env.setRestartStrategy(RestartStrategies.noRestart)
      env.javaEnv.getRestartStrategy should equal(RestartStrategies.noRestart)
    }
  }

}
