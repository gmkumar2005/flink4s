package com.ariskk.flink4s

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.FutureConverters.*
import scala.util.Random
object FlinkExecutor {
  /** parallelism for all test cases
    */
  val parallelismValue = 5
  private val logger = LoggerFactory.getLogger(getClass)
  private var flinkCluster: MiniClusterWithClientResource = _

  def startCluster(): Unit = {
    logger.debug("Starting cluster")
    flinkCluster = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(parallelismValue)
        .setNumberTaskManagers(parallelismValue)
        .build
    )
    flinkCluster.before()
    val scalaFuture = flinkCluster.getMiniCluster.requestClusterOverview.asScala
    Await.result(scalaFuture, Duration.Inf)
    logger.debug("Cluster started")
  }

  def stopCluster(): Unit = {
    logger.debug("Stopping cluster")
    flinkCluster.after()
  }

  def newEnv(parallelism: Int = 2): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    val rocks = new EmbeddedRocksDBStateBackend()
    env.setStateBackend(rocks)
    env
  }

}
