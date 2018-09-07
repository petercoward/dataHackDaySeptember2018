package com.ctm.modules

import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._


trait CloudWatch extends StrictLogging {

  val environmentDimensionName = "environment"

  val namespace: String
  val environmentDimensionValue: String

  def reportMetric(metrics: MetricDatum*)

  def createMetric(dimensions: List[Dimension], metricName: String, value: Double): MetricDatum = {
    val environment: Dimension = new Dimension()
      .withName(environmentDimensionName)
      .withValue(environmentDimensionValue)

    val allDimensions = environment :: dimensions

    new MetricDatum()
      .withMetricName(metricName)
      .withUnit(StandardUnit.Count)
      .withValue(value)
      .withDimensions(allDimensions: _*)
  }

  def logMetrics(metrics: MetricDatum*): Unit = {
    metrics.foreach { metricDatum =>
      val dimString = metricDatum.getDimensions.asScala.map(dim => s"@${dim.getName}=${dim.getValue}").mkString(" ")
      logger.info(s"$dimString @${metricDatum.getMetricName}=${metricDatum.getValue}")
    }
  }
}

object CloudWatch extends StrictLogging {
  def apply(cloudWatchNamespace: String, environment: String): CloudWatch = new CloudWatch() {

    @transient lazy val cloudWatch: AmazonCloudWatch = AmazonCloudWatchClientBuilder.defaultClient()

    override val namespace: String = cloudWatchNamespace
    override val environmentDimensionValue: String = environment

    override def reportMetric(metrics: MetricDatum*): Unit = {
      if (metrics.nonEmpty) {

        val request: PutMetricDataRequest = new PutMetricDataRequest()
          .withNamespace(namespace)
          .withMetricData(metrics: _*)

        Future(cloudWatch.putMetricData(request)).onComplete {
          case Failure(ex) =>
            logger.error(s"Failed send metrics to cloudwatch", ex)
          case Success(_) =>
            logMetrics(metrics: _*)
        }
      }
    }
  }
}
