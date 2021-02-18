package test

import java.net.URI

import scala.language.postfixOps
import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.Producer
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Source}
import divertto.DivertToExample
import graph.ErrorHandlingWithCustomOperatorAndGraphExample
import operator.ErrorHandlingWithCustomOperatorExample
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import recover.ErrorHandlingWithRecoverExample

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent._
import scala.concurrent.duration._

object Main extends App{

  val kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("5.4.1"))
  kafkaContainer.start()
  val bootstrapUri = URI.create(kafkaContainer.getBootstrapServers)
  try {
    val me = new Main
    me.run(s"${bootstrapUri.getHost}:${bootstrapUri.getPort}")
  } finally kafkaContainer.stop()
}

class Main {
  implicit val system = ActorSystem(Behaviors.empty, "DivertToExample")
  implicit val materializer: Materializer = Materializer.matFromSystem(system)
  implicit val ec: ExecutionContext = system.executionContext
  val log = LoggerFactory.getLogger(classOf[Main])

  private def run(kafkaServer: String): Unit = {
    log.info(s"kafkaServer:$kafkaServer")
    //run DivertTo example
    //new DivertToExample(kafkaServer).start();
    //run recover example
    //new ErrorHandlingWithRecoverExample(kafkaServer).start()
    //new ErrorHandlingWithCustomOperatorExample(kafkaServer).start()
    new ErrorHandlingWithCustomOperatorAndGraphExample(kafkaServer).start()
    new Generator(kafkaServer).start()
    Thread.sleep(60000)

  }
}

class Generator(bootstrapServers: String)(implicit system: ActorSystem[Nothing], materializer: Materializer){

  val interval :FiniteDuration = FiniteDuration(1,SECONDS)
  val kafkaProducerSettings =
    ProducerSettings(system.settings.config.getConfig("akka.kafka.producer"), new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  def start(): Future[Done] = {

    Source(List("1","2","3","4","Wrong5","6","7","8","9"))
      .throttle(1,interval)
      .map(msg => new ProducerRecord("topic","1",msg))
      .toMat(Producer.plainSink(kafkaProducerSettings))(Keep.both)
      .run()
      ._2
  }
}
