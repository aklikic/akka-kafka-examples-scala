package divertto

import java.util.concurrent.atomic.AtomicReference

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, RestartSource, Sink, Source}
import akka.stream.{Materializer, RestartSettings}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

sealed trait ProcessingError

final case class SerializationResult(data: Int)
final case class SerializationError(error: Throwable, message: String) extends ProcessingError

final case class BusinessLogicResult(data: SerializationResult)
final case class BusinessLogicError(error: String, data: SerializationResult) extends ProcessingError


class DivertToExample(bootstrapServers: String)(implicit system: ActorSystem[Nothing], materializer: Materializer, ec: ExecutionContext) {
  import SourceOpsNew._
  val log = LoggerFactory.getLogger(classOf[DivertToExample])
  val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)
  val resetSettings =  RestartSettings(minBackoff = 3.seconds, maxBackoff = 30.seconds, randomFactor = 0.2)

  val kafkaConsumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system.settings.config.getConfig("akka.kafka.consumer"), new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId("groupid")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val committerSettings = CommitterSettings(system)

  def businessLogicFlow[CommittableOffset]: FlowWithContext[SerializationResult, CommittableOffset, Either[BusinessLogicError,BusinessLogicResult], CommittableOffset, NotUsed] = {
    FlowWithContext[SerializationResult,CommittableOffset]
      .mapAsync(1)(businessLogic(_))

  }

  def businessLogic(input: SerializationResult): Future[Either[BusinessLogicError,BusinessLogicResult]] = Future(business(input))
  def business(input: SerializationResult): Either[BusinessLogicError,BusinessLogicResult] = {
    if(input.data == 4)
      Left(BusinessLogicError("Data is 4",input))
    else
      Right(BusinessLogicResult(input))
  }

  def serializeFlow[CommittableOffset]: FlowWithContext[String, CommittableOffset, Either[SerializationError,SerializationResult], CommittableOffset, NotUsed] = {
    FlowWithContext[String,CommittableOffset]
      .mapAsync(1)(serializeLogic(_))

  }
  def serializeLogic(input: String): Future[Either[SerializationError,SerializationResult]] = Future(serialize(input))
  def serialize(input: String): Either[SerializationError,SerializationResult] = {
    try {
      Right(SerializationResult(input.toInt))
    }catch {
      case e: NumberFormatException => Left(SerializationError(e,input))
    }
  }

  def successFlow: Flow[(BusinessLogicResult, CommittableOffset), Done, NotUsed] = {
    FlowWithContext[BusinessLogicResult,CommittableOffset]
      .map(result => {
        log.info("{} - SUCCESS",result.data.data)
        result
      }).asFlow
      .map(_._2) //get offset
      .via(Committer.flow(committerSettings))
  }

  def errorSink: Sink[(ProcessingError, CommittableOffset), Future[Done]] = {
    FlowWithContext[ProcessingError,CommittableOffset]
      .map{
        case e: SerializationError => log.info("{} - ERROR (Serialization)",e.message)
        case e: BusinessLogicError => log.info("{} - ERROR (Business Logic)",e.data.data)
      }
      .asFlow
      .map(_._2) //get offset
      .toMat(Committer.sink(committerSettings))(Keep.right)
      //.toMat(Sink.ignore)(Keep.right)
  }

  def start(): Future[Done] = {

    val consumerSource =
      Consumer
        .committableSource(kafkaConsumerSettings, Subscriptions.topics("topic"))
        .mapMaterializedValue(c => {
          control.set(c)
          NotUsed
        })
        .map(m => (m.record.value(),m.committableOffset))
        .divertingVia(serializeFlow, errorSink)
        .divertingVia(businessLogicFlow, errorSink)
        .via(successFlow)

    RestartSource.withBackoff(resetSettings){ () => consumerSource }
                 .run()
  }
}

object SourceOpsNew{

  //Extend Source with a divertingVia operation which diverts errors to an error sink
  implicit class KafkaSourceOps[A](source: Source[(A, CommittableOffset), NotUsed]) {

    def divertingVia[B, C](
                            flow: FlowWithContext[A, CommittableOffset, Either[B, C], CommittableOffset, NotUsed],
                            errorSink: Sink[(B, CommittableOffset), Future[Done]]
                          ): Source[(C, CommittableOffset), NotUsed] = { //Source with Consumer.Control

      val sink = Flow[(Either[B, C], CommittableOffset)]
        .collect {
          case (Left(error), committableOffset) => (error, committableOffset)
        }.to(errorSink)
      source
        .via(flow)
        .divertTo(sink, _._1.isLeft)
        .collect(pair => {
          pair._1 match {
            case Right(e) => (e, pair._2)
          }
        })
    }
  }
}