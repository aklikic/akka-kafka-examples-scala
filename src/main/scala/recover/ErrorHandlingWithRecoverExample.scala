package recover

import java.util.concurrent.atomic.AtomicReference

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, RestartSource, Source}
import akka.stream.{Materializer, RestartSettings}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

abstract class ProcessingErrorWithContext(context: CommittableOffset, message:String) extends RuntimeException(message)

final case class SerializationResult(data: Int)
final case class SerializationError(data: String,context: CommittableOffset, message:String) extends ProcessingErrorWithContext(context,message)

final case class BusinessLogicResult(data: SerializationResult)
final case class BusinessLogicError(data: SerializationResult, context: CommittableOffset, message:String) extends ProcessingErrorWithContext(context,message)

//final case class Error[T](data: T, context: CommittableOffset, message:String) extends ProcessingErrorWithContext(context,message)


class ErrorHandlingWithRecoverExample(bootstrapServers: String)(implicit system: ActorSystem[Nothing], materializer: Materializer, ec: ExecutionContext) {
  val log = LoggerFactory.getLogger(classOf[ErrorHandlingWithRecoverExample])
  val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)
  val resetSettings =  RestartSettings(minBackoff = 3.seconds, maxBackoff = 30.seconds, randomFactor = 0.2)

  val kafkaConsumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system.settings.config.getConfig("akka.kafka.consumer"), new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId("groupid")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val committerSettings = CommitterSettings(system)

  def businessLogicFlow: Flow[(SerializationResult, CommittableOffset), (BusinessLogicResult, CommittableOffset), NotUsed] =
    Flow[(SerializationResult,CommittableOffset)]
      .mapAsync(1)(businessLogic(_))


  def businessLogic(input: (SerializationResult,CommittableOffset)): Future[(BusinessLogicResult,CommittableOffset)] = Future(business(input))
  def business(input: (SerializationResult,CommittableOffset)): (BusinessLogicResult,CommittableOffset) = {
    if(input._1.data == 4)
      throw BusinessLogicError(input._1,input._2,"Data is 4")
    else
      (BusinessLogicResult(input._1),input._2)
  }

  def serializeFlow: Flow[(String, CommittableOffset), (SerializationResult, CommittableOffset), NotUsed] =
    Flow[(String,CommittableOffset)]
      .mapAsync(1)(serializeLogic(_))

  def serializeLogic(input: (String,CommittableOffset)): Future[(SerializationResult,CommittableOffset)] = Future(serialize(input))
  def serialize(input: (String,CommittableOffset)): (SerializationResult,CommittableOffset) = {
    try {
      (SerializationResult(input._1.toInt),input._2)
    }catch {
      case e: NumberFormatException => throw SerializationError(input._1,input._2,e.getMessage)
    }
  }


  def successFlow: Flow[(BusinessLogicResult, CommittableOffset), CommittableOffset, NotUsed] =
    Flow[(BusinessLogicResult,CommittableOffset)]
      .map(result => {
        log.info("{} - SUCCESS",result._1.data.data)
        result._2
      })

  def errorHandlingFlow: Flow[ProcessingErrorWithContext, CommittableOffset, NotUsed] =
    Flow[ProcessingErrorWithContext]
      .map{
        case e: SerializationError => {
          log.info("{} - ERROR (Serialization)",e.message)
          e.context
        }
        case e: BusinessLogicError => {
          log.info("{} - ERROR (Business Logic)",e.data.data)
          e.context
        }
      }

  def errorHandlingSource(error: ProcessingErrorWithContext): Source[CommittableOffset,NotUsed] =
    Source.single(error)
      .via(errorHandlingFlow)


  def start(): Future[Done] = {

    val consumerSource =
      Consumer
        .committableSource(kafkaConsumerSettings, Subscriptions.topics("topic"))
        .mapMaterializedValue(c => {
          control.set(c)
          NotUsed
        })
        .map(m => (m.record.value(),m.committableOffset))
        .via(serializeFlow)
        .via(businessLogicFlow)
        .via(successFlow)

        .recoverWithRetries(attempts = 1,{
          case e: ProcessingErrorWithContext => errorHandlingSource(e)
        })
        .via(Committer.flow(committerSettings))

    RestartSource.withBackoff(resetSettings){ () => consumerSource }
                 .run()
  }

  def stop(): Future[Done] = {
    control.get().shutdown()
  }
}

