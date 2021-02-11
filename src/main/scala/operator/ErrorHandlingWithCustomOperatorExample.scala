package operator

import java.util.concurrent.atomic.AtomicReference

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.scaladsl.{Flow, FlowWithContext, RestartSource, Sink, Source}
import akka.stream.{ActorAttributes, Materializer, RestartSettings, Supervision}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

sealed trait Error
final case class ExceptionWrapper(error: Error, context: CommittableOffset) extends RuntimeException

final case class SerializationResult(data: Int)
final case class SerializationError(data: String,message:String) extends Error

final case class BusinessLogicResult(data: SerializationResult)
final case class BusinessLogicError(data: SerializationResult, message:String) extends Error

//final case class Error[T](data: T, context: CommittableOffset, message:String) extends ProcessingErrorWithContext(context,message)


class ErrorHandlingWithCustomOperatorExample(bootstrapServers: String)(implicit system: ActorSystem[Nothing], materializer: Materializer, ec: ExecutionContext) {

  import SourceOps._
  val log = LoggerFactory.getLogger(classOf[ErrorHandlingWithCustomOperatorExample])
  val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)
  val resetSettings =  RestartSettings(minBackoff = 3.seconds, maxBackoff = 30.seconds, randomFactor = 0.2)

  val kafkaConsumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system.settings.config.getConfig("akka.kafka.consumer"), new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId("groupid")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val committerSettings = CommitterSettings(system)

  def businessLogicFlow: Flow[(SerializationResult, CommittableOffset), (Either[BusinessLogicResult,Error], CommittableOffset), NotUsed] =
    Flow[(SerializationResult,CommittableOffset)]
      .mapAsync(1)(businessLogic(_))


  def businessLogic(input: (SerializationResult,CommittableOffset)): Future[(Either[BusinessLogicResult,Error],CommittableOffset)] = Future(business(input))
  def business(input: (SerializationResult,CommittableOffset)): (Either[BusinessLogicResult,Error],CommittableOffset) = {
    if(input._1.data == 4)
      (Right(BusinessLogicError(input._1,"Data is 4")),input._2)
    else
      (Left(BusinessLogicResult(input._1)),input._2)
  }

  def serializeFlow: Flow[(String, CommittableOffset), (Either[SerializationResult,Error], CommittableOffset), NotUsed] =
    Flow[(String,CommittableOffset)]
      .mapAsync(1)(serializeLogic(_))

  def serializeLogic(input: (String,CommittableOffset)): Future[(Either[SerializationResult,Error],CommittableOffset)] = Future(serialize(input))
  def serialize(input: (String,CommittableOffset)): (Either[SerializationResult,Error],CommittableOffset) = {
    try {
      (Left(SerializationResult(input._1.toInt)),input._2)
    }catch {
      case e: NumberFormatException => (Right(SerializationError(input._1,e.getMessage)),input._2)
    }
  }


  def successFlow: Flow[(BusinessLogicResult, CommittableOffset), (Either[NotUsed, Error],CommittableOffset),NotUsed]=
    Flow[(BusinessLogicResult,CommittableOffset)]
      .map(result => {
        log.info("{} - SUCCESS",result._1.data.data)
        (Left(NotUsed),result._2)
      })

  def ifErrorHandlingFlow[A]: Flow[(Either[A,Error],CommittableOffset), CommittableOffset, NotUsed] =
    Flow[(Either[A,Error],CommittableOffset)]
      .map{
        case (Right(error: SerializationError),context:CommittableOffset) => {
          log.info("{} - ERROR (Serialization)",error.data)
          context
        }
        case (Right(error: BusinessLogicError),context:CommittableOffset) => {
          log.info("{} - ERROR (Business Logic)",error.data.data)
          context
        }
        case (_,context:CommittableOffset) => context
      }


  def start(): Future[Done] = {

    val consumerSource =
      Consumer
        .committableSource(kafkaConsumerSettings, Subscriptions.topics("topic"))
        .mapMaterializedValue(c => {
          control.set(c)
          NotUsed
        })
        .map(m => (Left(m.record.value()),m.committableOffset))
        .viaConditional(serializeFlow)
        .viaConditional(businessLogicFlow)
        .viaConditional(successFlow)
        .via(ifErrorHandlingFlow)
        .via(Committer.flow(committerSettings))

    RestartSource.withBackoff(resetSettings){ () => consumerSource }
                 .run()
  }

  def stop(): Future[Done] = {
    control.get().shutdown()
  }
}

object SourceOps{

  //Extend Source with a divertingVia operation which diverts errors to an error sink
  implicit class KafkaSourceOps[A](source: Source[(Either[A,Error], CommittableOffset), NotUsed]) {
    val decider: Supervision.Decider = {
      case _: ExceptionWrapper => Supervision.Resume
      case _                      => Supervision.Stop
    }
    def viaConditional[B](
                            flow: Flow[(A, CommittableOffset), (Either[B,Error], CommittableOffset), NotUsed]
                          ): Source[(Either[B,Error], CommittableOffset), NotUsed] = { //Source with Consumer.Control

      val conditionalFlow:  Flow[(Either[A, Error], CommittableOffset), (A, CommittableOffset), NotUsed] =
        Flow[(Either[A, Error], CommittableOffset)]
        .map{
          case (Right(error), committableOffset) => throw ExceptionWrapper(error,committableOffset)
          case (Left(message), committableOffset) => (message,committableOffset)
        }

      source
        .via(conditionalFlow)
        .via(flow)
        .recover({
          case ExceptionWrapper(error,context) => (Right(error),context)
        }).withAttributes(ActorAttributes.supervisionStrategy(decider))
    }

    private def graph2[O1, O2](
                                outlet1: Flow[(immutable.Seq[O1], Committable), (Unit, Committable), _],
                                outlet2: Flow[(immutable.Seq[O2], Committable), (Unit, Committable), _]
                              ): Graph[akka.stream.FlowShape[(MultiData2[O1, O2], Committable), (Unit, Committable)], NotUsed] =
      GraphDSL.create(outlet1, outlet2)((_, _) => NotUsed) { implicit builder: GraphDSL.Builder[NotUsed] => (o1, o2) =>
        import GraphDSL.Implicits._

        val spreadOut = builder.add(
          Flow[(MultiData2[_ <: O1, _ <: O2], Committable)]
            .map {
              case (multi, committable) =>
                ((multi.data1, committable), (multi.data2, committable))
            }
        )
        val split = builder.add(Unzip[(immutable.Seq[O1], Committable), (immutable.Seq[O2], Committable)]())
        val keepCommittable = builder.add(ZipWith[(_, Committable), (_, Committable), (Unit, Committable)]({
          case ((_, committable), (_, _)) =>
            ((), committable)

        }))

        // format: OFF
        spreadOut ~> split.in
        split.out0 ~> o1 ~> keepCommittable.in0
        split.out1 ~> o2 ~> keepCommittable.in1
        // format: ON
        FlowShape(spreadOut.in, keepCommittable.out)
      }
  }
}

