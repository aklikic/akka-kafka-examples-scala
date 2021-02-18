package graph

import java.util.concurrent.atomic.AtomicReference

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.scaladsl.{Broadcast, Flow, FlowWithContext, GraphDSL, Merge, RestartSource, Sink, Source, Unzip, ZipWith}
import akka.stream.{ActorAttributes, FlowShape, Graph, Materializer, RestartSettings, Supervision}
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


class ErrorHandlingWithCustomOperatorAndGraphExample(bootstrapServers: String)(implicit system: ActorSystem[Nothing], materializer: Materializer, ec: ExecutionContext) {

  import SourceOps._
  val log = LoggerFactory.getLogger(classOf[ErrorHandlingWithCustomOperatorAndGraphExample])
  val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)
  val resetSettings =  RestartSettings(minBackoff = 3.seconds, maxBackoff = 30.seconds, randomFactor = 0.2)

  val kafkaConsumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system.settings.config.getConfig("akka.kafka.consumer"), new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId("groupid")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val committerSettings = CommitterSettings(system)

  def businessLogicFlow[C]: Flow[(SerializationResult, C), (Either[BusinessLogicResult,Error], C), NotUsed] =
    Flow[(SerializationResult,C)]
      .mapAsync(1)(businessLogic(_))


  def businessLogic[C](input: (SerializationResult,C)): Future[(Either[BusinessLogicResult,Error],C)] = Future(business(input))
  def business[C](input: (SerializationResult,C)): (Either[BusinessLogicResult,Error],C) = {
    if(input._1.data == 4)
      (Right(BusinessLogicError(input._1,"Data is 4")),input._2)
    else
      (Left(BusinessLogicResult(input._1)),input._2)
  }

  def serializeFlow[C]: Flow[(String, C), (Either[SerializationResult,Error], C), NotUsed] =
    Flow[(String,C)]
      .mapAsync(1)(serializeLogic(_))

  def serializeLogic[C](input: (String,C)): Future[(Either[SerializationResult,Error],C)] = Future(serialize(input))
  def serialize[C](input: (String,C)): (Either[SerializationResult,Error],C) = {
    try {
      (Left(SerializationResult(input._1.toInt)),input._2)
    }catch {
      case e: NumberFormatException => (Right(SerializationError(input._1,e.getMessage)),input._2)
    }
  }


  def successFlow[C]: Flow[(BusinessLogicResult, C), (Either[NotUsed, Error],C),NotUsed]=
    Flow[(BusinessLogicResult,C)]
      .map(result => {
        log.info("{} - SUCCESS",result._1.data.data)
        (Left(NotUsed),result._2)
      })

  def ifErrorHandlingFlow[A,C]: Flow[(Either[A,Error],C), C, NotUsed] =
    Flow[(Either[A,Error],C)]
      .map{
        case (Right(error: SerializationError),context:C) => {
          log.info("{} - ERROR (Serialization)",error.data)
          context
        }
        case (Right(error: BusinessLogicError),context:C) => {
          log.info("{} - ERROR (Business Logic)",error.data.data)
          context
        }
        case (_,context:C) => context
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


  implicit class KafkaSourceOps[A,E,C](source: Source[(Either[A,E], C), NotUsed]) {

    def viaConditional[B](
         flow: Flow[(A, C), (Either[B, E], C), NotUsed]
    ): Source[(Either[B, E], C), NotUsed] = {

      val passThroughFlow =
        Flow[(Either[A,E], C)]
          .filter(_._1.isRight)
          .map {
            case (Right(e),context:C) => (Right(e),context)
          }

      val graphFlow = Flow.fromGraph(graph(flow,passThroughFlow))

      source
        .via(graphFlow)

    }

    private def graph[A, B, E, C](
         handleFlow: Flow[(A, C), (Either[B, E], C), _],
         passThrough: Flow [(Either[A, E], C), (Either[B, E], C), _]
    ):Graph[akka.stream.FlowShape[(Either[A, E], C), (Either[B, E], C)], NotUsed] =
      GraphDSL.create(handleFlow,passThrough)((_, _) => NotUsed){ implicit builder: GraphDSL.Builder[NotUsed] => (hf,ptf) =>
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[(Either[A,E], C)](2))

        val filterFlow: Flow [(Either[A, E], C), (A, C), NotUsed] =
          Flow[(Either[A,E], C)]
            .filter(_._1.isLeft)
            .map {
              case (Left(a),context:C) => (a,context)
            }

        val merge = builder.add(Merge[(Either[B, E], C)](2))

        // format: OFF
        broadcast.out(0) ~> filterFlow ~> hf  ~> merge
        broadcast.out(1) ~> ptf ~> merge
        // format: ON
        FlowShape(broadcast.in, merge.out)
      }
  }
}

