import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

object Streams {


  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes
    val deserializer = (aAsBytes: Array[Byte]) => {
      val aAsString = new String(aAsBytes)
      println(s"\ndata1 ====> $aAsString\n")
      val aOrError = decode[A](aAsString)
      aOrError match {
        case Right(a) =>{
          println(s"transformation good for case ==> ${a.getClass}")
          Option(a)
        }
        case Left(error) =>
          println(s"There was an error converting the message\n{ $aOrError},\n{ $error},\nthis is the data ===> ${aAsString}")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  final val OrdersByUserTopic = "orders-by-user"
  final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
  final val DiscountsTopic = "discounts"
  final val OrdersTopic = "orders"
  final val PaymentsTopic = "payments"
  final val PayedOrdersTopic = "paid-orders"

  type UserId  = String
  type Profile = String
  type Product = String
  type OrderId = String



  case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

  case class Discount(profile: Profile, amount: Double)

  case class Payment(orderId: OrderId, status: String)


  def main(args:Array[String]): Unit = {
    val builder = new StreamsBuilder
    val streamOrder: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

    val userProfilesTable = builder.table[UserId,Profile](DiscountProfilesByUserTopic)
    val dicountGtable = builder.globalTable[Profile,Discount](DiscountsTopic)

    val orderwithProfile = streamOrder.join[Profile,(Order,Profile)](userProfilesTable){ (order, profile) => (order,profile)}

    val discountedOrder = orderwithProfile.join(dicountGtable)(
      { case (userId: UserId,(order: Order,profile: Profile) ) => profile},
      { case ((order: Order,profile: Profile),discount) => order.copy( amount = order.amount - discount.amount ) }

    )

   val ordersStream = discountedOrder.selectKey( ( userid,order ) => order.orderId)

   val paymentsStream = builder.stream[OrderId,Payment](PaymentsTopic)
   val  joinPaumentsFuntion = (order:Order,payment:Payment) =>
     if( payment.status == "PAID" ) Option(order)
     else Option.empty[Order]

   val joinWindows = JoinWindows.of(Duration.of(5,ChronoUnit.MINUTES))
   val orderPaid: KStream[OrderId, Order] =  ordersStream.join[ Payment, Option[Order] ](paymentsStream)(joinPaumentsFuntion,joinWindows).flatMapValues(maybeOrder => maybeOrder.toIterable )
    orderPaid.to( PayedOrdersTopic )



    val props = new Properties()

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)


    val topology  = builder.build()
    println(topology.describe())
    val aplication = new KafkaStreams(topology,props)
    aplication.start()
  }
  
}
