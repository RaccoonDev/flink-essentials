package com.devraccoon.shopping

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.UUID
import scala.annotation.tailrec
import com.devraccoon.shopping.ShoppingCartEventsGenerator._

// Let's keep very very simple shopping cart events
sealed trait ShoppingCartEvent {
  def userId: String
  def time: java.time.Instant
}

// We'll have only add to shopping cart events for now
case class AddToShoppingCartEvent(
    userId: String,
    sku: String,
    quantity: Int,
    time: java.time.Instant
) extends ShoppingCartEvent

// The below generator is inspired by generator in flink-training repository:
// https://github.com/apache/flink-training/
// Look for TaxiRideGenerator class
// The data will be generated in batches with
// sleep between batches (BATCH_SIZE * SLEEP_PER_EVENT)

// Here we have an ability to simulate data that is read from some persistent store:
// if baseInstant parameter is specified all event times would start from that time
// and would gradually grow in time.
class ShoppingCartEventsGenerator(
    sleepMillisPerEvent: Int,
    batchSize: Int,
    baseInstant: java.time.Instant = java.time.Instant.now()
) extends SourceFunction[ShoppingCartEvent] {
  import com.devraccoon.shopping.ShoppingCartEventsGenerator._

  @volatile private var running = true

  @tailrec
  private def run(
      startId: Long,
      ctx: SourceFunction.SourceContext[ShoppingCartEvent]
  ): Unit =
    if (running) {
      generateRandomEvents(startId).foreach(ctx.collect)
      Thread.sleep(batchSize * sleepMillisPerEvent)
      run(startId + batchSize, ctx)
    }

  private def generateRandomEvents(id: Long): Seq[AddToShoppingCartEvent] = {
    val events = (1 to batchSize)
      .map(_ =>
        AddToShoppingCartEvent(
          getRandomUser,
          UUID.randomUUID().toString,
          getRandomQuantity,
          baseInstant.plusSeconds(id)
        )
      )

    println(s"[${System.currentTimeMillis()}] Releasing events: $events")
    events
  }

  override def run(
      ctx: SourceFunction.SourceContext[ShoppingCartEvent]
  ): Unit = run(0, ctx)

  override def cancel(): Unit = { running = false }
}

class SingleShoppingCartEventsGenerator(
    sleepMillisBetweenEvents: Int,
    baseInstant: java.time.Instant = java.time.Instant.now(),
    extraDelayInMillisOnEveryTenEvents: Option[Long] = None,
    sourceId: Option[String] = None
) extends EventGenerator[ShoppingCartEvent](
      sleepMillisBetweenEvents,
      id =>
        AddToShoppingCartEvent(
          getRandomUser,
          sourceId
            .map(sId => s"${sId}_${UUID.randomUUID()}")
            .getOrElse(UUID.randomUUID().toString),
          getRandomQuantity,
          baseInstant.plusSeconds(id)
        ),
      extraDelayInMillisOnEveryTenEvents
    )

class EventGenerator[T](
    sleepMillisBetweenEvents: Int,
    generator: Long => T,
    extraDelayInMillisOnEveryTenEvents: Option[Long] = None
) extends SourceFunction[T] {
  @volatile private var running = true

  @tailrec
  private def run(
      id: Long,
      ctx: SourceFunction.SourceContext[T]
  ): Unit =
    if (running) {
      ctx.collect(
        generator(id)
      )
      Thread.sleep(sleepMillisBetweenEvents)
      if (id % 10 == 0) extraDelayInMillisOnEveryTenEvents.foreach(Thread.sleep)
      run(id + 1, ctx)
    }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit =
    run(1, ctx)

  override def cancel(): Unit = {
    running = false
  }
}

object ShoppingCartEventsGenerator {
  val users: Vector[String] = Vector("Bob", "Alice", "Sam", "Tom", "Rob")

  def getRandomUser: String = users(scala.util.Random.nextInt(users.length))

  def getRandomQuantity: Int = scala.util.Random.nextInt(10)
}

sealed trait CatalogEvent {
  def userId: String
  def time: java.time.Instant
}

case class ProductDetailsViewed(
    userId: String,
    time: java.time.Instant,
    productId: String
) extends CatalogEvent

class CatalogEventsGenerator(
    sleepMillisBetweenEvents: Int,
    baseInstant: java.time.Instant = java.time.Instant.now(),
    extraDelayInMillisOnEveryTenEvents: Option[Long] = None
) extends EventGenerator[CatalogEvent](
      sleepMillisBetweenEvents,
      id =>
        ProductDetailsViewed(
          getRandomUser,
          baseInstant.plusSeconds(id),
          UUID.randomUUID().toString
        ),
      extraDelayInMillisOnEveryTenEvents
    )
