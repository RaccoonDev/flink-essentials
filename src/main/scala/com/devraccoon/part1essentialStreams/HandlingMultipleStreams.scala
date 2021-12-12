package com.devraccoon.part1essentialStreams

import com.devraccoon.part1essentialStreams.Windows.CountInAllWindow
import com.devraccoon.shopping.{
  CatalogEvent,
  CatalogEventsGenerator,
  ShoppingCartEvent,
  SingleShoppingCartEventsGenerator
}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object HandlingMultipleStreams {
  def main(args: Array[String]): Unit = {

    /*
      Let's explore what we can do to handle more than one stream.
     */

    // Union
//    union

    // Window Join
//    windowJoins

    // Interval Joins

    // very similar to Window Joins but operates on keyed streams. So, if we key both streams of catalog and shopping
    // cart events by user id, we can user interval join with telling flink lower and upper time bounds to correlate
    // the events.

    // Note that to properly work with time interval Flink must know how to get event time out involved event types

    val intervalJoinEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEvent: KeyedStream[ShoppingCartEvent, String] =
      intervalJoinEnv
        .addSource(new SingleShoppingCartEventsGenerator(300))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
            .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(
              new SerializableTimestampAssigner[ShoppingCartEvent] {
                override def extractTimestamp(
                    element: ShoppingCartEvent,
                    recordTimestamp: Long
                ): Long =
                  element.time.toEpochMilli
              }
            )
        )
        .keyBy(_.userId)

    val catalogEvents =
      intervalJoinEnv
        .addSource(new CatalogEventsGenerator(500))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
            .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(
              new SerializableTimestampAssigner[CatalogEvent] {
                override def extractTimestamp(
                    element: CatalogEvent,
                    recordTimestamp: Long
                ): Long =
                  element.time.toEpochMilli
              }
            )
        )
        .keyBy(_.userId)

    val intervalJoinedStream = shoppingCartEvent
      .intervalJoin(catalogEvents)
      .between(Time.seconds(-2), Time.seconds(2))
      .lowerBoundExclusive() // both bounds are inclusive by default. These two methods makes them exclusive
      .upperBoundExclusive()
      .process(
        (
            left: ShoppingCartEvent,
            right: CatalogEvent,
            _: ProcessJoinFunction[
              ShoppingCartEvent,
              CatalogEvent,
              (ShoppingCartEvent, CatalogEvent)
            ]#Context,
            out: Collector[(ShoppingCartEvent, CatalogEvent)]
        ) => out.collect((left, right))
      )

    intervalJoinedStream.print()

    /*
      Sample output shows joined events
      9> (AddToShoppingCartEvent(Alice,840c0c74-e0d7-47c7-9021-2577aabb50fa,6,2021-12-12T23:17:28.960702Z),ProductDetailsViewed(Alice,2021-12-12T23:17:27.303883Z,b94ee291-4ee9-42cf-80cb-29e7b4ec39eb))
      12> (AddToShoppingCartEvent(Bob,795ae0da-338f-418e-8422-72feaca67abe,4,2021-12-12T23:17:27.960702Z),ProductDetailsViewed(Bob,2021-12-12T23:17:28.303883Z,96c68cd6-d69c-411c-97f5-478e11562b08))
      1> (AddToShoppingCartEvent(Sam,699c2c8b-6d43-4ec3-8165-adfaf9ccd18a,3,2021-12-12T23:17:30.960702Z),ProductDetailsViewed(Sam,2021-12-12T23:17:29.303883Z,981a6ff9-699b-4cf8-b47a-2549c34adebc))
      1> (AddToShoppingCartEvent(Sam,699c2c8b-6d43-4ec3-8165-adfaf9ccd18a,3,2021-12-12T23:17:30.960702Z),ProductDetailsViewed(Sam,2021-12-12T23:17:31.303883Z,b6f29a7c-9bf9-4a94-836e-71c30ed2bdde))
      11> (AddToShoppingCartEvent(Rob,d78583e9-a7ef-47e3-b489-54989a5c9763,3,2021-12-12T23:17:32.960702Z),ProductDetailsViewed(Rob,2021-12-12T23:17:34.303883Z,4af7ecc0-5980-4040-b1c1-d90f18177a8a))
      11> (AddToShoppingCartEvent(Tom,a9daa4b8-4256-4d65-b94f-abb61358aa35,1,2021-12-12T23:17:37.960702Z),ProductDetailsViewed(Tom,2021-12-12T23:17:37.303883Z,2821458f-9cf6-44a0-977a-9ee8bd5af81e))
      11> (AddToShoppingCartEvent(Tom,a9daa4b8-4256-4d65-b94f-abb61358aa35,1,2021-12-12T23:17:37.960702Z),ProductDetailsViewed(Tom,2021-12-12T23:17:38.303883Z,6a04ba5b-c16a-429f-84df-ebb5231c99b8))
      9> (AddToShoppingCartEvent(Alice,7d9f72a5-1bd9-43d7-8d31-dace05a2c668,8,2021-12-12T23:17:40.960702Z),ProductDetailsViewed(Alice,2021-12-12T23:17:39.303883Z,6b253157-47ab-4049-9794-b6caddff2acd))

     */

    intervalJoinEnv.execute()

  }

  private def union: JobExecutionResult = {
    // This one is easy. Take two or more streams and send events from the streams to output.

    val unionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    // Let's say we have to streams from different sources, but about the same data
    val shoppingCartEventStreamFromKafka: DataStream[ShoppingCartEvent] =
      unionEnv.addSource(
        new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka"))
      )
    val shoppingCartEventStreamFromFileMonitor: DataStream[ShoppingCartEvent] =
      unionEnv.addSource(
        new SingleShoppingCartEventsGenerator(1000, sourceId = Option("file"))
      )

    val combinedSingleStreamOfEventsFromBothSources
        : DataStream[ShoppingCartEvent] =
      shoppingCartEventStreamFromKafka.union(
        shoppingCartEventStreamFromFileMonitor
      )

    combinedSingleStreamOfEventsFromBothSources.print()

    /*
    Sample output of the above shows that events from both streams are combined:
    6> AddToShoppingCartEvent(Bob,file_545ac79b-7196-4690-a93e-f47ab5d6eca8,5,2021-12-12T22:37:56.369417Z)
    3> AddToShoppingCartEvent(Rob,kafka_07176b92-91ff-4460-b950-5b60efb356c7,6,2021-12-12T22:37:56.086243Z)
    4> AddToShoppingCartEvent(Sam,kafka_6ca2830d-c543-458e-b718-c799a352a0be,6,2021-12-12T22:37:57.086243Z)
    5> AddToShoppingCartEvent(Rob,kafka_dfa2f2df-f15d-4bbd-a389-28f36e4cda64,3,2021-12-12T22:37:58.086243Z)
    6> AddToShoppingCartEvent(Tom,kafka_a07c4419-5527-4594-a1fa-e1231ae5879c,9,2021-12-12T22:37:59.086243Z)
    7> AddToShoppingCartEvent(Alice,file_c5379d29-3bf6-409c-89c2-df668facbbb0,8,2021-12-12T22:37:57.369417Z)
    7> AddToShoppingCartEvent(Rob,kafka_f50835cd-2174-40ba-a547-43ee6ceaa362,8,2021-12-12T22:38:00.086243Z)
    8> AddToShoppingCartEvent(Tom,kafka_f5bad5e2-709d-4eba-8649-93f77a7867b4,0,2021-12-12T22:38:01.086243Z)
    9> AddToShoppingCartEvent(Alice,kafka_55a76df2-0938-457e-b581-b77e908e8771,0,2021-12-12T22:38:02.086243Z)
     */

    unionEnv.execute()
  }

  private def windowJoins: JobExecutionResult = {
    // Let's get an interesting representative example of this join. Let's say we have two website
    // and users work with both websites simultaneously and we need to know how many events per given
    // user happens within a give tumbling processing time window of let's say 10 seconds. That might
    // show us how much of interaction between the websites is necessary for users to make the job done.

    // More concrete example can be that user interacts with shopping cart and products details page.
    // We need to know how many events happened in the same time window as events in shopping cart.

    val windowJoinEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEventsString = windowJoinEnv.addSource(
      new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka"))
    )
    val catalogEventsString =
      windowJoinEnv.addSource(new CatalogEventsGenerator(1000))

    // This one connects two streams of different events
    val join = shoppingCartEventsString
      .join(catalogEventsString)
      // here we specify how to tell that two events correlate one to another
      .where(shoppingCartEvent => shoppingCartEvent.userId)
      .equalTo(catalogEvent => catalogEvent.userId)
      // now we specify in what time interval should flink look for this correlation
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      // When correlation found the following function tells what to do with two
      // correlated events
      .apply(
        new JoinFunction[
          ShoppingCartEvent,
          CatalogEvent,
          (ShoppingCartEvent, CatalogEvent)
        ] {
          override def join(
              first: ShoppingCartEvent,
              second: CatalogEvent
          ): (ShoppingCartEvent, CatalogEvent) = (first, second)
        }
      )

    join.print()

    windowJoinEnv.execute()
  }
}
