package com.devraccoon.part1essentialStreams

import com.devraccoon.part1essentialStreams.Windows.CountInAllWindow
import com.devraccoon.shopping.{ShoppingCartEvent, ShoppingCartEventsGenerator}
import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  Watermark,
  WatermarkGenerator,
  WatermarkGeneratorSupplier,
  WatermarkOutput,
  WatermarkStrategy
}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{
  TumblingEventTimeWindows,
  TumblingProcessingTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.time.Instant
import scala.concurrent.duration._

object TimeBasedTransformations {

  // Here we are going to go deeper in notion of time (events vs processing),
  // watermarks, watermarks generation and handling late responses

  /*
    Let's start with seeing what notions of time are out there (https://www.youtube.com/watch?v=QVDJFZVHZ3c):
    - event time: time assigned on a device that produces the event (sensor, smartphone, browser, etc.)
    - storage time: time when an events was written or read by any server side component (kafka, API, cache, etc.)
    - ingestion time: time when flink first time saw the event
    - processing time: time when flink is doing computations with an events
   */

  val env: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  // Let's grab our starcraft events from previous clip
  import com.devraccoon.starcraft.events._

  implicit val startTime: Instant =
    Instant.parse("2020-02-02T00:00:00.000Z") // 1643760000000

  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // 1643760002000
    bob.online(2.second), // 1643760002000
    sam.register(3.seconds), // 1643760004000
    sam.online(4.second), // 1643760004000
    rob.register(4.seconds), // 1643760004000
    alice.register(4.seconds), // 1643760004000
    mary.register(6.seconds), // 1643760006000
    mary.online(6.seconds), // 1643760006000
    carl.register(8.seconds), // 1643760008000
    rob.online(10.seconds), // 1643760010000
    alice.online(10.seconds), // 1643760010000
    carl.online(11.seconds) // 1643760011000
  )

  // For that example we can see that all those events are from second of February, 2020.
  // Let's see what would happen if we would group then into a window as is
  // (we did that in previous clip, but let's take a closer look)

  val threeSecondsWindowAll: DataStream[(TimeWindow, Long)] =
    env
      .fromCollection(events)
      // Note that here we create not "TumblingEventTimeWindows", but "TumblingProcessingTimeWindows"
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .process(new CountInAllWindow(_ => true))

  // Running that we see that there is nothing in output stream. We have asked Flink to group events into 3 seconds
  // windows by the processing time, but, the application never processed anything for 3 seconds. So, processing
  // exists without emitting any results.

  // Let's try some more realistic and relevant scenario for processing time.

  /* EXPERIMENTAL USE-CASE - START */

  // Assume that we have an online store where user opens many tabs of the same shop and sends
  // events to a single shopping cart.

  // We are receiving all those events and the result shopping cart does not really care what is the order
  // of the events are coming in. Assuming here that person adds items to the card and removes items from the card.

  // Let's create a handy generator for such events. For that let's go to 'com.devraccoon.shopping.CartEventsGenerator'
  // ....
  // Now we can use the newly created generator

  val processingTimeShoppingCartEventsEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents: DataStream[(TimeWindow, Long)] =
    processingTimeShoppingCartEventsEnv
      .addSource(
        // this setup is going to give us batches of 5 events two times per second
        // Note that we are going to simulate that events are starting from Jan 15, 2021.
        new ShoppingCartEventsGenerator(
          sleepMillisPerEvent = 100,
          batchSize = 5,
          baseInstant = Instant.parse("2021-01-15T00:00:00.000Z")
        )
      )
      .windowAll(
        // so, with the above setup we should expect about 30 events per 3 seconds long windows
        TumblingProcessingTimeWindows.of(Time.seconds(3))
      )
      .process(new CountInAllWindow(_ => true))

  /*
    Sample output:
    6> (TimeWindow{start=1635022839000, end=1635022842000},5) // 2021-10-23T21:00:39Z
    7> (TimeWindow{start=1635022842000, end=1635022845000},30)
    8> (TimeWindow{start=1635022845000, end=1635022848000},30)
    1> (TimeWindow{start=1635022848000, end=1635022851000},30)
    2> (TimeWindow{start=1635022851000, end=1635022854000},30)
   */

  // The above example completely ignores the time when event actually occurs.
  // If that was the case that the data was read from a database of kafka broker,
  // reprocessing of the data would generate completely different result - all windows would be windows based on current
  // processing time.

  /*
    If we run the same program once again we would get completely new windows:
    8> (TimeWindow{start=1635023124000, end=1635023127000},5) // 2021-10-23T21:05:24Z
    1> (TimeWindow{start=1635023127000, end=1635023130000},30)

    OR

    2> (TimeWindow{start=1635024138000, end=1635024141000},30) // 2021-10-23T21:22:18Z
    3> (TimeWindow{start=1635024141000, end=1635024144000},30)
   */

  /* EXPERIMENTAL USE-CASE - END */

  /*
    Other areas where this type of event processing might be useful:
    - Data synchronization - when data must be moved from one storage to another one and we don't really care when the original request came from

    In many cases pipeline can be made much more reliable with event times and more fail tolerant.
   */

  // Now let's see what we can do if we specify event time as time to use
  val eventTimeShoppingCartEventsEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  // We are using the generator with the same settings
  val eventTimeShoppingCartCountInWindows: DataStream[(TimeWindow, Long)] =
    eventTimeShoppingCartEventsEnv
      .addSource(
        new ShoppingCartEventsGenerator(
          sleepMillisPerEvent = 100,
          batchSize = 5,
          baseInstant = Instant.parse("2021-01-15T00:00:00.000Z")
        )
      )
      // The below tells flink what watermark strategy to use and what would
      // define event time.
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
        // Bounded out of orderness means here that we promise flink that
        // at any point in time no events will come with delay more than 500 milliseconds
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          // The setting below tells flink where to find event time property
          .withTimestampAssigner(
            new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(
                  element: ShoppingCartEvent, // Element to assign timestamp
                  recordTimestamp: Long // Previously assigned timestamp (there might be several timestamp assigners)
              ): Long =
                element.time.toEpochMilli // returning value of timestamp to use in further processing
            }
          )
      )
      // And most importantly, let's change that windowing function
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
      .process(new CountInAllWindow(_ => true))

  /*
    Let's run this.

    3> (TimeWindow{start=1610668800000, end=1610668803000},5) // 2021-01-15T00:00:00Z
    4> (TimeWindow{start=1610668803000, end=1610668806000},5)
    5> (TimeWindow{start=1610668809000, end=1610668812000},5)
    6> (TimeWindow{start=1610668815000, end=1610668818000},5)
    7> (TimeWindow{start=1610668818000, end=1610668821000},5)
    8> (TimeWindow{start=1610668824000, end=1610668827000},5)
    1> (TimeWindow{start=1610668830000, end=1610668833000},5)
    2> (TimeWindow{start=1610668833000, end=1610668836000},5)
    3> (TimeWindow{start=1610668839000, end=1610668842000},5)

    And once again
    3> (TimeWindow{start=1610668800000, end=1610668803000},5) // 2021-01-15T00:00:00Z
    4> (TimeWindow{start=1610668803000, end=1610668806000},5)
    5> (TimeWindow{start=1610668809000, end=1610668812000},5)
    6> (TimeWindow{start=1610668815000, end=1610668818000},5)
    7> (TimeWindow{start=1610668818000, end=1610668821000},5)
    8> (TimeWindow{start=1610668824000, end=1610668827000},5)
    1> (TimeWindow{start=1610668830000, end=1610668833000},5)
    2> (TimeWindow{start=1610668833000, end=1610668836000},5)
    3> (TimeWindow{start=1610668839000, end=1610668842000},5)

    Ha! Results are identical after both executions. And window timestamps indicate that the processing
    is from 15th of January, 2021

    As well you can see that output is much faster than for "processing time" mode. That's why there is no real
    need to really wait for 3 seconds to construct a window it is enough to get an event with timestamp older than
    start of the window + window size + allowed out of orderness.

    TODO: Verify understanding of allowed orderness with tests!!!

    Let's elaborate on that:
    - Give the same window configuration: allowed out of orderness 500ms, window size of 3 seconds.
    - Let's say we have first event with event time 00:00:01.
      - Window #1 is created and assigned puts this event there.
      - At this point Flink would know that no events with event time older than 00:00:00.500 are coming.
    - Next event comes with event time 00:00:02.
      - Window assigned puts this event to Window #1.
      - And marks that no events with event time older than 00:00:01.500 are coming
    - Next event comes with event time 00:00:03
      - Assigned puts that into Window #1
      - Marks that no events with event time older than 00:00:02.500 are coming
        ^^^^ Here I am already tiered of calling this things with so verbose description and I have an urge to give it a name
    - Next event comes with event time 00:00:04
      - Assigned puts that into Window #2 since it's event time is out put Window #1
      - Marks that no events with event time older than 00:00:03.500 are coming! HA! That means that we are not expecting
        any newcomers for Window #1. That means that we can safely release the window and do any further calculations and
        processing with the data.
        ^^^^ - and that mark in time that tells Flink that no more events older than this mark are coming is called Watermark.

    That's what essentially watermarking is. A way to tell the system, when it is safe to release data for further processing.
   */

  // Let's see how watermark generator can be implemented
  // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/event-time/generating_watermarks/

  /**
    * This generator generates watermarks assuming that elements arrive out of order,
    * but only to a certain degree. The latest elements for a certain timestamp t will arrive
    * at most n milliseconds after the earliest elements for timestamp t.
    */
  class BoundedOutOfOrdernessGenerator
      extends WatermarkGenerator[ShoppingCartEvent] {

    val maxOutOfOrderness = 500L // 0.5 seconds

    var currentMaxTimestamp: Long = _

    override def onEvent(
        element: ShoppingCartEvent,
        eventTimestamp: Long,
        output: WatermarkOutput
    ): Unit = {
      currentMaxTimestamp = Math.max(eventTimestamp, currentMaxTimestamp)
    }

    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
      // emit the watermark as current highest timestamp minus the out-of-orderness bound
      output.emitWatermark(
        new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1)
      )
    }
  }

  // Let's see that in action
  val customWatermarkGeneratorEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  val countWithCustomWatermarkGenerator: DataStream[(TimeWindow, Long)] =
    customWatermarkGeneratorEnv
      .addSource(
        new ShoppingCartEventsGenerator(
          sleepMillisPerEvent = 100,
          batchSize = 5,
          baseInstant = Instant.parse("2021-01-15T00:00:00.000Z")
        )
      )
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator((_: WatermarkGeneratorSupplier.Context) =>
            new BoundedOutOfOrdernessGenerator
          )
          .withTimestampAssigner(
            new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(
                  element: ShoppingCartEvent,
                  recordTimestamp: Long
              ): Long = element.time.toEpochMilli
            }
          )
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
      .process(new CountInAllWindow(_ => true))

  // Let's run this.

  /*
    And result is identical of the one that we had before!

    5> (TimeWindow{start=1610668800000, end=1610668803000},5)
    6> (TimeWindow{start=1610668803000, end=1610668806000},5)
    7> (TimeWindow{start=1610668809000, end=1610668812000},5)
    8> (TimeWindow{start=1610668815000, end=1610668818000},5)
    1> (TimeWindow{start=1610668818000, end=1610668821000},5)
    2> (TimeWindow{start=1610668824000, end=1610668827000},5)
    3> (TimeWindow{start=1610668830000, end=1610668833000},5)
    4> (TimeWindow{start=1610668833000, end=1610668836000},5)
    5> (TimeWindow{start=1610668839000, end=1610668842000},5)
   */

  // What would happen if we won't produce a watermark?
  class DoNothingWatermarkGenerator
      extends WatermarkGenerator[ShoppingCartEvent] {

    override def onEvent(
        element: ShoppingCartEvent,
        eventTimestamp: Long,
        output: WatermarkOutput
    ): Unit = ()

    override def onPeriodicEmit(output: WatermarkOutput): Unit = ()
  }

  val noWatermarkEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment
  val noWatermarkCount: DataStream[(TimeWindow, Long)] = noWatermarkEnv
    .addSource(
      new ShoppingCartEventsGenerator(
        sleepMillisPerEvent = 100,
        batchSize = 5,
        baseInstant = Instant.parse("2021-01-15T00:00:00.000Z")
      )
    )
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forGenerator(_ => new DoNothingWatermarkGenerator())
        .withTimestampAssigner(
          new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(
                element: ShoppingCartEvent,
                recordTimestamp: Long
            ): Long = element.time.toEpochMilli
          }
        )
    )
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    .process(new CountInAllWindow(_ => true))

  // The job is stuck. No watermark is issues, Flink does not have a clue when to release any windows.

  def main(args: Array[String]): Unit = {
    // threeSecondsWindowAll.print()
    // env.execute()

    // shoppingCartEvents.print()
    // processingTimeShoppingCartEventsEnv.execute()

    // eventTimeShoppingCartCountInWindows.print()
    // eventTimeShoppingCartEventsEnv.execute()

    // countWithCustomWatermarkGenerator.print()
    // customWatermarkGeneratorEnv.execute()

    noWatermarkCount.print()
    noWatermarkEnv.execute()
  }

}
