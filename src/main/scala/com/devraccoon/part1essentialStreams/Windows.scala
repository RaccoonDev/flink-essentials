package com.devraccoon.part1essentialStreams

import com.devraccoon.utils.{
  CountAggregate,
  CountInAllWindowFunction,
  CountInWindow
}
import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.time.Instant
import scala.concurrent.duration._

object Windows {

  // As usual, let's grab an execution environment
  val env: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  // Streaming application can be defined by four distinct questions that we can ask about the processing
  // (the source of this questions is the O'Reilly "Streaming Systems" book)
  // - What - results are calculated?
  // - Where - in event time the results are calculated?
  // - When - in processing time the results are materialized?
  // - How - do we refine results?

  // Let's imagine that we have a stream of events from some online game like StarCraft.
  // Events that we are getting from a single stream can be found in `com.devraccoon.starcraft.events` object.
  import com.devraccoon.starcraft.events._

  // lets define a point in the time that we'll use as reference for our exercise
  implicit val startTime: Instant =
    Instant.parse("2022-02-02T00:00:00.000Z") // 1643760000000

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

  // That should be enough for a quick review of what windows can do for us.
  val stream: DataStream[ServerEvent] = env
    .fromCollection(events)
    // THE LINES BELOW ADDED WHEN [[MARK A]] REACHED
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(
              element: ServerEvent,
              recordTimestamp: Long
          ): Long = element.eventTime.toEpochMilli
        })
    )

  // Windows can be keyed or not. That is defined by either .keyby function was called or not.
  // Let's start with non-keyed one.
  val threeSecondsTumblingWindowNonKeyed
      : AllWindowedStream[ServerEvent, TimeWindow] =
    stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  // Next let's define some windows on top of the data.
  // let's say that we want to calculate how many registrations we have per 3 seconds.

  // How would we imagine the result of a streaming application? In our simple example,
  // we have a bounded dataset, but let's pretend that the data set is potentially unbounded - infinite.
  // Result of the question above (how many registrations we have per 3 seconds) in a data stream would be
  // a sequence of results per each window. Each window lasts 3 seconds.
  // The entity that is responsible of assigning events to a particular window is called "assigner".
  // Flink comes with bunch different assigned available out of the box.

  // With that we have defined Where in event timeline we want to calculate results.
  // With the events above we are expecting:

  /*

  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    | carl online  |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one -----------|^|-------------- window two -----------------|^|------------- window three --------------|^|----------- window four ----------|^
  |                                     |                                             |                                           |                                    |
  |            1 registrations          |               3 registrations               |              1 registration               |            0 registrations         |
  |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
   */
  // So, we expect 4 results: 4 windows / 1 result per window

  val nonKeyedRegistrationsPerThreeSeconds: DataStream[(TimeWindow, Long)] =
    threeSecondsTumblingWindowNonKeyed.process(
      new CountInAllWindowFunction[ServerEvent, String](
        _.isInstanceOf[PlayerRegistered]
      )
    )

  // let's print it and see the results.
  // OOPS! Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
  // Exactly we did not tell Flink where to read event time. Let's do that.
  // [[MARK A]]
  // Re-run with printing in the main

  // Example of a keyed stream. Key here represents a very unique id of an event
  val streamByType: KeyedStream[ServerEvent, String] =
    stream.keyBy(e => e.getClass.getSimpleName)

  val threeSecondsTumblingWindows
      : WindowedStream[ServerEvent, String, TimeWindow] =
    streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  val countPerTypePerWindow: DataStream[(String, TimeWindow, Long)] =
    threeSecondsTumblingWindows
      .process(
        new CountInWindow[ServerEvent, String]()
      )
  // with the above we can see that we are getting results per type per window

  def main(args: Array[String]): Unit = {

    nonKeyedRegistrationsPerThreeSeconds.print()
    countPerTypePerWindow.print()

    env.execute()
  }
}
