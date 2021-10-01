package com.devraccoon.part1essentialStreams

import com.devraccoon.utils.{CountInAllWindow, CountInWindow}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

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
  |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
  |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
   */
  // So, we expect 4 results: 4 windows / 1 result per window

  val nonKeyedRegistrationsPerThreeSeconds: DataStream[(TimeWindow, Long)] =
    threeSecondsTumblingWindowNonKeyed.process(
      new CountInAllWindow(
        _.isInstanceOf[PlayerRegistered]
      )
    )

  // let's print it and see the results.
  // OOPS! Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
  // Exactly we did not tell Flink where to read event time. Let's do that.
  // [[MARK A]]
  // Re-run with printing in the main

  /*
    Sample output:
    4> (TimeWindow{start=1643760003000, end=1643760006000},3)
    6> (TimeWindow{start=1643760009000, end=1643760012000},0)
    3> (TimeWindow{start=1643760000000, end=1643760003000},1)
    5> (TimeWindow{start=1643760006000, end=1643760009000},2)
   */

  // Now let's see what happens if we create keyed stream. Let's group events by simple class name.
  val streamByType: KeyedStream[ServerEvent, String] =
    stream.keyBy(e => e.getClass.getSimpleName)

  // Each elements of the original stream is going to be assigned to a separate "stream" for a given key
  val threeSecondsTumblingWindows
      : WindowedStream[ServerEvent, String, TimeWindow] =
    streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))
  /*

  We can expect something like that:

  === Registration Events Stream ===
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |               |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one -----------|^|-------------- window two -----------------|^|------------- window three --------------|^|----------- window four ----------|^
  |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
  |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |

  === Online Events Stream ===
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob online      |                 | sam online        |       | mary online     |       |                 |     | rob online    | carl online  |
  |         |         |                 |                 |                   |       |                 |       |                 |     | alice online  |              |
  ^|------------ window one -----------|^|-------------- window two -----------------|^|------------- window three --------------|^|----------- window four ----------|^
  |            1 online                 |               1 online                      |              1 online                     |            3 online                |
  |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |

  So, there would be 7 elements in the output of this processing: 3 windows with count of registrations and 4 windows with count of online events.
   */

  // Let's see what we would have for counting.
  val countPerTypePerWindow: DataStream[(String, TimeWindow, Long)] =
    threeSecondsTumblingWindows
      .process(
        new CountInWindow()
      )
  /*
  1> (PlayerRegistered,TimeWindow{start=1643760000000, end=1643760003000},1)
  6> (PlayerOnline,TimeWindow{start=1643760000000, end=1643760003000},1)
  6> (PlayerOnline,TimeWindow{start=1643760003000, end=1643760006000},1)
  1> (PlayerRegistered,TimeWindow{start=1643760003000, end=1643760006000},3)
  6> (PlayerOnline,TimeWindow{start=1643760006000, end=1643760009000},1)
  1> (PlayerRegistered,TimeWindow{start=1643760006000, end=1643760009000},2)
  6> (PlayerOnline,TimeWindow{start=1643760009000, end=1643760012000},3)
   */

  // Sliding windows
  val windowSize: Time = Time.seconds(3)
  val windowSlide: Time = Time.seconds(1)
  val slidingWindowsAll: AllWindowedStream[ServerEvent, TimeWindow] =
    stream.windowAll(SlidingEventTimeWindows.of(windowSize, windowSlide))

  /*
  === Sliding windows ===
    Assuming that are counting registration in the same fashion
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    | carl online  |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one -----------|^|
                1 registration

           |^|---------------- window two ---------------|^|
                                  2 registrations

                      |^|------------------- window three -------------------|^|
                                           4 registrations

                                        |^|---------------- window four ---------------|^|
                                                          3 registrations

                                                         |^|---------------- window five --------------|^|
                                                                           3 registrations

                                                                              |^|---------- window six --------|^|
                                                                                           1 registration

                                                                                       |^|------------ window seven -----------|^|
                                                                                                    2 registrations

                                                                                                        |^|------- window eight-------|^|
                                                                                                                  1 registration

                                                                                                               |^|----------- window nine -----------|^|
                                                                                                                        1 registration

                                                                                                                                  |^|---------- window ten ---------|^|
                                                                                                                                              0 registrations
   9 windows in total with sliding number of registrations for each window:
    Sequence of counts: 1, 2, 4, 3, 3, 1, 2, 1, 1, 0
   */

  val registrationInSlidingWindow: DataStream[(TimeWindow, Long)] =
    slidingWindowsAll.process(
      new CountInAllWindow(
        _.isInstanceOf[PlayerRegistered]
      )
    )
  // Sample output of printing the above:
  /*
    (TimeWindow{start=1643760000000, end=1643760003000},1)
    (TimeWindow{start=1643760001000, end=1643760004000},2)
    (TimeWindow{start=1643760002000, end=1643760005000},4)
    (TimeWindow{start=1643760003000, end=1643760006000},3)
    (TimeWindow{start=1643760004000, end=1643760007000},3)
    (TimeWindow{start=1643760005000, end=1643760008000},1)
    (TimeWindow{start=1643760006000, end=1643760009000},2)
    (TimeWindow{start=1643760007000, end=1643760010000},1)
    (TimeWindow{start=1643760008000, end=1643760011000},1)
    (TimeWindow{start=1643760009000, end=1643760012000},0)
    (TimeWindow{start=1643760010000, end=1643760013000},0)
    (TimeWindow{start=1643760011000, end=1643760014000},0)
   */

  // Session window
  // For this type of window we define a gap between events. When gap is big enough, old window
  // is closed and new one is opened.

  // Using the data that we are already familiar with, let's see who from the users had two session
  // if we define that a gap between sessions is 2 seconds
  val sessionTimedWindows: AllWindowedStream[ServerEvent, TimeWindow] =
    stream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(2)))

  val countEventsInSession: DataStream[(TimeWindow, Long)] =
    sessionTimedWindows.process(
      new CountInAllWindow(_ => true)
    )

  // if we print the above as is we would spot a one big window with all elements counted in.
  // that happened because we are not separating the stream by users and all events in the stream
  // are quite close together to form one large session.

  // let's see how that will look like with proper keyed stream
  val keyedSessionWindows: WindowedStream[ServerEvent, String, TimeWindow] =
    stream
      .keyBy(_.getId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(2)))

  val countEventsInKeyedSession: DataStream[(String, TimeWindow, Long)] =
    keyedSessionWindows.process(
      new CountInWindow
    )

  /*
  As expected we have:
  - 1 window for bob, sam and mary
  - 2 windows for carl, alice and rob
  Number of windows here equals number of sessions with a gap of 2 seconds
   */

  // global window
  // global window is one lage window for all events. More like batch mode.
  val globalWindowAll: DataStream[(GlobalWindow, Long)] = stream
    .windowAll(GlobalWindows.create())
    .trigger(CountTrigger.of[GlobalWindow](10))
    .process(new CountInAllWindow(_ => true))

  // Ooops! Nothing is returned. Why is that?
  // Global window is global. Means that for potentially unbounded stream
  // the global window is infinite. Flink needs a hint here when to release
  // the window and run the transformations. The hint in a form of a custom
  // trigger.
  // Custom trigger can be based on count, for instance.

  // With added trigger we have only one result fired with 10 elements counted
  /*
  Example output:
  5> (GlobalWindow,10)
   */

  /**
    * Exercise:
    * - What was the time slot of two seconds when we had the most number of online players?
    */

  def main(args: Array[String]): Unit = {

    // nonKeyedRegistrationsPerThreeSeconds.print()
    // countPerTypePerWindow.print()
    // registrationInSlidingWindow.print()
    // countEventsInSession.print()
    // countEventsInKeyedSession.print()
    globalWindowAll.print()

    env.execute()
  }
}
