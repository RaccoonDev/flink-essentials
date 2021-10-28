package com.devraccoon.part1essentialStreams

import com.devraccoon.part1essentialStreams.Windows.CountInAllWindow
import com.devraccoon.shopping.{ShoppingCartEvent, ShoppingCartEventsGenerator}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{
  CountTrigger,
  PurgingTrigger,
  Trigger,
  TriggerResult
}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

object WindowAssignersAndTriggers {

  // Here let's explore what other mechanism in Flink can release a window
  // and affect released results

  // And that would be triggers and Evictors

  // Triggers determine when a window is ready to be processed by the window function.
  // Each window assigner comes with default trigger, but we can use a custom one
  // if default behavior is not exactly what you need.

  // For instance if you need to process window by count of the events.

  /*

    Flink comes with the following built-in custom triggers:
    - EventTimeTriggers <- fires when event time watermark is registered
    - ProcessingTimeTrigger <- fires based on processing time
    - CountTrigger <- fires once the number of events in a window exceeds a given value
    - PurgingTrigger <- wraps a trigger and transforms it into a purging one
   */

  // Let's experiment with count trigger.
  val basicCountTriggerEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  // We will get new events using our shopping cart events generator
  val countTriggerOutputCount: DataStream[(TimeWindow, Long)] =
    basicCountTriggerEnv
    // The below is going to generate events in batches of 2 items in one second
      .addSource(new ShoppingCartEventsGenerator(500, 2))
      // We are using processing time windows of 10 seconds here
      // So, without a trigger this window would contain 20 events
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      // adding a trigger that is going to release window result every 10 events
      .trigger(CountTrigger.of[TimeWindow](10))
      .process(new CountInAllWindow(_ => true))

  /*
  That's the example output:
    11> (TimeWindow{start=1635332210000, end=1635332220000},10)
    12> (TimeWindow{start=1635332220000, end=1635332230000},10)
    1> (TimeWindow{start=1635332220000, end=1635332230000},20)
    2> (TimeWindow{start=1635332230000, end=1635332240000},10)
    3> (TimeWindow{start=1635332230000, end=1635332240000},20)
    4> (TimeWindow{start=1635332240000, end=1635332250000},10)
    5> (TimeWindow{start=1635332240000, end=1635332250000},20)
    6> (TimeWindow{start=1635332250000, end=1635332260000},10)
    7> (TimeWindow{start=1635332250000, end=1635332260000},20)

   In the output above we can see that we have two outputs per window (except first one).
   12> (TimeWindow{start=1635332220000, end=1635332230000},10)
    1> (TimeWindow{start=1635332220000, end=1635332230000},20)

    2> (TimeWindow{start=1635332230000, end=1635332240000},10)
    3> (TimeWindow{start=1635332230000, end=1635332240000},20)

    4> (TimeWindow{start=1635332240000, end=1635332250000},10)
    5> (TimeWindow{start=1635332240000, end=1635332250000},20)

    For every window we have two results fired into our processing function: one with 10 elements, another one with 20 elements.
    So, what's happening:

    Window starts to accumulate events and when the specified trigger counts 10 events everything that is in
    the window is released to results. For a freshly created window it is only 10 events. But for the same window that is
    still exists in the processing time of interval of 10 seconds we receive another 10 events. Trigger counts these 10
    event and fires the same window with all the events accumulated. So, next fire for the same window contains all the
    20 events.

    And that's the difference between purging and not purging triggers. Non purging triggers keep accumulated events
    in the window.

    Now let's see what would happen if we wrap the trigger with purging trigger.
   */

  val purgingCountTriggerEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  val purgingTriggerOutputCount: DataStream[(TimeWindow, Long)] =
    purgingCountTriggerEnv
      .addSource(new ShoppingCartEventsGenerator(500, 2))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](10)))
      .process(new CountInAllWindow(_ => true))

  /*
    Output example:
    12> (TimeWindow{start=1635342170000, end=1635342180000},10)
    1> (TimeWindow{start=1635342170000, end=1635342180000},10)
    2> (TimeWindow{start=1635342180000, end=1635342190000},10)
    3> (TimeWindow{start=1635342180000, end=1635342190000},10)
    4> (TimeWindow{start=1635342190000, end=1635342200000},10)
    5> (TimeWindow{start=1635342190000, end=1635342200000},10)
    6> (TimeWindow{start=1635342200000, end=1635342210000},10)
    7> (TimeWindow{start=1635342200000, end=1635342210000},10)

    Now we see the similar picture of two triggers per window, but now the number of events is 10 for both
    fires. That's because the events are removed from window when a trigger is fired and processing function
    sees only new events on each run.
   */

  // Custom triggers can be useful to control windows firing in cases like: fire a window when 1000 events accumulated
  // or there are no events in 30 seconds. That's basically a batch cap limit with timeout.

  // To implement a trigger we can inherit from abstract trigger class. Before showing code for that it is good
  // to understand what decision making outcomes can trigger return:
  // - continue: do nothing
  // - fire: release window events for processing function
  // - purge: remove events in the window
  // - fire_and_purge: release the window events and remove events from the window

  // Let's see how TimedOutCountTrigger can be implemented:
  class TimedOutCountTrigger[E, W <: Window](
      maxCount: Long,
      timeoutMillis: Long
  ) extends Trigger[E, W] {

    // Self explanatory
    private val defaultDeadlineValue = Long.MaxValue

    // These two are new concepts: trigger must store current counters
    // and deadline values somewhere. Flink providers state-related facilities
    // and to access those we need a description of what we want.
    // Here we want to keep two long values in state. These descriptions says
    // exactly that: associate long value with key "count" and another long value
    // with key "deadline".
    private val countStateDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor(
        "count",
        classOf[Long]
      )
    private val deadlineDescriptor: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("deadline", classOf[Long])

    // This is called on every received element
    override def onElement(
        element: E,
        timestamp: Long,
        window: W,
        ctx: Trigger.TriggerContext
    ): TriggerResult = {
      // Given Flink context we can get access to the inner state facilities
      // passing our descriptors. Note that state management is partition aware.
      // We'll dig into that in later chapters.
      val deadlineState = ctx.getPartitionedState(deadlineDescriptor)
      val countState = ctx.getPartitionedState(countStateDescriptor)

      // State values might not be yet initialized, so, let's handle missing
      // values and defaults here
      val currentTime = System.currentTimeMillis()
      val currentDeadline =
        Option(deadlineState.value()).getOrElse(defaultDeadlineValue)
      val newCount = Option(countState.value()).getOrElse(0L) + 1L

      // Now the trigger outcomes
      if (currentTime >= currentDeadline || newCount >= maxCount) {
        // We can release the window results now.
        // this returns Fire outcome and resets the state to default
        deadlineState.update(defaultDeadlineValue)
        countState.update(0L)
        println(
          s"Firing the triggers onElement. " +
            s"Current time: [${currentTime >= currentDeadline}]; Count: [${newCount >= maxCount}]"
        )
        TriggerResult.FIRE
      } else {

        // We are not yet done with the window, so let's continue,
        if (currentDeadline == defaultDeadlineValue) {
          // but as side effect, if deadline is set to default, we need to
          // store a new value of it in state and setup a processing time
          // trigger in case no new events coming in
          val nextDeadline = currentTime + timeoutMillis
          deadlineState.update(nextDeadline)
          ctx.registerProcessingTimeTimer(nextDeadline)
        }

        // This is self explanatory
        countState.update(newCount)

        TriggerResult.CONTINUE
      }

    }

    // Triggered on a processing-time timer set by context
    // Remember the processing-time timers setup in onElement method.
    // When time comes the below would be called.
    override def onProcessingTime(
        time: Long,
        window: W,
        ctx: Trigger.TriggerContext
    ): TriggerResult = {
      val deadlineState = ctx.getPartitionedState(deadlineDescriptor)
      // There might be new timeout already configured because new elements arrived.
      if (deadlineState.value() == time) {
        // If no new elements so far, let's release the results
        deadlineState.update(defaultDeadlineValue)
        ctx.getPartitionedState(countStateDescriptor).update(0L)
        println("Firing the trigger onProcessingTime")
        TriggerResult.FIRE
      } else {
        TriggerResult.CONTINUE
      }
    }

    // Triggered on a event-time timer set by context
    // We are not interested on that here.
    override def onEventTime(
        time: Long,
        window: W,
        ctx: Trigger.TriggerContext
    ): TriggerResult = TriggerResult.CONTINUE

    // Clears the state of this trigger
    override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
      println(s"[${System.currentTimeMillis()}] clearing the trigger")
      val deadlineState = ctx.getPartitionedState(deadlineDescriptor)
      val deadline = deadlineState.value()
      // Timer cleanup
      if (deadline != defaultDeadlineValue) {
        ctx.deleteProcessingTimeTimer(deadline)
      }
      // State cleanup
      deadlineState.clear()
      ctx.getPartitionedState(countStateDescriptor).clear()
    }

  }

  val customTriggerEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment
  val customTriggerCounts: DataStream[(TimeWindow, Long)] =
    customTriggerEnvironment
      .addSource(new ShoppingCartEventsGenerator(500, 2))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .trigger(new TimedOutCountTrigger[ShoppingCartEvent, TimeWindow](5, 2000))
      .process(new CountInAllWindow(_ => true))

  /*
    With timeout set to 200 milliseconds we hit timeout first because we are generating two
    events per 1 second.

    1> (TimeWindow{start=1635346540000, end=1635346550000},2)
    2> (TimeWindow{start=1635346540000, end=1635346550000},4)
    3> (TimeWindow{start=1635346540000, end=1635346550000},6)
    4> (TimeWindow{start=1635346540000, end=1635346550000},8)
    5> (TimeWindow{start=1635346540000, end=1635346550000},10)
    6> (TimeWindow{start=1635346550000, end=1635346560000},2)
    7> (TimeWindow{start=1635346550000, end=1635346560000},4)
    8> (TimeWindow{start=1635346550000, end=1635346560000},6)
    9> (TimeWindow{start=1635346550000, end=1635346560000},8)
    10> (TimeWindow{start=1635346550000, end=1635346560000},10)
    11> (TimeWindow{start=1635346550000, end=1635346560000},12)
    12> (TimeWindow{start=1635346550000, end=1635346560000},14)
    1> (TimeWindow{start=1635346550000, end=1635346560000},16)

  Increasing trigger timeout to 2000ms we are getting increments of 5 or what is caused by the timeout
    8> (TimeWindow{start=1635417750000, end=1635417760000},5)<- trigger fired because count is equal to 5.
                                                                  but we were generating 2 events in batch with 1 second
                                                                  delay between batches. That means that one event is still in the queue.


    9> (TimeWindow{start=1635417750000, end=1635417760000},8)
    10> (TimeWindow{start=1635417750000, end=1635417760000},12)
    11> (TimeWindow{start=1635417760000, end=1635417770000},4)
    12> (TimeWindow{start=1635417760000, end=1635417770000},8)
    1> (TimeWindow{start=1635417760000, end=1635417770000},12)
    2> (TimeWindow{start=1635417760000, end=1635417770000},16)
    3> (TimeWindow{start=1635417770000, end=1635417780000},4)
    4> (TimeWindow{start=1635417770000, end=1635417780000},8)
    5> (TimeWindow{start=1635417770000, end=1635417780000},12)
    6> (TimeWindow{start=1635417770000, end=1635417780000},16)
    7> (TimeWindow{start=1635417780000, end=1635417790000},4)
    8> (TimeWindow{start=1635417780000, end=1635417790000},8)
    9> (TimeWindow{start=1635417780000, end=1635417790000},12)
    10> (TimeWindow{start=1635417780000, end=1635417790000},16)

    !!! Apparently the trigger is losing events !!!
   */

  def main(args: Array[String]): Unit = {
    // countTriggerOutputCount.print()
    // basicCountTriggerEnv.execute()

    // purgingTriggerOutputCount.print()
    // purgingCountTriggerEnv.execute()

    customTriggerCounts.print()
    customTriggerEnvironment.execute()
  }

}
