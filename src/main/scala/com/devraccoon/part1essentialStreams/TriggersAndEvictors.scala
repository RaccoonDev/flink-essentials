package com.devraccoon.part1essentialStreams

import com.devraccoon.shopping.{
  ShoppingCartEvent,
  SingleShoppingCartEventsGenerator
}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{
  Trigger,
  TriggerResult
}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

import java.lang
import java.time.Instant

object TriggersAndEvictors {

  // Every window has a way to tell when window results must be send for processing.
  // That mechanism is called triggers. Each window assigner comes with own default trigger.
  // TumblingProcessingTimeWindows assigned comes with ProcessingTimeTriggers, TumblingEventTimeWindows
  // assigner comes with EventTimeTrigger.

  // Any trigger has couple responsibilities:
  // - tell window assigned what to do with result when time comes. Available actions are do nothing, release the
  //      results, clear the elements in the window, do both release and clear.
  // - merge itself with another trigger when correspondent windows are being merged
  // - do something when results are being released from a window

  def main(args: Array[String]): Unit = {

    // Flink comes with built-in triggers: EventTimeTrigger, ProcessingTimeTrigger, CountTrigger, PurgingTrigger

    // Let's see count trigger in action

    val countTriggerEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val windowedShoppingCartEventsStream =
      countTriggerEnv
        .addSource(
          new SingleShoppingCartEventsGenerator(
            500,
            Instant.parse("2021-01-15T00:00:00.000Z")
          )
        )
        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))

    // As a reminder here. Without custom trigger this setup is going to release a window every
    // 10 seconds and count elements within the window. Since, we are issuing 2 events per second
    // each window will have ~20 elements based on processing time.
    // windowedShoppingCartEventsStream.process(new CountInAllWindow(_ => true)).print()

    // now let's add a count trigger that should release window results without purging every 4 elements
//    windowedShoppingCartEventsStream
//      .trigger(CountTrigger.of[TimeWindow](4))
//      .process(new CountInAllWindow(_ => true))
//      .print()

    // Sample output of the above:
    /*
      3> (TimeWindow{start=1639315980000, end=1639315990000},4)
      4> (TimeWindow{start=1639315990000, end=1639316000000},4)
      5> (TimeWindow{start=1639315990000, end=1639316000000},8)
      6> (TimeWindow{start=1639315990000, end=1639316000000},12)
      7> (TimeWindow{start=1639315990000, end=1639316000000},16)
      8> (TimeWindow{start=1639315990000, end=1639316000000},20)
      9> (TimeWindow{start=1639316000000, end=1639316010000},4)
      10> (TimeWindow{start=1639316000000, end=1639316010000},8)
      11> (TimeWindow{start=1639316000000, end=1639316010000},12)
      12> (TimeWindow{start=1639316000000, end=1639316010000},16)
      1> (TimeWindow{start=1639316000000, end=1639316010000},20)
      2> (TimeWindow{start=1639316010000, end=1639316020000},4)

     As you can see the window is changing every 10 seconds, but for every window we are getting results
     on every 4 elements. And the elements remain in the window until next fire.
     */

    // Now let's see what would happen if we convert this trigger to a purging one.

//    windowedShoppingCartEventsStream
//      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](4)))
//      .process(new CountInAllWindow(_ => true))
//      .print()

    /*
      Sample output of the above:
      3> (TimeWindow{start=1639316150000, end=1639316160000},4)
      4> (TimeWindow{start=1639316160000, end=1639316170000},4)
      5> (TimeWindow{start=1639316160000, end=1639316170000},4)
      6> (TimeWindow{start=1639316160000, end=1639316170000},4)
      7> (TimeWindow{start=1639316160000, end=1639316170000},4)
      8> (TimeWindow{start=1639316160000, end=1639316170000},4)
      9> (TimeWindow{start=1639316170000, end=1639316180000},4)
      10> (TimeWindow{start=1639316170000, end=1639316180000},4)

      Now we are getting 4 event per trigger. Already triggered elements are removed from the window.
      But the window keep changing every 10 seconds.
     */

    // Now let's implement a simplified version of count trigger ourselves. We are omitting complexity related
    // to fault tolerance and state management.
    class MyCountTrigger[W <: Window](maxCount: Long) extends Trigger[Any, W] {
      var count = 0

      override def onElement(
          element: Any,
          timestamp: Long,
          window: W,
          ctx: Trigger.TriggerContext
      ): TriggerResult = {
        count += 1
        if (count >= maxCount) {
          count = 0
          TriggerResult.FIRE
        } // Let's release the results
        else TriggerResult.CONTINUE // do nothing
      }

      override def onProcessingTime(
          time: Long,
          window: W,
          ctx: Trigger.TriggerContext
      ): TriggerResult = TriggerResult.CONTINUE // do nothing

      override def onEventTime(
          time: Long,
          window: W,
          ctx: Trigger.TriggerContext
      ): TriggerResult = TriggerResult.CONTINUE // do nothing

      override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
        count = 0
      }
    }

//    windowedShoppingCartEventsStream
//      .trigger(new MyCountTrigger[TimeWindow](4))
//      .process(new CountInAllWindow(_ => true))
//      .print()

    /*
    Sample output:
    12> (TimeWindow{start=1639316700000, end=1639316710000},4)
    1> (TimeWindow{start=1639316700000, end=1639316710000},8)
    2> (TimeWindow{start=1639316700000, end=1639316710000},12)
    3> (TimeWindow{start=1639316710000, end=1639316720000},4)
    4> (TimeWindow{start=1639316710000, end=1639316720000},8)
    5> (TimeWindow{start=1639316710000, end=1639316720000},12)
    6> (TimeWindow{start=1639316710000, end=1639316720000},16)

    So, our trigger works the same the built-in one, but without fault tolerance and merging technique
     */

    // let's see if we can create our own timeout trigger that will release elements if the stream is idle for
    // a given time interval.

    class MyProcessingTimeoutTrigger[W <: Window](idleTimeoutMillis: Long)
        extends Trigger[Any, W] {

      var timeoutValue: Option[Long] = None

      override def onElement(
          element: Any,
          timestamp: Long,
          window: W,
          ctx: Trigger.TriggerContext
      ): TriggerResult = {

        val newTimer = ctx.getCurrentProcessingTime + idleTimeoutMillis

        timeoutValue.fold({
          // timeoutValue is empty
          ctx.registerProcessingTimeTimer(newTimer)
          timeoutValue = Some(newTimer)
        }) { t =>
          ctx.deleteProcessingTimeTimer(t) // remove previous timer
          ctx.registerProcessingTimeTimer(newTimer) // set new one
          timeoutValue = Some(newTimer)
        }
        TriggerResult.CONTINUE
      }

      override def onProcessingTime(
          time: Long,
          window: W,
          ctx: Trigger.TriggerContext
      ): TriggerResult = TriggerResult.FIRE

      override def onEventTime(
          time: Long,
          window: W,
          ctx: Trigger.TriggerContext
      ): TriggerResult = TriggerResult.CONTINUE // do nothing

      override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
        timeoutValue = None
      }
    }

    // To test the timeout trigger I want to add extra delay every 10 elements
    val windowedShoppingCartEventsStreamWithExtraDelay =
      countTriggerEnv
        .addSource(
          new SingleShoppingCartEventsGenerator(
            500,
            Instant.parse("2021-01-15T00:00:00.000Z"),
            Option(7000)
          )
        )
        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))

//    windowedShoppingCartEventsStreamWithExtraDelay
//      .trigger(new MyProcessingTimeoutTrigger[TimeWindow](5000))
//      .process(new CountInAllWindow(_ => true))
//      .print()

    // With 5 seconds timeout the above trigger should release results on every delay cause by avery 10 elements (note that we fire
    // an event every 500ms with extra delay of 7 seconds every 10 events)
    /*
        4> (TimeWindow{start=1639318290000, end=1639318300000},10)
        5> (TimeWindow{start=1639318300000, end=1639318310000},10)
        6> (TimeWindow{start=1639318310000, end=1639318320000},9)
        7> (TimeWindow{start=1639318320000, end=1639318330000},1)
        8> (TimeWindow{start=1639318320000, end=1639318330000},5)
        9> (TimeWindow{start=1639318330000, end=1639318340000},6)
        10> (TimeWindow{start=1639318330000, end=1639318340000},7)

        TODO: Check what exactly is causing that behavior.
     */

    /**
      * Exercise: create a custom trigger that will release results either if X elements received or no elements
      *           received during Y milliseconds.
      */

    // TODO: Implement the exercise

    // Evictors are much simpler: these gives ability to remove elements from a window after the trigger fires and
    // before and/or after teh window function is applied.

    class MyCountEvictor(maxSize: Int, runBefore: Boolean)
        extends Evictor[ShoppingCartEvent, TimeWindow] {

      private def evict(
          size: Int,
          elements: lang.Iterable[TimestampedValue[ShoppingCartEvent]]
      ): Unit =
        if (size > maxSize) {
          val iterator = elements.iterator()
          (0 until size - maxSize).foreach { _ =>
            iterator.next()
            iterator.remove()
          }
        }

      override def evictBefore(
          elements: lang.Iterable[TimestampedValue[ShoppingCartEvent]],
          size: Int,
          window: TimeWindow,
          evictorContext: Evictor.EvictorContext
      ): Unit = if (runBefore) evict(size, elements)

      override def evictAfter(
          elements: lang.Iterable[TimestampedValue[ShoppingCartEvent]],
          size: Int,
          window: TimeWindow,
          evictorContext: Evictor.EvictorContext
      ): Unit = if (!runBefore) evict(size, elements)
    }

    // This configuration should keep only 3 last events per window
//    windowedShoppingCartEventsStream
//      .evictor(new MyCountEvictor(3, true))
//      .process(new CountInAllWindow(_ => true))
//      .print()

    /*
      Sample output:
      4> (TimeWindow{start=1639336320000, end=1639336330000},2)
      5> (TimeWindow{start=1639336330000, end=1639336340000},3)
      6> (TimeWindow{start=1639336340000, end=1639336350000},3)
      7> (TimeWindow{start=1639336350000, end=1639336360000},3)

      For every window we emitting only 3 events per window. Others
      events are evicted by our evictor.
     */

    countTriggerEnv.execute()

  }

}
