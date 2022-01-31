package com.devraccoon.part2state

import com.devraccoon.shopping.{
  ShoppingCartEvent,
  SingleShoppingCartEventsGenerator
}
import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  MapState,
  MapStateDescriptor,
  StateTtlConfig,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object KeyedState {

  /*
   Alright. Now when we have access to "rich" functions that can give us
   access to state, let's see how that can be done.

   Since Flink jobs are distributed and are supposed to process large and unbounded
   streams of data, it is a must have mechanism to split the load among several physical
   computers. Making an application and state distributed and chopped to distributable chunks of data
   give, actually, tons of benefits. Such as smaller and more predictable operations on the state and data.
   (for instance, saving state as a snapshot to a persistent storage).

   That's why the most common type of state is "keyed" state. Means that every processing function happens
   in a context of some already known key.
   */

  def main(args: Array[String]): Unit = {

    // Let's assume that we do want to count number of requests per user in our shopping cart stream processing
    // application.

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEvents =
      env.addSource(
        new SingleShoppingCartEventsGenerator(
          sleepMillisBetweenEvents = 100,
          generateRemoved =
            true // Adding this flag to generate second type of events
        )
      )

    // That's how we do create a keyed stream
    //                                                  ↓ - type of key
    val eventsByUserId: KeyedStream[ShoppingCartEvent, String] =
      //                             ↑ - type of element
      shoppingCartEvents.keyBy(_.userId)

    // Now let's do the calculation, but with one extra requirement, we would like to keep the counter
    // and continue the counter from the already calculated number even after the job restart.
    // For that, let's use Keyed Process Function. Difference between keyed and non-keyed (normal) process
    // function is basically is the function is going to be used in a context of a key.
    eventsByUserId
      .process(
        //                       ↓ - key type | ↓ IN type | ↓ - OUT type
        new KeyedProcessFunction[String, ShoppingCartEvent, String] {

          // Now the trick. Instance of this process function will be unique for a given key.
          // So, in our case, each user id is going to have an independent instance deployed to a one
          // particular compute node. If the node crashes, this instance, would be redeployed to another
          // node. Using state we can make sure that state will be restored as well.

          // Getting back to counting. Let's a variable that will give us access to the state.

          private var stateCounter: ValueState[Long] = _

          // Couple things here:
          // 1. I like to prefix my state variables with word "state". Autocomplete makes it easy to work with later
          //    when I want to use that variable among other things, such as metrics and accumulators.
          // 2. ValueState[T] is a type of state that holds a single value of T.
          // 3. You can extract value from the state by using `.value()` method and update the value by `.update(T v)` method
          // 4. We promised scala compiler that we are going to instantiate the state somewhere before we use that.
          //    This promise must be fulfilled in overridden `open` method.

          override def open(parameters: Configuration): Unit = {
            // First we use current runtime context
            stateCounter = getRuntimeContext
            // ask the runtime context to get value state
              .getState(
                // specify description of the state that you would like to get.
                // This description (defined as a descriptor) give the state some
                // name and types to be used to store the values.
                new ValueStateDescriptor[Long]("events-counter", classOf[Long])
              )
          }

          override def processElement(
              value: ShoppingCartEvent,
              ctx: KeyedProcessFunction[
                String,
                ShoppingCartEvent,
                String
              ]#Context,
              out: Collector[String]
          ): Unit = {
            // Here we do update the value in the state
            stateCounter.update(stateCounter.value() + 1)
            // Here we read the value from the state and send it downstream with user id
            // Note how we can get access to current key using context. So, the key value might
            // not necessarily be part of every element.
            out.collect(
              s"user_id: ${ctx.getCurrentKey} - ${stateCounter.value()}"
            )
          }
        }
      )
//      .print() // Let's print the result.

    /*
      Note how counter is incremented with each iteration per each user id.
      Sample output:
      1> user_id: Sam - 1
      1> user_id: Sam - 2
      1> user_id: Sam - 3
      11> user_id: Tom - 1
      1> user_id: Sam - 4
      9> user_id: Alice - 1
      12> user_id: Bob - 1
      11> user_id: Rob - 1
      11> user_id: Tom - 2
      12> user_id: Bob - 2
      12> user_id: Bob - 3
      11> user_id: Rob - 2
      11> user_id: Tom - 3
      11> user_id: Tom - 4
      1> user_id: Sam - 5
      1> user_id: Sam - 6
      12> user_id: Bob - 4
      11> user_id: Rob - 3
      1> user_id: Sam - 7
      11> user_id: Tom - 5
      11> user_id: Tom - 6
      12> user_id: Bob - 5
      1> user_id: Sam - 8
      11> user_id: Rob - 4
     */

    /*
      Other types of keyed state is available:
      - ListState
      - MapState

      Both do exactly what they should be doing by their names: persist lists and maps (dictionaries).
     */

    // Same as before.
    class ListStateDemoProcessFunction
        extends KeyedProcessFunction[String, ShoppingCartEvent, String] {

      private var stateListOfAllEvents: ListState[ShoppingCartEvent] = _

      override def open(parameters: Configuration): Unit = {
        stateListOfAllEvents = getRuntimeContext
          .getListState( // method to get list state
            new ListStateDescriptor[ShoppingCartEvent](
              "shopping-cart-events", // <- state name
              classOf[ShoppingCartEvent] // <- type of elements in the state
            )
          )
      }

      override def processElement(
          value: ShoppingCartEvent,
          ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
          out: Collector[String]
      ): Unit = {
        // Adding value
        stateListOfAllEvents.add(value)
        // Alternatively can be used
        // stateListOfAllEvents.addAll(List(value).asJava) // Adds elements
        // stateListOfAllEvents.update(List(value).asJava) // Replaces all elements with given list

        val currentListState: Iterable[ShoppingCartEvent] =
          // `get` method returns value as java.lang.Iterable, so converting here to scala Iterable
          stateListOfAllEvents.get().asScala

        out.collect(
          s"user_id: ${ctx.getCurrentKey} - [${currentListState.mkString(",")}]"
        )
      }
    }

//    eventsByUserId.process(new ListStateDemoProcessFunction).print()

    /*
    In this output we can see how the events are being accumulated in the list.
    Sample output:
    11> user_id: Rob - [AddToShoppingCartEvent(Rob,cbdff47e-d705-473c-b3ef-f4e2b007d1ac,4,2022-01-31T15:22:04.661Z)]
    1> user_id: Sam - [AddToShoppingCartEvent(Sam,17c4b4d5-3229-48ec-8f61-f321b6d1fa3e,9,2022-01-31T15:22:05.661Z)]
    11> user_id: Tom - [AddToShoppingCartEvent(Tom,b5b5a4e7-59e0-471b-ba04-60c8c5534b64,2,2022-01-31T15:22:06.661Z)]
    1> user_id: Sam - [AddToShoppingCartEvent(Sam,17c4b4d5-3229-48ec-8f61-f321b6d1fa3e,9,2022-01-31T15:22:05.661Z),AddToShoppingCartEvent(Sam,30e8affc-825f-4387-8107-651d29bbbce0,8,2022-01-31T15:22:07.661Z)]
    11> user_id: Rob - [AddToShoppingCartEvent(Rob,cbdff47e-d705-473c-b3ef-f4e2b007d1ac,4,2022-01-31T15:22:04.661Z),AddToShoppingCartEvent(Rob,68b33ec7-bb06-4dd3-be55-fd2f769029a2,6,2022-01-31T15:22:08.661Z)]
    12> user_id: Bob - [AddToShoppingCartEvent(Bob,f5093f0a-002f-4948-a546-5dcb529cb132,1,2022-01-31T15:22:09.661Z)]
    1> user_id: Sam - [AddToShoppingCartEvent(Sam,17c4b4d5-3229-48ec-8f61-f321b6d1fa3e,9,2022-01-31T15:22:05.661Z),AddToShoppingCartEvent(Sam,30e8affc-825f-4387-8107-651d29bbbce0,8,2022-01-31T15:22:07.661Z),AddToShoppingCartEvent(Sam,df85a997-75a7-4da3-acd1-0cec964a8716,1,2022-01-31T15:22:10.661Z)]
    12> user_id: Bob - [AddToShoppingCartEvent(Bob,f5093f0a-002f-4948-a546-5dcb529cb132,1,2022-01-31T15:22:09.661Z),AddToShoppingCartEvent(Bob,72a33851-dc43-4661-9040-1c4667627623,9,2022-01-31T15:22:11.661Z)]
    1> user_id: Sam - [AddToShoppingCartEvent(Sam,17c4b4d5-3229-48ec-8f61-f321b6d1fa3e,9,2022-01-31T15:22:05.661Z),AddToShoppingCartEvent(Sam,30e8affc-825f-4387-8107-651d29bbbce0,8,2022-01-31T15:22:07.661Z),AddToShoppingCartEvent(Sam,df85a997-75a7-4da3-acd1-0cec964a8716,1,2022-01-31T15:22:10.661Z),AddToShoppingCartEvent(Sam,d04cbc11-977f-49f9-8bf8-d65cee92d553,9,2022-01-31T15:22:12.661Z)]
    1> user_id: Sam - [AddToShoppingCartEvent(Sam,17c4b4d5-3229-48ec-8f61-f321b6d1fa3e,9,2022-01-31T15:22:05.661Z),AddToShoppingCartEvent(Sam,30e8affc-825f-4387-8107-651d29bbbce0,8,2022-01-31T15:22:07.661Z),AddToShoppingCartEvent(Sam,df85a997-75a7-4da3-acd1-0cec964a8716,1,2022-01-31T15:22:10.661Z),AddToShoppingCartEvent(Sam,d04cbc11-977f-49f9-8bf8-d65cee92d553,9,2022-01-31T15:22:12.661Z),AddToShoppingCartEvent(Sam,e61d8ccf-3597-428f-a038-86a6d8ee9872,4,2022-01-31T15:22:13.661Z)]
    11> user_id: Tom - [AddToShoppingCartEvent(Tom,b5b5a4e7-59e0-471b-ba04-60c8c5534b64,2,2022-01-31T15:22:06.661Z),AddToShoppingCartEvent(Tom,1f753576-ff25-4dbd-8492-3071e79a1338,9,2022-01-31T15:22:14.661Z)]
    1> user_id: Sam - [AddToShoppingCartEvent(Sam,17c4b4d5-3229-48ec-8f61-f321b6d1fa3e,9,2022-01-31T15:22:05.661Z),AddToShoppingCartEvent(Sam,30e8affc-825f-4387-8107-651d29bbbce0,8,2022-01-31T15:22:07.661Z),AddToShoppingCartEvent(Sam,df85a997-75a7-4da3-acd1-0cec964a8716,1,2022-01-31T15:22:10.661Z),AddToShoppingCartEvent(Sam,d04cbc11-977f-49f9-8bf8-d65cee92d553,9,2022-01-31T15:22:12.661Z),AddToShoppingCartEvent(Sam,e61d8ccf-3597-428f-a038-86a6d8ee9872,4,2022-01-31T15:22:13.661Z),AddToShoppingCartEvent(Sam,7d489231-9b4c-4b15-9866-8a415b777de5,1,2022-01-31T15:22:15.661Z)]
    12> user_id: Bob - [AddToShoppingCartEvent(Bob,f5093f0a-002f-4948-a546-5dcb529cb132,1,2022-01-31T15:22:09.661Z),AddToShoppingCartEvent(Bob,72a33851-dc43-4661-9040-1c4667627623,9,2022-01-31T15:22:11.661Z),AddToShoppingCartEvent(Bob,cc9aab7f-0ab5-4c47-a967-78a35f4bc686,3,2022-01-31T15:22:16.661Z)]
    1> user_id: Sam - [AddToShoppingCartEvent(Sam,17c4b4d5-3229-48ec-8f61-f321b6d1fa3e,9,2022-01-31T15:22:05.661Z),AddToShoppingCartEvent(Sam,30e8affc-825f-4387-8107-651d29bbbce0,8,2022-01-31T15:22:07.661Z),AddToShoppingCartEvent(Sam,df85a997-75a7-4da3-acd1-0cec964a8716,1,2022-01-31T15:22:10.661Z),AddToShoppingCartEvent(Sam,d04cbc11-977f-49f9-8bf8-d65cee92d553,9,2022-01-31T15:22:12.661Z),AddToShoppingCartEvent(Sam,e61d8ccf-3597-428f-a038-86a6d8ee9872,4,2022-01-31T15:22:13.661Z),AddToShoppingCartEvent(Sam,7d489231-9b4c-4b15-9866-8a415b777de5,1,2022-01-31T15:22:15.661Z),AddToShoppingCartEvent(Sam,71876eba-f789-45ad-88a1-7b0fdecaa6ab,6,2022-01-31T15:22:17.661Z)]
    12> user_id: Bob - [AddToShoppingCartEvent(Bob,f5093f0a-002f-4948-a546-5dcb529cb132,1,2022-01-31T15:22:09.661Z),AddToShoppingCartEvent(Bob,72a33851-dc43-4661-9040-1c4667627623,9,2022-01-31T15:22:11.661Z),AddToShoppingCartEvent(Bob,cc9aab7f-0ab5-4c47-a967-78a35f4bc686,3,2022-01-31T15:22:16.661Z),AddToShoppingCartEvent(Bob,bdfa1bc2-1283-4406-88bf-207806bccb26,2,2022-01-31T15:22:18.661Z)]
    12> user_id: Bob - [AddToShoppingCartEvent(Bob,f5093f0a-002f-4948-a546-5dcb529cb132,1,2022-01-31T15:22:09.661Z),AddToShoppingCartEvent(Bob,72a33851-dc43-4661-9040-1c4667627623,9,2022-01-31T15:22:11.661Z),AddToShoppingCartEvent(Bob,cc9aab7f-0ab5-4c47-a967-78a35f4bc686,3,2022-01-31T15:22:16.661Z),AddToShoppingCartEvent(Bob,bdfa1bc2-1283-4406-88bf-207806bccb26,2,2022-01-31T15:22:18.661Z),AddToShoppingCartEvent(Bob,32299a8c-896b-4832-8b0c-6dd598cbbcaa,7,2022-01-31T15:22:19.661Z)]
    11> user_id: Rob - [AddToShoppingCartEvent(Rob,cbdff47e-d705-473c-b3ef-f4e2b007d1ac,4,2022-01-31T15:22:04.661Z),AddToShoppingCartEvent(Rob,68b33ec7-bb06-4dd3-be55-fd2f769029a2,6,2022-01-31T15:22:08.661Z),AddToShoppingCartEvent(Rob,587f3faa-311b-4756-a42f-73e2ceb65ee1,5,2022-01-31T15:22:20.661Z)]
    11> user_id: Rob - [AddToShoppingCartEvent(Rob,cbdff47e-d705-473c-b3ef-f4e2b007d1ac,4,2022-01-31T15:22:04.661Z),AddToShoppingCartEvent(Rob,68b33ec7-bb06-4dd3-be55-fd2f769029a2,6,2022-01-31T15:22:08.661Z),AddToShoppingCartEvent(Rob,587f3faa-311b-4756-a42f-73e2ceb65ee1,5,2022-01-31T15:22:20.661Z),AddToShoppingCartEvent(Rob,8398c22c-6749-4187-9352-9a261c444cbf,2,2022-01-31T15:22:21.661Z)]
    12> user_id: Bob - [AddToShoppingCartEvent(Bob,f5093f0a-002f-4948-a546-5dcb529cb132,1,2022-01-31T15:22:09.661Z),AddToShoppingCartEvent(Bob,72a33851-dc43-4661-9040-1c4667627623,9,2022-01-31T15:22:11.661Z),AddToShoppingCartEvent(Bob,cc9aab7f-0ab5-4c47-a967-78a35f4bc686,3,2022-01-31T15:22:16.661Z),AddToShoppingCartEvent(Bob,bdfa1bc2-1283-4406-88bf-207806bccb26,2,2022-01-31T15:22:18.661Z),AddToShoppingCartEvent(Bob,32299a8c-896b-4832-8b0c-6dd598cbbcaa,7,2022-01-31T15:22:19.661Z),AddToShoppingCartEvent(Bob,9d813950-fc46-4789-88b1-cca59d473b97,0,2022-01-31T15:22:22.661Z)]
    11> user_id: Rob - [AddToShoppingCartEvent(Rob,cbdff47e-d705-473c-b3ef-f4e2b007d1ac,4,2022-01-31T15:22:04.661Z),AddToShoppingCartEvent(Rob,68b33ec7-bb06-4dd3-be55-fd2f769029a2,6,2022-01-31T15:22:08.661Z),AddToShoppingCartEvent(Rob,587f3faa-311b-4756-a42f-73e2ceb65ee1,5,2022-01-31T15:22:20.661Z),AddToShoppingCartEvent(Rob,8398c22c-6749-4187-9352-9a261c444cbf,2,2022-01-31T15:22:21.661Z),AddToShoppingCartEvent(Rob,cb8c3364-f988-4025-8706-814a788c9946,4,2022-01-31T15:22:23.661Z)]
    9> user_id: Alice - [AddToShoppingCartEvent(Alice,a18fecea-52b6-4bd2-a090-c85054f8cc77,4,2022-01-31T15:22:24.661Z)]
     */

    // Now let's explore MapState

    class MapStateDemoProcessFunction
        extends KeyedProcessFunction[String, ShoppingCartEvent, String] {

      // Here we will keep the actual type of events represented as sting as key
      // for map state
      private var stateCounterPerEventType: MapState[String, Long] = _

      override def open(parameters: Configuration): Unit = {
        stateCounterPerEventType = getRuntimeContext.getMapState(
          new MapStateDescriptor[String, Long](
            "per-type-counters",
            classOf[String], // type information for key
            classOf[Long] // type information for value
          )
        )
      }

      override def processElement(
          value: ShoppingCartEvent,
          ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
          out: Collector[String]
      ): Unit = {
        val eventTypeString = value.getClass.getSimpleName
        stateCounterPerEventType.put(
          eventTypeString,
          stateCounterPerEventType.get(eventTypeString) + 1
        )
        out.collect(
          s"user_id: ${ctx.getCurrentKey} - ${stateCounterPerEventType.entries().asScala}"
        )
      }
    }

//    eventsByUserId.process(new MapStateDemoProcessFunction).print()

    /*
    Sample output:
    1> user_id: Sam - Wrappers.JIterableWrapper(AddToShoppingCartEvent=1)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=1)
    1> user_id: Sam - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=1)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=2)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=3)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=1)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=3)
    9> user_id: Alice - Wrappers.JIterableWrapper(AddToShoppingCartEvent=1)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=1)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=1)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=2)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=2)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=4)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=3, AddToShoppingCartEvent=4)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=3)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=2)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=3)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=2)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=3)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=4, AddToShoppingCartEvent=4)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=4)
    1> user_id: Sam - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=1)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=4, AddToShoppingCartEvent=5)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=5)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=4)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=3, AddToShoppingCartEvent=2)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=3, AddToShoppingCartEvent=3)
    1> user_id: Sam - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=2)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=3, AddToShoppingCartEvent=4)
    1> user_id: Sam - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=3)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=4, AddToShoppingCartEvent=6)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=4, AddToShoppingCartEvent=4)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=5)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=6)
    1> user_id: Sam - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=3, AddToShoppingCartEvent=3)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=5, AddToShoppingCartEvent=4)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=7)
    11> user_id: Rob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=5, AddToShoppingCartEvent=6)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=6)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=1, AddToShoppingCartEvent=8)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=8)
    12> user_id: Bob - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=2, AddToShoppingCartEvent=9)
    9> user_id: Alice - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=3, AddToShoppingCartEvent=6)
    11> user_id: Tom - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=5, AddToShoppingCartEvent=5)
    1> user_id: Sam - Wrappers.JIterableWrapper(RemovedFromShoppingCartEvent=3, AddToShoppingCartEvent=4)

    As you can see different event types are being counted in the map as expected.
     */

    // There are couple more operations that are available for the state in Flink. Among those
    // are state `clear` and `timeToLive`.
    // - `clear` basically removes the state freeing up the occupied space. Note that cleanup request and
    //   actual data removal do not happen at the same time and you can chose if you want to see data marked
    //   as "to be removed" while it is not yet permanently deleted.
    // - `timeToLive` specifies amount of time to keep the state around before it will be automatically deleted.
    //   You can specify when the TTL counter should be reset: either on writer or writes and reads. That can
    //   help with keeping the data set small and probably to conform to some regulations if you need to setup
    //   automatic deletion of some data in, let's say 90 days.

    // Let's explore these two in the next process function.

    class CleanUpAndTtlDemoProcessFunction
        extends KeyedProcessFunction[String, ShoppingCartEvent, String] {

      private var stateCounter: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        val descriptor =
          new ValueStateDescriptor[Long]("counter", classOf[Long])
        descriptor.enableTimeToLive(
          StateTtlConfig
            .newBuilder(Time.hours(1)) // expire in one hour
            .setUpdateType(
              StateTtlConfig.UpdateType.OnCreateAndWrite
            ) // update TTL on create and write
            .setStateVisibility(
              StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp // keep returning state until physical deletion
            )
            .build()
        )
        stateCounter = getRuntimeContext.getState(
          descriptor
        )
      }

      override def processElement(
          value: ShoppingCartEvent,
          ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
          out: Collector[String]
      ): Unit = {
        // Let's reset the counter every 3 events
        if (stateCounter.value() >= 3) stateCounter.clear()

        stateCounter.update(stateCounter.value() + 1)
        out.collect(s"user_id: ${ctx.getCurrentKey} - ${stateCounter.value()}")
      }
    }

//    eventsByUserId.process(new CleanUpAndTtlDemoProcessFunction).print()

    /*
    Sample output that shows that clean up works.
    12> user_id: Bob - 1
    11> user_id: Tom - 1
    1> user_id: Sam - 1
    9> user_id: Alice - 1
    11> user_id: Rob - 1
    1> user_id: Sam - 2
    11> user_id: Tom - 2
    1> user_id: Sam - 3
    9> user_id: Alice - 2
    9> user_id: Alice - 3
    11> user_id: Rob - 2
    12> user_id: Bob - 2
    9> user_id: Alice - 1
    1> user_id: Sam - 1
    12> user_id: Bob - 3
    11> user_id: Tom - 3

     */

    /**
      * Exercise time. Create a processing function that will count number of events
      *   in next 10 seconds after receiving one and resets its timer afterwards repeating the aggregation.
      *   That function is pretty much a windowing function with start on receiving an event.
      *
      *   Hint: use onTimer method
      */

    class TenSecondsAggregatingCount
        extends KeyedProcessFunction[String, ShoppingCartEvent, String] {

      // Let's create a stateful counter
      private var stateCounter: ValueState[Long] = _
      private var stateCounting: ValueState[Boolean] = _

      override def open(parameters: Configuration): Unit = {
        stateCounter = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("counter", classOf[Long])
        )

        stateCounting = getRuntimeContext.getState(
          new ValueStateDescriptor[Boolean]("isCounting", classOf[Boolean])
        )
      }

      override def processElement(
          value: ShoppingCartEvent,
          ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
          out: Collector[String]
      ): Unit = {
        stateCounter.update(stateCounter.value() + 1)
        if (!stateCounting.value()) {
          ctx
            .timerService()
            .registerEventTimeTimer(
              ctx.timestamp() + Time.seconds(10).toMilliseconds
            )
          stateCounting.update(true)
        }
      }

      override def onTimer(
          timestamp: Long,
          ctx: KeyedProcessFunction[
            String,
            ShoppingCartEvent,
            String
          ]#OnTimerContext,
          out: Collector[String]
      ): Unit = {
        out.collect(
          s"user_id: ${ctx.getCurrentKey} - collected ${stateCounter.value()} in last 10 seconds"
        )
        stateCounting.clear()
        stateCounter.clear()
      }
    }

    eventsByUserId.process(new TenSecondsAggregatingCount).print()

    /*
    TODO: Consider maybe to change this. Cause this one is tricky. Especially in the setup
          of artificial event times in this generator. Event time is not equal to processing time
          so, output comes quite fast because Flink works in the artificial time ignoring wallclock processing
          time. Maybe confusing for new people.

    Sample output:
    2> user_id: Bob - collected 1 in last 10 seconds
    1> user_id: Sam - collected 6 in last 10 seconds
    11> user_id: Rob - collected 5 in last 10 seconds
    11> user_id: Tom - collected 1 in last 10 seconds
    9> user_id: Alice - collected 5 in last 10 seconds
    1> user_id: Sam - collected 2 in last 10 seconds
    12> user_id: Bob - collected 1 in last 10 seconds
    9> user_id: Alice - collected 3 in last 10 seconds
    1> user_id: Sam - collected 2 in last 10 seconds
    9> user_id: Alice - collected 4 in last 10 seconds
     */

    env.execute()

  }
}
