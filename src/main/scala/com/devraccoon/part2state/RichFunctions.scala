package com.devraccoon.part2state

import com.devraccoon.shopping.{
  AddToShoppingCartEvent,
  SingleShoppingCartEventsGenerator
}
import org.apache.flink.api.common.functions.{
  MapFunction,
  RichFlatMapFunction,
  RichMapFunction
}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFunctions {

  /*
    To start our conversation about state and stateful stream processing in Flink,
    we should discuss the main mechanism to get access to that features in Flink.

    And that mechanism is called Rich Functions.
   */

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamOfInts: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // As you have seen in part 1 processing functions can be defined in flink using
    // couple different methods. For instance a transformation can be coded as a call to 'map'
    // method of a stream.
    val streamOfDoubledIntsAsString: DataStream[String] =
      streamOfInts.map(i => (i * 2).toString)

//    streamOfDoubledIntsAsString.print()

    /*
      Sample output:
        2
        4
        6
        8
     */

    // The exactly same effect can be achieved by creating an anonymous object of a MapFunction interface
    val streamOfDoubledIntsAsString_2: DataStream[String] =
      streamOfInts.map(new MapFunction[Int, String] {
        override def map(value: Int): String = (value * 2).toString
      })

//    streamOfDoubledIntsAsString_2.print()
    // Output is the same as above.

    // Note that the same can be applied to flat mapping with the main difference that
    // flat mapping in flink can send out zero or many elements.

    // And there is a third way of achieving the same result in flink: Using Rich* functions.
    // in out case that would be RichMapFunction.

    val richMapFunctionProcessed: DataStream[String] =
      streamOfInts.map(new RichMapFunction[Int, String] {
        override def map(value: Int): String = (value * 2).toString
      })

//    richMapFunctionProcessed.print()
    // Output is the same.

    // Now let's check why we have that extra mechanism.

    // MapFunction is an interface with a single method 'def map(value: IN): OUT'.
    // But RichMapFunction is an abstract class that implements the same MapFunction interface,
    // and inherits from AbstractRichFunction. AbstractRichFunction in turn implements RichFunction
    // interface. Let's see what is there.

    // RichFunction interface contains
    // - def open(parameters: Configuration): Unit
    // - def close(): Unit
    // - def getRuntimeContext(): RuntimeContext
    // - def getIterationRuntimeContext(): IterationRuntimeContext
    // - def setRuntimeContext(context: RuntimeContext): Unit

    // And those functions are very very important for us. Open and close functions give us a chance to perform
    // initialization, preparation and resources clean up for our processing functions. While runtime context
    // can provide us with access to parts of execution environment where the processing function is running right now.

    // Let's see what we can do with RichFunction and MapFunction interfaces together (hence RichMapFunction).

    streamOfInts
      .map(new RichMapFunction[Int, String] {
        override def open(parameters: Configuration): Unit = {
          println("!!! Starting my work !!!")
        }

        override def close(): Unit = {
          println("!!! Finishing my work !!!")
        }

        // The only mandatory method to implement
        override def map(value: Int): String = (value * 2).toString
      })
//      .print()

    /*
      Sample output:
        !!! Starting my work !!!
        2
        4
        6
        8
        !!! Finishing my work !!!
     */

    // That's cool. What else is there?

    streamOfInts
      .map(new RichMapFunction[Int, String] {
        override def open(parameters: Configuration): Unit = {
          val jobId = getRuntimeContext.getJobId
          println(s"!!! Starting my work ($jobId) !!!")
        }

        override def close(): Unit = {
          val jobId = getRuntimeContext.getJobId
          println(s"!!! Finishing my work ($jobId) !!!")
        }

        // The only mandatory method to implement
        override def map(value: Int): String = (value * 2).toString
      })
      .print()

    /*
      Sample output:
        !!! Starting my work !!!
        !!! Starting my work (98676289ab824cc6eabc59b51571e927) !!!
        2
        4
        6
        8
        !!! Finishing my work (98676289ab824cc6eabc59b51571e927) !!!
        !!! Finishing my work !!!

        Wait a minute. What are we looking at right now? Why "Starting my work" appears two times?
        That shows that flink executes all rich functions disregard if the output of the functions is being used.

        Labels without ids are printed by print lines of previous example. But I have commented out the print
        task in that previous example, so the output is not printed, but 'open' and 'close' methods are called
        anyway,
     */

    // Skimming through runtime context properties and methods you can find access to many many Flink features
    // such as metrics, accumulators, distributed cache, broadcasted state, normal state, stats, etc.

    // That's why RichFunctions are so important for us when we start working with state.

    env.execute()

    /**
      * Exercise time: get our shopping cart stream and convert that to stream
      *                that outputs a single event per each ordered sku element.
      *
      *                AddToShoppingCart(sku="boots", quantity=2), AddToShoppingCart(sku="jacket", quantity=1)
      *                should be transformed to
      *                "boots", "boots", "jacket"
      *
      *                I'll help with setting up a stream of AddToShoppingCart events.
      */

    val shoppingCartProcessingEnv =
      StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEvents = shoppingCartProcessingEnv
      .addSource(
        new SingleShoppingCartEventsGenerator(100)
      )
      .map(_ match {
        case e: AddToShoppingCartEvent => e
      })

    shoppingCartEvents
      .flatMap(
        new RichFlatMapFunction[AddToShoppingCartEvent, String] {
          override def flatMap(
              value: AddToShoppingCartEvent,
              out: Collector[String]
          ): Unit = {
            println(s"Processing: $value")
            Range.Int(0, value.quantity, 1).foreach(_ => out.collect(value.sku))
          }
        }
      )
      .print()

    /*
      Sample output:
        Processing: AddToShoppingCartEvent(Alice,1f35baec-9636-4040-84a2-ba33fbbc7a34,1,2022-01-03T14:18:40.932Z)
        11> 1f35baec-9636-4040-84a2-ba33fbbc7a34
        Processing: AddToShoppingCartEvent(Alice,c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1,8,2022-01-03T14:18:41.932Z)
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
        12> c6ace93f-e0ea-49ff-b3b7-469c71aa0ad1
     */

    shoppingCartProcessingEnv.execute()

    // What other rich functions (Rich*Functions) are available:
    // - RichMapFunction
    // - RichFlatMapFunction
    // - RichFilterFunction
    // - RichReduceFunction

    // The idea is the same for all of the above. Though, there is one universal function that can be used for everything:
    //
    //        |--------------------------------|
    //        |       The ProcessFunction.     |
    //        |--------------------------------|
    //
    // That one inherits from AbstractRichFunction and add methods to work with timers: define what should be processed
    // relatively to either processing or event time.

    // Spoiler alert: almost everything in flink can be defined as a ProcessFunction. That's probably the most powerful
    // abstraction in Flink.

    class MyProcessFunction
        extends ProcessFunction[AddToShoppingCartEvent, String] {
      override def open(parameters: Configuration): Unit =
        super.open(parameters)

      override def close(): Unit = super.close()

      override def onTimer(
          timestamp: Long,
          ctx: ProcessFunction[AddToShoppingCartEvent, String]#OnTimerContext,
          out: Collector[String]
      ): Unit = super.onTimer(timestamp, ctx, out)

      // The only mandatory method
      // context here is a way to get access to time characteristics and side outputs.
      override def processElement(
          value: AddToShoppingCartEvent,
          ctx: ProcessFunction[AddToShoppingCartEvent, String]#Context,
          out: Collector[String]
      ): Unit =
        Range.Int(0, value.quantity, 1).foreach(_ => out.collect(value.sku))
    }

    // We'll get deeper to that abstraction later on during the course.
  }

}
