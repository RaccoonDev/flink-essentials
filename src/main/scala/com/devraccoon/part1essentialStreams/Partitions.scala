package com.devraccoon.part1essentialStreams

import com.devraccoon.shopping.{
  AddToShoppingCartEvent,
  ShoppingCartEvent,
  SingleShoppingCartEventsGenerator
}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object Partitions {

  /*
   Flink is a distributed stateful streaming processing engine. "Distributed" here means that the application
   is supposed to run on many machines. So, each processing unit (a computer) should get a portion of incoming stream
   to process. This portion of events is called partition. Also, stream processing can be started from several sources.
   Anyway, Flink somehow must know which computer should process what:


                            |-------------|                                        |--------------|
                            |             |--[Subscript of Shopping Cart Events]-->| Processor #1 |
                            |             |                                        |--------------|
  --[Shopping Cart Event]-->| Partitioner |              .... N times.....
                            |             |                                        |--------------|
                            |             |--[Subscript of Shopping Cart Events]-->| Processor #N |
                            |-------------|                                        |--------------|

    Exactly how efficient processing would happen is very related to both partitioning and parallelism setting.

    Parallelism tells flink how many physical works you would like to have in the application running in parallel.
    Let's say we have Flink cluster with 10 task managers running. Each task manager is configured to have 5 slots.

    That means that each task manager is capable of running 5 tasks in parallel. Hence, total capacity of the cluster is
    50 tasks.

    If you specify that parallelism for a given job should be one, that would tell flink to have only one task running per
    each operation. Partitioning would still happen. Here I mean a process when a stream element is taken and hash code or key
    is calculated. But because we have parallelism of 1, all stream elements would end up in the same single processing task.

    If we defined parallelism to be 3. Flink would instantiate up to 3 versions of job's tasks and partitioned stream elements
    would be routed to a destination processing slot based on partition code that was calculated.

    Very important to have a very consistent logic of partitioning incoming elements. When we would cover state, that
    would become even more important, because state is bound to an instant of a task, so If we send all user's stream
    element to a single task (partitioning by user_id) we must be sure that we won't randomly send data to another task
    instance that is responsible for a different subset of users.

    As summary:
    - Number of tasks is controlled by desired parallelism
    - Each tasks received a subset of partitioned data for processing
    - Tasks are expected to run in parallel on different machines, so, networking and serialization is happening
   */

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents: DataStream[ShoppingCartEvent] =
      env
        .addSource(
          new SingleShoppingCartEventsGenerator(100)
        )

    // Flink gives access to a custom logic on how to define a partition.
    // The below one is the example. Partition function is called with some value and
    // total number of partitioned and it should return one of the partitions for the given
    // key.
    val partitioner = new Partitioner[String] {
      // Here we take a hashcode of the key and module that by number of partitions
      // to get a desired partition index. Hopefully our hashCode function is uniform enough
      // to provide good distribution of the events.
      // Let's as well include a printing line to show number of partitions
      override def partition(key: String, numPartitions: Int): Int = {
        println(s"Number of partitions: $numPartitions")
        key.hashCode % numPartitions
      }
    }
    val partitionedStream: DataStream[ShoppingCartEvent] =
      shoppingCartEvents.partitionCustom(
        partitioner,
        shoppingCartEvent => shoppingCartEvent.userId
      )

//    partitionedStream.print()

    /*
    Sample output:
      Number of partitions: 12
      5> AddToShoppingCartEvent(Alice,23af312c-89d5-461f-9032-d04b10537e10,5,2021-12-27T14:58:32.355Z)
      Number of partitions: 12
      8> AddToShoppingCartEvent(Sam,c6cec556-e2c0-46f0-b5f3-e2e35cdc7ff7,7,2021-12-27T14:58:33.355Z)
      Number of partitions: 12
      10> AddToShoppingCartEvent(Rob,12c07b67-a8e9-4d19-85fe-f282d885b4cd,6,2021-12-27T14:58:34.355Z)
      Number of partitions: 12
      6> AddToShoppingCartEvent(Bob,8b1a8024-373f-461e-bf09-9f637aa6f4f4,5,2021-12-27T14:58:35.355Z)
      Number of partitions: 12

     As we can see the number of partitions is 12. And partitioner is called for every stream element. And output
     stream elements are reported from different workers ("10>", "6>", etc.). So, our function properly distributed the
     values among available workers.
     */

    // Let's try simulate a bad-written hash function or partitioner

    val awfulPartitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int =
        numPartitions - 1
    }

    // Can think of why it is awful?

    // Since this partitioner always returns last available partition index, all elements would be routed to a single
    // processing task. That basically kills distribution benefits.

    val awfullyPartitionedStream =
      shoppingCartEvents.partitionCustom(awfulPartitioner, _.userId)
//    awfullyPartitionedStream.print()

    /*
    Sample output:
    12> AddToShoppingCartEvent(Sam,94c21fc0-c62d-4fae-bc5d-84700adf82b7,7,2021-12-27T15:03:51.060Z)
    12> AddToShoppingCartEvent(Rob,a8aecc74-1b4b-493f-8871-51778b9dc782,6,2021-12-27T15:03:52.060Z)
    12> AddToShoppingCartEvent(Tom,cba06b65-9ba6-453d-81bf-7703a4f284f7,7,2021-12-27T15:03:53.060Z)
    12> AddToShoppingCartEvent(Rob,26057184-b76d-4169-903a-2de5cca733f3,8,2021-12-27T15:03:54.060Z)
    12> AddToShoppingCartEvent(Rob,412f523a-8dc3-4fd4-8672-988c30451bab,3,2021-12-27T15:03:55.060Z)
    12> AddToShoppingCartEvent(Sam,a3e5fe99-105f-459a-b4c7-e36d708de97f,7,2021-12-27T15:03:56.060Z)
    12> AddToShoppingCartEvent(Rob,c6fe44e7-de3b-48fc-8366-875d2865dd63,4,2021-12-27T15:03:57.060Z)

    As you can see all events are reported from a single processing task with index 12.
     */

    // There are cases when you need to control the number of running tasks. For instance if you must have only one
    // task that sends data to a service via HTTP. To control that it is much better to just specify parallelism as 1 and not
    // mess it up with custom partitioner.

    // For instance:
//    env.setParallelism(1) // <- this one will do the trick
    val sizedParallelPartitionedStream = shoppingCartEvents
      .partitionCustom(partitioner, _.userId)
//    sizedParallelPartitionedStream.print()

    /*
    Sample output:
    Number of partitions: 1
    AddToShoppingCartEvent(Alice,ce87fec3-48d8-4ecb-9515-70ec0290ce55,7,2021-12-27T15:11:41.722Z)
    Number of partitions: 1
    AddToShoppingCartEvent(Tom,781df9f9-99bd-47d7-b5a4-51815abab941,4,2021-12-27T15:11:42.722Z)
    Number of partitions: 1
    AddToShoppingCartEvent(Alice,02980a01-fe27-4176-b255-c6c3cfdbd78c,4,2021-12-27T15:11:43.722Z)

    With that we have only one task that is processing our stream.
     */

    // In Flink we have a "shuffle" function that if called will redistribute incoming stream elements randomly through
    // available partitions.
    // We can demonstrate the effect of shuffle when it negates custom partition locked to a single index:
    val shuffledStream = awfullyPartitionedStream.shuffle
//    shuffledStream.print()

    /*
    Sample output:
    3> AddToShoppingCartEvent(Tom,ea717809-28c6-4fbf-994c-b6ce67ce8352,7,2021-12-27T15:15:39.427Z)
    5> AddToShoppingCartEvent(Sam,199789b3-1ab8-4654-924f-4235b2908fee,2,2021-12-27T15:15:40.427Z)
    4> AddToShoppingCartEvent(Sam,aaff4acc-a1db-4b58-bbae-36e2e9e6a250,7,2021-12-27T15:15:41.427Z)
    6> AddToShoppingCartEvent(Alice,edcc9d19-0f93-488f-8ce2-4699ae590ed2,5,2021-12-27T15:15:42.427Z)
    8> AddToShoppingCartEvent(Tom,c00b8ea7-7279-4925-ad40-f4c279317979,4,2021-12-27T15:15:43.427Z)
    8> AddToShoppingCartEvent(Rob,80fe6630-7789-4878-88e8-a94629f1c60f,0,2021-12-27T15:15:44.427Z)
    11> AddToShoppingCartEvent(Bob,d5290efe-16a8-4335-96bc-89137a4a9796,8,2021-12-27T15:15:45.427Z)
    4> AddToShoppingCartEvent(Bob,3afb9fd6-0a62-4501-893b-2093f2895383,1,2021-12-27T15:15:46.427Z)
    6> AddToShoppingCartEvent(Bob,de15d849-f5df-481f-ad53-d7c3f222e96d,3,2021-12-27T15:15:47.427Z)
    3> AddToShoppingCartEvent(Bob,94181024-333d-4353-8911-1fdac24ead7e,1,2021-12-27T15:15:48.427Z)

    As you can see events again are distributed
     */

    // Now let's talk about Task Chaining and Resource Groups

    /*
    In Flink we do have an optimization technique called chaining. It is quite similar to Fusing in Akka Streams.
    The idea is simple, some transformations are much more efficient if they are happen in the same thread on the same
    machine.

    For instance, if we have Shopping Cart Event and the only data that we need from the events is userid. It is very very
    inefficient to send a shopping cart event over the network to another machine to then extract user id of it. Much better
    option here would be to chain user id extraction into the same operation of reading (or generating) a shopping cart event.

    And flink does chaining automatically whenever it can. That's why using appropriate function interface(Map vs ProcessingFunction)
    is important. These interfaces are promise to Flink of what exactly is going to happen within a function. If it is Map, then
    no side effects to happen and it is safe to chain this operation.

    Switching from streaming to batch job gives flink more information about a dataset, so even more chaining and optimizations
    is possible.

    Complementing automatic chaining process there is an API to control how the chaining is happening.
     */

    // We can create a new chain using "startNewChain" method:
    val addEvents = shoppingCartEvents
      .filter(e => {
        println(s"Filter - ${Thread.currentThread().getId}")
        e.isInstanceOf[AddToShoppingCartEvent]
      })
      .map(e => {
        println(s"First map - ${Thread.currentThread().getId}")
        e.asInstanceOf[AddToShoppingCartEvent]
      })
      .startNewChain()
      .map(e => {
        println(s"Second map - ${Thread.currentThread().getId}")
        e.sku
      })

//    addEvents.print()

    /*
    Sample output:
      3> cd5f7aeb-1b56-4b3c-8066-c82f2448a384
      Filter - 77
      First map - 102
      Second map - 102
      4> 6b206d69-de5b-4261-9e3d-f6c4bfa5bf33
      First map - 103
      Filter - 84
      Second map - 103
      5> 9e2a3662-ee37-477c-9af2-9b3421d21b62
      Filter - 86
      First map - 104
      Second map - 104
      6> 32ef8c51-dd7f-4de3-9eed-2d9c6260c8a2

      Here we see that filter is processed on one thread separately from maps and two maps are running on a single thread.
     */

    // Let's see what would happen by default, without call to a new chain:
    val addEventsNoChain = shoppingCartEvents
      .filter(e => {
        println(s"Filter - ${Thread.currentThread().getId}")
        e.isInstanceOf[AddToShoppingCartEvent]
      })
      .map(e => {
        println(s"First map - ${Thread.currentThread().getId}")
        e.asInstanceOf[AddToShoppingCartEvent]
      })
      .map(e => {
        println(s"Second map - ${Thread.currentThread().getId}")
        e.sku
      })
    addEventsNoChain.print()

    /*
    Sample output:
      Filter - 116
      Filter - 79
      First map - 116
      Second map - 116
      2> d51c0a42-19bf-4f97-9a0d-34c800c35592
      First map - 106
      Second map - 106
      Filter - 117
      Filter - 80
      First map - 117
      Second map - 117
      3> dc4adf49-40b8-4bf5-af8f-31c1e5ca4e3a

    Here output is quite random and filtering happens on the same thread sometimes, sometimes not.
     */

    // Chaining can be disabled by calling "disableChaining" method. I do encourage you to create a sample for that
    // yourself.

    env.execute("Partitions")
  }

}
