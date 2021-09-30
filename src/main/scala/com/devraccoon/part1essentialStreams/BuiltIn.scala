package com.devraccoon.part1essentialStreams

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

/**
  * This section covers introductory Flink elements end to end.
  * It covers basic built in sources, operators and sinks. There are
  * changes to Flink API that marked a lot of things as deprecated, so
  * immediately we start pointing what interfaces are deprecated and which
  * other APIs to use instead.
  */
object BuiltIn {

  /**
    * We are starting with initialization of Streaming environment
    * that is part of DataStream API covered withing first section
    * of this course.
    *
    * We are getting access to all Flink mechanics using this environment
    * instance.
    *
    * Everything that we put in between environment initialization and environment
    * execution is a blueprint of a Flink Job that we would like Flink to execute.
    *
    * See env.execute in the main method.
    */
  val env: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  // using environment we create above we can create the easiest stream of items
  // from some element

  // Note that this one needs TypeInformation as an implicit.
  // Type information can be found in package: import org.apache.flink.streaming.api.scala._
  val myDataStreamOfInt: DataStream[Int] = env.fromElements(1, 2, 3, 4)

  // Now we can use well know transformations such as map, flatMap, filter
  // note that this creates another stream instance.
  val myDataStreamX10: DataStream[Int] = myDataStreamOfInt.map(_ * 10)

  // flatMap
  val myDataStreamX10WithDuplicates: DataStream[Int] =
    myDataStreamX10.flatMap(n => List(n, n))

  // filter
  val myDataStreamX10WithDuplicatesEven: DataStream[Int] =
    myDataStreamX10WithDuplicates.filter(_ % 2 == 0)

  // the above is enough for now to demonstrate some Flink specifics
  // lets write this into a file.
  // "writeAsText" API is marked as deprecated and not reliable to use
  // for anything, except debugging purposes. We'll see later how to
  // properly write data to disk with exactly-once delivery guarantees.
  val myTextSink: DataStreamSink[Int] =
    myDataStreamX10WithDuplicatesEven.writeAsText(
      "outputs/myDataStreamX10WithDuplicatesEven.out"
    )

  // Let's run the function and see the result.
  // if you see error "Unable to initialize main class com.devraccoon.part1essentialStreams.BuiltIn"
  // that means that there is no Flink libraries available in class path on execution.
  // That is intentional. When we have a packaged application we don't want to ship Flink with every application
  // that we're running on Flink. That's why Flink dependencies are marked as "Provided".
  // So, we have two ways of fixing that:
  // 1 - in this SBT project we have a "mainRunner" that will remove "Provided" flag for the dependencies.
  //     To activate that go to: Menu -> Run -> Edit Configurations... -> BuiltIn -> for "-cp" choose "mainRunner"
  //     instead of "flink-essentials"
  // 2 - go to build.sbt and remove "provided" flag from flink dependencies. Just don't forget to put them back
  //     when you'll ship your application to a Flink cluster.

  // Now when you run the application that will run successfully.
  // Let's see the result:
  // Surprisingly instead of one text file called "myDataStreamX10WithDuplicatesEven.out" we see a folder called
  // "myDataStreamX10WithDuplicatesEven.out" with 8 files: 4 empty and 4 not empty.
  // each non empty file contains a pair of duplicated stream elements.

  // That's because of the parallel nature of Flink. Let's check our parallelism settings.
  println(s"Parallelism: ${env.getParallelism}")

  // Running the job, let's make sure that the destination folder is deleted first.

  // Alright. Parallelism level is set to 8. That explains the number of files pre-created by
  // Flink in anticipation of the incoming data.

  // Let's try to set parallelism to 1 and see how that will do. Again, deleting the destination folder.
  env.setParallelism(1)
  println(s"New parallelism: ${env.getParallelism}")

  // hm... nothing happened. We still have the same output: a directory with 8 files.
  // Let's check where else we can control parallelism:

  // we can set parallelism to the instance of data set just before the sink:
  myDataStreamX10WithDuplicatesEven.setParallelism(1)

  // let's see what that does
  // same result

  // let's see what would happen if we set parallelism on the sink itself
  myTextSink.setParallelism(1)

  // Ha! now we do have one file as output with pairs written out. They are
  // out of order as pairs, because the processing before sink happened in parallel.

  // We'll get back to parallelism and more detailed control of the parallelism later.
  // For now let's keep sink parallelism as a default value.

  // It looks like a time for an exercise.

  /**
    * Exercise (fizz-buzz on flink):
    * - Given case class that represents fizz-buzz game output
    *   Fizz Buzz game: for numbers from 1 to 100, if a number divides by 3, say Fizz, if by 5 say Buzz,
    *    if both, by 3 and 5, say FizzBuzz.
    * - using built-in function "fromSequence" get a Stream with 1 to 100 numbers
    * - map the stream of numbers to the correct FizzBuzzOutput
    * - filter only those who should say FizzBuzz
    * - print them to a single CSV file
    *   hint: use writeAsCsv function.
    */

  // Let's use a clean new env
  val exerciseEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  case class FizzBuzzOutput(n: Long, output: String)

  /* That might be tempting to write everything like that:
     but it is failing because Flink cannot fully understand the
     type of Map function.
  env
    .generateSequence(1, 100)
    .map {
      case x if x % 3 == 0 && x % 5 == 0 => FizzBuzzOutput(x, "FizzBuzz")
      case x if x % 3 == 0               => FizzBuzzOutput(x, "Fizz")
      case x if x % 5 == 0               => FizzBuzzOutput(x, "Buzz")
      case x                             => FizzBuzzOutput(x, x.toString)
    }
    .filter(_.output == "FizzBuzz")
    .writeAsCsv("fizzbuzz.csv")
    .setParallelism(1)
   */

  exerciseEnv
    .fromSequence(1, 100)
    .map((n: Long) =>
      n match {
        case x if x % 3 == 0 && x % 5 == 0 => FizzBuzzOutput(x, "FizzBuzz")
        case x if x % 3 == 0               => FizzBuzzOutput(x, "Fizz")
        case x if x % 5 == 0               => FizzBuzzOutput(x, "Buzz")
        case x                             => FizzBuzzOutput(x, x.toString)
      }
    )
    .filter(_.output == "FizzBuzz")
    // Let's use overwrite mode not to bother with deleting the file each time
    .writeAsCsv(
      "outputs/fizzbuzz.csv",
      writeMode = FileSystem.WriteMode.OVERWRITE
    )
    .setParallelism(1)

  // Now let's see how we can achieve the same result without using deprecated API

  val envNoDeprecated: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  // let's gram our fizz buzz generator
  envNoDeprecated
    .fromSequence(1, 100)
    .map((n: Long) =>
      n match {
        case x if x % 3 == 0 && x % 5 == 0 => FizzBuzzOutput(x, "FizzBuzz")
        case x if x % 3 == 0               => FizzBuzzOutput(x, "Fizz")
        case x if x % 5 == 0               => FizzBuzzOutput(x, "Buzz")
        case x                             => FizzBuzzOutput(x, x.toString)
      }
    )
    // The simplest sink implementation can be just a function:
    // .addSink(fb => println(s"I am FizzBuzzOutput: $fb"))
    //
    // The above is fine to print stuff to the console. But is it a good idea to
    // writing something to a file like that?
    // We are in the world of potentially unbounded data sets. That means that if we
    // naively would write to a single file, the file size potentially would be infinite.
    // That's not very practical and most likely will crash quite soon. Also Flink is a fault tolerant
    // and stateful processing, that means that Sink mechanisms must support that as well.
    //
    // At the same time, writing to File System is a very very common task. That's why
    // Flink comes with quite sophisticated mechanism of writing streams into a file.
    // Meet StreamingFileSink class.
    // To create one we need to specify the directory for output and how to write data into a file.
    // Please note that the Path comes from "org.apache.flink.core.fs.Path".
    .addSink(
      StreamingFileSink
        .forRowFormat(
          new Path("outputs/streaming_sink"),
          new SimpleStringEncoder[FizzBuzzOutput]("UTF-8")
        )
        // Additionally for encoding, rolling policy can be specified
        .withRollingPolicy(
          DefaultRollingPolicy
            .builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
            .withInactivityInterval(TimeUnit.SECONDS.toMillis(30))
            .withMaxPartSize(1024 * 1024) // in bytes. This is 1Mb
            .build()
        )
        .build()
    )

  // let's run it (don't forget to switch execute in the main method.
  // We have 8 files created in quite a sophisticated file and directory structure.

  // let's run it again and see what would happen.
  // number of files doubled. Cool. Nothing lost. Everything is there.

  // The state of files and the files lifecycle we'll cover in one of our next lessons.

  def main(args: Array[String]): Unit = {

    // synchronous execution of a Flink job without specifying job name
    // env.execute()
    // exerciseEnv.execute()
    // envNoDeprecated.execute()
    val r = envNoDeprecated.execute("Fizz Buzz to File System")
    println(s"Job finished with result: $r")

    // synchronous execution of a Flink job with specifying job name
    // env.execute("Built In Essentials")

    // async flink job execution
    // env.executeAsync()
  }

}
