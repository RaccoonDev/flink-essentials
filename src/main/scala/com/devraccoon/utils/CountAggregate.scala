package com.devraccoon.utils

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.{
  ProcessAllWindowFunction,
  ProcessWindowFunction
}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Aggregate function that just counts the elements of a stream in a window.
  * @param predicate takes a predicate to check if value must be counted or not
  */
class CountAggregate[A](predicate: A => Boolean)
    extends AggregateFunction[A, Long, Long] {

  /**
    * That's basically count
    * @return 0 as starting value of accumulator
    */
  override def createAccumulator(): Long = 0L

  /**
    * This function decides if we need to increase count or not
    * @param value new element
    * @param accumulator count value
    * @return new count
    */
  override def add(value: A, accumulator: Long): Long =
    if (predicate(value)) accumulator + 1 else accumulator

  /**
    * Returns the final result of the counter
    * @param accumulator counter so far
    * @return final value of the counter
    */
  override def getResult(accumulator: Long): Long = accumulator

  /**
    * When process is running in parallel and the aggregation is merged from different streams
    * this method tells how to merge the counter.
    *
    * Here it is a simple sum.
    * @param a count from one stream
    * @param b count from another stream
    * @return merge of the counts
    */
  override def merge(a: Long, b: Long): Long = a + b
}

class CountInWindow[A, Key]
    extends ProcessWindowFunction[A, (Key, TimeWindow, Long), Key, TimeWindow] {
  override def process(
      key: Key,
      context: Context,
      elements: Iterable[A],
      out: Collector[(Key, TimeWindow, Long)]
  ): Unit = {
    out.collect((key, context.window, elements.size))
  }
}

class CountInAllWindowFunction[A, Key](predicate: A => Boolean)
    extends ProcessAllWindowFunction[A, (TimeWindow, Long), TimeWindow] {
  override def process(
      context: Context,
      elements: Iterable[A],
      out: Collector[(TimeWindow, Long)]
  ): Unit = {
    out.collect((context.window, elements.count(predicate)))
  }
}
