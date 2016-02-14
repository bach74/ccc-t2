/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.streaming;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage: DirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or more
 * Kafka brokers <topics> is a list of one or more kafka topics to consume from
 *
 * Example: $ bin/run-example streaming.KafkaWordCount broker1-host:port,broker2-host:port topic1,topic2
 */
public final class JavaDirectKafkaWordCount
{
	private static final Pattern SPACE = Pattern.compile(" ");

	private static final Logger LOGGER = Logger.getLogger(JavaDirectKafkaWordCount.class);

	private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

	private static class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable
	{
		private Comparator<V> comparator;

		public ValueComparator(Comparator<V> comparator)
		{
			this.comparator = comparator;
		}

		@Override
		public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2)
		{
			return comparator.compare(o1._2(), o2._2());
		}
	}

	private static Function2<List<Long>, Optional<Long>, Optional<Long>> COMPUTE_RUNNING_SUM = (nums, current) -> {
		long sum = current.or(0L);
		for (long i : nums) {
			sum += i;
		}
		return Optional.of(sum);
	};

	public static void main(String[] args)
	{
		if (args.length < 2) {
			System.err.println("Usage: DirectKafkaWordCount <brokers> <topics>\n" + "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
		if (!log4jInitialized) {
			// We first log something to initialize Spark's default logging, then we override the
			// logging level.
			LOGGER.info("Setting log level to [WARN] for streaming example." + " To override add a custom log4j.properties to the classpath.");
			Logger.getRootLogger().setLevel(Level.WARN);
		}

		String brokers = args[0];
		String topics = args[1];

		// Create context with 2 second batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");

		try (JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(4))) {

			// must set for statefull operations
			jssc.checkpoint(".");

			// initialize Kafka Consumer
			HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
			HashMap<String, String> kafkaParams = new HashMap<String, String>();
			kafkaParams.put("metadata.broker.list", brokers);
			// start from begin
			kafkaParams.put("auto.offset.reset", "smallest");
			// Create direct kafka stream with brokers and topics
			JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
					StringDecoder.class, kafkaParams, topicsSet);

			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				@Override
				public String call(Tuple2<String, String> tuple2)
				{
					return tuple2._2();
				}
			});

			JavaDStream<OriginDestInput> originDestinationStream = lines.map(OriginDestInput::parseFromLogLine);

			// Now we have non-empty lines, lets split them into words
			/*
			 * JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			 * 
			 * @Override public Iterable<String> call(String x) { return Lists.newArrayList(SPACE.split(x)); } });
			 * 
			 * // Convert words to Pairs, remember the TextPair class in Hadoop world JavaPairDStream<String, Long> wordCounts = words.mapToPair(new
			 * PairFunction<String, String, Long>() {
			 * 
			 * @Override public Tuple2<String, Long> call(String s) { return new Tuple2<String, Long>(s, 1L); } });
			 */

			LOGGER.info("----***#### Starting KafkaWordCount ####***----");

			// This will give a Dstream made of state (which is the cumulative count of the words)

			JavaPairDStream<String, Long> stateDstream = originDestinationStream.mapToPair(s -> new Tuple2<>(s.getOrigin(), 1L)).reduceByKey(SUM_REDUCER)
					.updateStateByKey(COMPUTE_RUNNING_SUM);

			stateDstream.print();

			// Top words
			stateDstream.foreachRDD(rdd -> {
				List<Tuple2<String, Long>> topWords = rdd.takeOrdered(10, new ValueComparator<>(Comparator.<Long> naturalOrder()));
				System.out.println("Top Words: " + topWords);
				return null;
			});

			// Start the computation
			jssc.start();
			jssc.awaitTermination();
		}
	}

}