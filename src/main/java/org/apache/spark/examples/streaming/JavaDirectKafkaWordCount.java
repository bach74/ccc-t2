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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

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

		try (JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2))) {

			// initialize Kafka Consumer
			HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
			HashMap<String, String> kafkaParams = new HashMap<String, String>();
			kafkaParams.put("metadata.broker.list", brokers);
			kafkaParams.put("auto.offset.reset", "smallest");
			// Create direct kafka stream with brokers and topics
			JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
					StringDecoder.class, kafkaParams, topicsSet);

			// Get the lines, split them into words, count the words and print
			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				@Override
				public String call(Tuple2<String, String> tuple2)
				{
					return tuple2._2();
				}
			});

			// Now we have non-empty lines, lets split them into words
			JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public Iterable<String> call(String x)
				{
					return Lists.newArrayList(SPACE.split(x));
				}
			});

			// Convert words to Pairs, remember the TextPair class in Hadoop world
			JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
				@Override
				public Tuple2<String, Integer> call(String s)
				{
					return new Tuple2<String, Integer>(s, 1);
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				@Override
				public Integer call(Integer i1, Integer i2)
				{
					return i1 + i2;
				}
			});

			// Just for debugging, NOT FOR PRODUCTION
			wordCounts.foreach(new Function<JavaPairRDD<String, Integer>, Void>() {
				@Override
				public Void call(JavaPairRDD<String, Integer> t) throws Exception
				{
					List<Tuple2<String, Integer>> list = t.collect();
					for (Tuple2<String, Integer> l : list) {
						LOGGER.info(String.format("%s - %d", l._1(), l._2()));
					}
					return null;
				}
			});

			LOGGER.info("----***#### Starting KafkaWordCount ####***----");
			wordCounts.print();
			// Start the computation
			jssc.start();
			jssc.awaitTermination();
		}
	}

}