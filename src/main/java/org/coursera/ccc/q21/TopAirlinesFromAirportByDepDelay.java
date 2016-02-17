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
package org.coursera.ccc.q21;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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
public final class TopAirlinesFromAirportByDepDelay
{

	private static PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, CarrierDelay> splitOriginCarrier = s -> {
		String[] split = s._1().split("-");
		if (split.length > 1) {
			String origin = split[0];
			String uniqueCarrier = split[1];
			Double depDelayMinutesSum = s._2()._1();
			Integer depDelayMinutesCount = s._2()._2();
			return new Tuple2<>(origin, new CarrierDelay(uniqueCarrier, depDelayMinutesSum, depDelayMinutesCount));
		}
		return new Tuple2<>("#N.A", new CarrierDelay("#N.A", 99999.99, 1));

	};

	private static Function<Double, Tuple2<Double, Integer>> createAcc = x -> new Tuple2<Double, Integer>(x, 1);

	private static Function2<Tuple2<Double, Integer>, Double, Tuple2<Double, Integer>> addAndCount = (Tuple2<Double, Integer> x, Double y) -> {
		return new Tuple2<Double, Integer>(x._1() + y, x._2() + 1);
	};

	private static Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>> combine = (Tuple2<Double, Integer> x,
			Tuple2<Double, Integer> y) -> {
		return new Tuple2<Double, Integer>(x._1() + y._1(), x._2() + y._2());
	};

	private static class AverageComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable
	{
		private Comparator<V> comparator;

		public AverageComparator(Comparator<V> comparator)
		{
			this.comparator = comparator;
		}

		@Override
		public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2)
		{
			return comparator.compare(o1._2(), o2._2());
		}
	}

	private static Function2<List<CarrierDelay>, Optional<Set<CarrierDelay>>, Optional<Set<CarrierDelay>>> mergeOrigins = (newRecords, currentRecords) -> {
		Set<CarrierDelay> agg = currentRecords.or(new TreeSet<>());

		return Optional.of(agg.stream().limit(10).collect(Collectors.toSet()));
	};

	private static Function<Tuple2<String, String>, String> mapLines = x -> x._2();

	// Return a new DStream by selecting only the records of the source DStream on which func returns true
	private static Function<String, Boolean> filterCsvHeader = x -> {
		return x.contains("UniqueCarrier") ? false : true;
	};

	public static void main(String[] args)
	{
		if (args.length < 2) {
			System.err.println("Usage: TopAirlinesFromAirportByDepDelay <brokers> <topics>\n" + "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];

		// Create context with 2 second batch interval
		SparkConf sparkConf = new SparkConf().setAppName("TopAirlinesFromAirportByDepDelay");

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

			JavaDStream<String> lines = messages.map(mapLines).filter(filterCsvHeader);

			JavaDStream<OnTime> airlinePerformance = lines.map(OnTime::parseOneLine);

			// This will give a Dstream made of state (which is the cumulative count of the words)
			JavaPairDStream<String, Set<CarrierDelay>> performance = airlinePerformance
					.mapToPair(s -> new Tuple2<>(s.getOrigin() + "-" + s.getUniqueCarrier(), s.getDepDelayMinutes()))
					.combineByKey(createAcc, addAndCount, combine, new HashPartitioner(jssc.sc().defaultParallelism()))
					.mapToPair(splitOriginCarrier)
					.updateStateByKey(mergeOrigins);

			performance.print();

			/*
			 * performance.foreachRDD(rdd -> { List<Tuple2<String, CountAndSum>> topCarriersByArrivalPerformance = rdd.filter(FILTER_NA).takeOrdered(10, new
			 * AverageComparator<>(Comparator.<CountAndSum> naturalOrder()));
			 * System.out.println("--------------------------------------------------------------------------------------------"); System.out.println(
			 * "Top 10 Carriers by arrival Performance: " + topCarriersByArrivalPerformance);
			 * System.out.println("--------------------------------------------------------------------------------------------"); return null; });
			 */
			// Start the computation
			jssc.start();
			jssc.awaitTermination();
		}
	}

}