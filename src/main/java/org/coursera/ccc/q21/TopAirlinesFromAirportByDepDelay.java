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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
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

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
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

	private static final AtomicLong runningCount = new AtomicLong(0);
	
	private static final String CASSANDRA_TABLE = "origin_airline";

	private static final String CASSANDRA_KEYSPACE = "ccc";

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

	private static Function2<List<CarrierDelay>, Optional<Set<CarrierDelay>>, Optional<Set<CarrierDelay>>> mergeOrigins = (newRecords, currentRecords) -> {
		Set<CarrierDelay> agg = currentRecords.or(new TreeSet<>());
		agg.addAll(newRecords);
		runningCount.addAndGet(newRecords.size());
		System.out.println("  # Processed = " + runningCount);
		//return Optional.of(new TreeSet<>(agg.stream().limit(10).collect(Collectors.toSet())));
		return Optional.of(agg);
	};

	private static Function<Tuple2<String, String>, String> mapLines = x -> x._2();

	// Return a new DStream by selecting only the records of the source DStream on which func returns true
	private static Function<String, Boolean> filterCsvHeader = x -> {
		return x.contains("UniqueCarrier") ? false : true;
	};

	private static Function<Tuple2<String, Double>, Boolean> filterNA = x -> {
		return x._1().contains("#N.A.") ? false : true;
	};

	public static void main(String[] args)
	{
		if (args.length < 3) {
			System.err.println("Usage: TopAirlinesFromAirportByDepDelay <brokers> <topics> <cassandraIP>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n" + "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];
		String cassandraIP = args[2];

		// Create context with 10 second batch interval
		SparkConf sparkConf = new SparkConf().setAppName("TopAirlinesFromAirportByDepDelay");
		sparkConf.set("spark.cassandra.connection.host", cassandraIP);

		try (JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10))) {

			prepareCassandraKeyspace(jssc);

			// must set for statefull operations
			jssc.checkpoint(".");

			// initialize Kafka Consumer
			JavaPairInputDStream<String, String> messages = createKafkaConsumerStream(brokers, topics, jssc);

			JavaDStream<String> lines = messages.map(mapLines).filter(filterCsvHeader);

			JavaDStream<OnTime> airlinePerformance = lines.map(OnTime::parseOneLine);

			// This will give a Dstream made of state (which is the cumulative count of the words)
			JavaPairDStream<String, Set<CarrierDelay>> performance = airlinePerformance
					.mapToPair(s -> new Tuple2<>(s.getOrigin() + "-" + s.getUniqueCarrier(), s.getDepDelayMinutes())).filter(filterNA)
					.combineByKey(createAcc, addAndCount, combine, new HashPartitioner(jssc.sc().defaultParallelism())).mapToPair(splitOriginCarrier)
					.updateStateByKey(mergeOrigins);

			// performance.print();

			performance.foreachRDD(new WriteToCassandra(jssc));

			// Start the computation
			jssc.start();
			jssc.awaitTermination();
			
			System.out.println("------------THE END-------------------------------------------");
			
		}
	}

	private static JavaPairInputDStream<String, String> createKafkaConsumerStream(String brokers, String topics, JavaStreamingContext jssc)
	{
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		// start from begin
		kafkaParams.put("auto.offset.reset", "smallest");
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
				StringDecoder.class, kafkaParams, topicsSet);
		return messages;
	}

	private static void prepareCassandraKeyspace(JavaStreamingContext jssc)
	{
		CassandraConnector connector = CassandraConnector.apply(jssc.sc().getConf());
		try (Session session = connector.openSession()) {
			//session.execute("DROP KEYSPACE IF EXISTS " + CASSANDRA_KEYSPACE);
			session.execute("CREATE KEYSPACE if not exists " + CASSANDRA_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE if not exists " + CASSANDRA_KEYSPACE + "." + CASSANDRA_TABLE
					+ " (origin text, carrier text, avg_delay float, PRIMARY KEY (origin, avg_delay, carrier)) WITH CLUSTERING ORDER BY (avg_delay ASC, carrier ASC)");
		}
	}
}
