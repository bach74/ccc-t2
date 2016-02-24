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
package org.coursera.ccc.q31;

import static java.time.temporal.ChronoUnit.DAYS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
public final class TopScheduleForRoute
{

	private static final String CASSANDRA_TABLE = "route_schedule";

	private static final String CASSANDRA_KEYSPACE = "ccc";

	private static Function<Tuple2<String, String>, String> mapLines = x -> x._2();

	// Return a new DStream by selecting only the records of the source DStream on which func returns true
	private static Function<String, Boolean> filterCsvHeader = x -> {
		return x.contains("Destination") ? false : true;
	};

	private static PairFunction<OnTimeFull, String, OnTimeFull> mapToDestinationKey = x -> {
		String key = x.getDest();
		return new Tuple2<String, OnTimeFull>(key, x);
	};

	private static PairFunction<OnTimeFull, String, OnTimeFull> mapToOriginKey = x -> {
		String key = x.getOrigin();
		return new Tuple2<String, OnTimeFull>(key, x);
	};

	private static Function<Tuple2<String, Tuple2<OnTimeFull, OnTimeFull>>, Boolean> journeyCriteria = x -> {
		OnTimeFull firstLeg = x._2()._1();
		OnTimeFull secondLeg = x._2()._2();

		// (a) The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y).
		// For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
		boolean daysBetween2 = (DAYS.between(secondLeg.getFlightDate(), firstLeg.getFlightDate()) == 2);
		// (b) Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
		boolean depXbefore12 = (firstLeg.getCrsDepTime().getHour() < 12);
		boolean depYafter12 = (secondLeg.getCrsDepTime().getHour() > 12);
		// (c) Tom wants to arrive at each destination with as little delay as possible (assume you know the actual delay of each flight).
		return daysBetween2 && depXbefore12 && depYafter12;
	};

	private static PairFunction<Tuple2<String, Tuple2<OnTimeFull, OnTimeFull>>, String, Tuple2<OnTimeFull, OnTimeFull>> createJourneyKey = x -> {
		String key = x._2()._1().getOrigin() + "-" + x._2()._2().getDest();
		return new Tuple2<>(key, x._2);
	};

	private static Function2<List<Tuple2<OnTimeFull, OnTimeFull>>, Optional<Set<RoutePlan>>, Optional<Set<RoutePlan>>> mergeJourneys = (newState,
			currentState) -> {
			Set<RoutePlan> set = currentState.or(new TreeSet<>());
			for (Tuple2<OnTimeFull, OnTimeFull> r:newState) {
				RoutePlan route = new RoutePlan();
				route.setDate1(r._1().getFlightDate());
				route.setDate2(r._2().getFlightDate());
				route.setDelay1(r._1().getDepDelayMinutes());
				route.setDelay2(r._2().getArrDelayMinutes());
				route.setCarrier1(r._1().getUniqueCarrier());
				route.setCarrier2(r._2().getUniqueCarrier());
				set.add(route);
			}
			return Optional.of(set);
	};

	public static void main(String[] args)
	{
		if (args.length < 3) {
			System.err.println("Usage: TopScheduleForRoute <brokers> <topics> <cassandraIP>\n" + "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];
		String cassandraIP = args[2];

		// Create context with 10 second batch interval
		SparkConf sparkConf = new SparkConf().setAppName("TopScheduleForRoute");
		sparkConf.set("spark.cassandra.connection.host", cassandraIP);

		try (JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10))) {

			prepareCassandraKeyspace(jssc);

			// must set for statefull operations
			jssc.checkpoint(".");

			// initialize Kafka Consumer
			JavaPairInputDStream<String, String> messages = createKafkaConsumerStream(brokers, topics, jssc);

			JavaDStream<String> lines = messages.map(mapLines).filter(filterCsvHeader);

			JavaDStream<OnTimeFull> routeSchedule = lines.map(OnTimeFull::parseOneLine);

			// destination is the key for the first leg
			JavaPairDStream<String, OnTimeFull> firstLeg = routeSchedule.mapToPair(mapToDestinationKey);
			// origin is the key for second leg of the journey
			JavaPairDStream<String, OnTimeFull> secondLeg = routeSchedule.mapToPair(mapToOriginKey);
			// join legs on key and filter based on given criteria
			JavaPairDStream<String, Tuple2<OnTimeFull, OnTimeFull>> journey = firstLeg.join(secondLeg).filter(journeyCriteria).mapToPair(createJourneyKey);

			// This will give a Dstream made of state
			JavaPairDStream<String, Set<RoutePlan>> performance = journey.updateStateByKey(mergeJourneys);

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
			// session.execute("DROP KEYSPACE IF EXISTS " + CASSANDRA_KEYSPACE);
			session.execute("CREATE KEYSPACE if not exists " + CASSANDRA_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
			session.execute("CREATE TABLE if not exists " + CASSANDRA_KEYSPACE + "." + CASSANDRA_TABLE
					+ " (route text, carrier1 text, carrier2 text, date1 text, date2 text, delay float, PRIMARY KEY (route, delay)) WITH CLUSTERING ORDER BY (delay ASC)");
		}
	}
}
