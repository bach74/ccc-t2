package org.coursera.ccc.q22;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;

public class WriteToCassandra implements Function<JavaPairRDD<String, Set<AirportDelay>>, Void>
{
	private static final String CASSANDRA_TABLE = "origin_destination";

	private static final String CASSANDRA_KEYSPACE = "ccc";

	transient private JavaStreamingContext jssc;

	public WriteToCassandra()
	{
	}

	WriteToCassandra(JavaStreamingContext jssc)
	{
		this.jssc = jssc;
	}

	@Override
	public Void call(JavaPairRDD<String, Set<AirportDelay>> rdd) throws Exception
	{

		List<Tuple2<String, Set<AirportDelay>>> topDestinationsByDelay = rdd.collect();// take(10);
		List<AirportDelayEntity> destinationsDelays = new ArrayList<>();

		for (Tuple2<String, Set<AirportDelay>> t : topDestinationsByDelay) {
			String origin = t._1();
			Set<AirportDelay> listDestinations = t._2();
			for (AirportDelay c : listDestinations) {
				destinationsDelays.add(new AirportDelayEntity(origin, c.getDestAirport(), new Float(c.getDepDelayMinutes() / c.getCount())));
			}
		}

		CassandraJavaUtil.javaFunctions(jssc.sc().parallelize(destinationsDelays))
				.writerBuilder(CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CassandraJavaUtil.mapToRow(AirportDelayEntity.class)).saveToCassandra();
		;

		return null;
	}
}
