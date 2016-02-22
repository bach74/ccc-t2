package org.coursera.ccc.q23;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;

public class WriteToCassandra implements Function<JavaPairRDD<String, Set<RouteDelay>>, Void>
{
	private static final String CASSANDRA_TABLE = "top_carriers_route";

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
	public Void call(JavaPairRDD<String, Set<RouteDelay>> rdd) throws Exception
	{

		List<Tuple2<String, Set<RouteDelay>>> topCarrierByRoute = rdd.collect();
		List<RouteDelayEntity> arrivalDelays = new ArrayList<>();

		for (Tuple2<String, Set<RouteDelay>> t : topCarrierByRoute) {
			String route = t._1();
			Set<RouteDelay> listDestinations = t._2();
			for (RouteDelay c : listDestinations) {
				arrivalDelays.add(new RouteDelayEntity(route, c.getCarrier(), new Float(c.getArrDelayMinutes() / c.getCount())));
			}
		}

		CassandraJavaUtil.javaFunctions(jssc.sc().parallelize(arrivalDelays))
				.writerBuilder(CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CassandraJavaUtil.mapToRow(RouteDelayEntity.class)).saveToCassandra();
		;

		return null;
	}
}
