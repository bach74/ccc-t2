package org.coursera.ccc.q31;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;

public class WriteToCassandra implements Function<JavaPairRDD<String, Set<RoutePlan>>, Void>
{
	private static final String CASSANDRA_TABLE = "route_schedule";

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
	public Void call(JavaPairRDD<String, Set<RoutePlan>> rdd) throws Exception
	{

		List<Tuple2<String, Set<RoutePlan>>> journeyList = rdd.collect();
		List<RoutePlanEntity> arrivalDelays = new ArrayList<>();

		for (Tuple2<String, Set<RoutePlan>> t : journeyList) {
			String route = t._1();
			Set<RoutePlan> listDestinations = t._2();
			for (RoutePlan c : listDestinations) {
				arrivalDelays.add(new RoutePlanEntity(route, c.getCarrier1(), c.getCarrier2(), c.getDate1().toString(), c.getDate2().toString(), c.getDelay1()+c.getDelay2()));
			}
		}

		CassandraJavaUtil.javaFunctions(jssc.sc().parallelize(arrivalDelays))
				.writerBuilder(CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CassandraJavaUtil.mapToRow(RoutePlanEntity.class)).saveToCassandra();
		

		return null;
	}
}
