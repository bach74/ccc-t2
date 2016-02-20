package org.coursera.ccc.q21;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;

public class WriteToCassandra implements Function<JavaPairRDD<String, Set<CarrierDelay>>, Void>
{
	private static final String CASSANDRA_TABLE = "origin_airline";

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
	public Void call(JavaPairRDD<String, Set<CarrierDelay>> rdd) throws Exception
	{

		List<Tuple2<String, Set<CarrierDelay>>> topCarriersByDelay = rdd.take(10);
		List<CarrierDelayEntity> carrierDelays = new ArrayList<>();

		for (Tuple2<String, Set<CarrierDelay>> t : topCarriersByDelay) {
			String origin = t._1();
			Set<CarrierDelay> listCarriers = t._2();
			for (CarrierDelay c : listCarriers) {
				// session.execute(boundStatement.bind(origin, c.getUniqueCarrier(), new Float(c.getDepDelayMinutes() / c.getCount())));
				carrierDelays.add(new CarrierDelayEntity(origin, c.getUniqueCarrier(), new Float(c.getDepDelayMinutes() / c.getCount())));
			}
			System.out.println("Top 10 Carriers from " + origin + " :" + listCarriers);
		}

		CassandraJavaUtil.javaFunctions(jssc.sc().parallelize(carrierDelays))
				.writerBuilder(CASSANDRA_KEYSPACE, CASSANDRA_TABLE, CassandraJavaUtil.mapToRow(CarrierDelayEntity.class)).saveToCassandra();
		;

		System.out.println("--------------------------------------------------------------------------------------------");
		System.out.println("Top 10 Carriers: " + topCarriersByDelay);
		System.out.println("--------------------------------------------------------------------------------------------");
		return null;
	}
}
