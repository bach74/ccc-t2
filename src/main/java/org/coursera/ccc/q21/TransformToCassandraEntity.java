package org.coursera.ccc.q21;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class TransformToCassandraEntity implements Function<JavaPairRDD<String, Set<CarrierDelay>>, JavaRDD<CarrierDelayEntity>>
{

	private transient JavaStreamingContext jssc;

	public TransformToCassandraEntity(JavaStreamingContext jssc)
	{
		this.jssc = jssc;
	}

	@Override
	public JavaRDD<CarrierDelayEntity> call(JavaPairRDD<String, Set<CarrierDelay>> e) throws Exception
	{
		List<CarrierDelayEntity> carrierDelays = new ArrayList<>();
		e.foreach(t -> {
			String origin = t._1();
			Set<CarrierDelay> listCarriers = t._2();
			for (CarrierDelay c : listCarriers) {
				carrierDelays.add(new CarrierDelayEntity(origin, c.getUniqueCarrier(), new Float(c.getDepDelayMinutes() / c.getCount())));
			}
		});
		return jssc.sc().parallelize(carrierDelays);
	}

}
