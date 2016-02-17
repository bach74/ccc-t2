package org.coursera.ccc.q21;

import java.io.Serializable;

public class CarrierDelay implements Serializable, Comparable<CarrierDelay>
{

	private String uniqueCarrier;

	private Double depDelayMinutes;

	private Integer count;

	public CarrierDelay(String uniqueCarrier, Double depDelayMinutes, Integer count)
	{
		this.uniqueCarrier = uniqueCarrier;
		this.depDelayMinutes = depDelayMinutes;
		this.count = count;
	}

	public String getUniqueCarrier()
	{
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier)
	{
		this.uniqueCarrier = uniqueCarrier;
	}

	public Double getDepDelayMinutes()
	{
		return depDelayMinutes;
	}

	public void setDepDelayMinutes(Double depDelayMinutes)
	{
		this.depDelayMinutes = depDelayMinutes;
	}

	@Override
	public String toString()
	{
		return "CarrierDelay [uniqueCarrier=" + uniqueCarrier + ", avgDepDelayMinutes=" + depDelayMinutes / count + "]";
	}

	@Override
	public int compareTo(CarrierDelay o)
	{
		double avgDelay1 = this.depDelayMinutes / this.count;
		double avgDelay2 = o.getDepDelayMinutes() / o.getCount();
		
		if (avgDelay1 > avgDelay2) {
			return 1;
		}
		if (avgDelay1 < avgDelay2) {
			return -1;
		}
		return 0;
	}

	public Integer getCount()
	{
		return count;
	}

	public void setCount(Integer count)
	{
		this.count = count;
	}

}
