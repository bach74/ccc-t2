package org.coursera.ccc.q22;

import java.io.Serializable;

public class AirportDelay implements Serializable, Comparable<AirportDelay>
{

	private String destAirport;

	private Double depDelayMinutes;

	private Integer count;

	public AirportDelay(String destAirport, Double depDelayMinutes, Integer count)
	{
		this.destAirport = destAirport;
		this.depDelayMinutes = depDelayMinutes;
		this.count = count;
	}

	public String getDestAirport()
	{
		return destAirport;
	}

	public void setDestAirport(String uniqueCarrier)
	{
		this.destAirport = uniqueCarrier;
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
		return "AirportDelay [destAirport=" + destAirport + ", avgDepDelayMinutes=" + depDelayMinutes / count + "]";
	}

	@Override
	public int compareTo(AirportDelay o)
	{
		double avgDelay1 = this.depDelayMinutes / this.count;
		double avgDelay2 = o.getDepDelayMinutes() / o.getCount();

		if (avgDelay1 >= avgDelay2) {
			return 1;
		}
		return -1;
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
