package org.coursera.ccc.q23;

import java.io.Serializable;

public class RouteDelay implements Serializable, Comparable<RouteDelay>
{

	private String carrier;

	private Double arrDelayMinutes;

	private Integer count;

	public RouteDelay(String carrier, Double arrDelayMinutes, Integer count)
	{
		this.carrier = carrier;
		this.arrDelayMinutes = arrDelayMinutes;
		this.count = count;
	}

	public String getCarrier()
	{
		return carrier;
	}

	public void setCarrier(String uniqueCarrier)
	{
		this.carrier = uniqueCarrier;
	}

	public Double getArrDelayMinutes()
	{
		return arrDelayMinutes;
	}

	public void setArrDelayMinutes(Double arrDelayMinutes)
	{
		this.arrDelayMinutes = arrDelayMinutes;
	}

	@Override
	public String toString()
	{
		return "RouteDelay [carrier=" + carrier + ", avgarrDelayMinutes=" + arrDelayMinutes / count + "]";
	}

	@Override
	public int compareTo(RouteDelay o)
	{
		double avgDelay1 = this.arrDelayMinutes / this.count;
		double avgDelay2 = o.getArrDelayMinutes() / o.getCount();

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
