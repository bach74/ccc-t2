package org.coursera.ccc.q22;

import java.io.Serializable;

public class AirportDelayEntity implements Serializable
{
	private String origin;
	private String destination;
	private Float avgDelay;

	public AirportDelayEntity()
	{
	}

	public AirportDelayEntity(String origin, String destination, Float avgDelay)
	{
		this.setOrigin(origin);
		this.setDestination(destination);
		this.setAvgDelay(avgDelay);
	}

	public String getOrigin()
	{
		return origin;
	}

	public void setOrigin(String origin)
	{
		this.origin = origin;
	}

	public String getDestination()
	{
		return destination;
	}

	public void setDestination(String destination)
	{
		this.destination = destination;
	}

	public Float getAvgDelay()
	{
		return avgDelay;
	}

	public void setAvgDelay(Float avgDelay)
	{
		this.avgDelay = avgDelay;
	}

}
