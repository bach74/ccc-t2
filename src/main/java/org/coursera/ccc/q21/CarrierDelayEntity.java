package org.coursera.ccc.q21;

import java.io.Serializable;

public class CarrierDelayEntity implements Serializable
{
	private String origin;
	private String carrier;
	private float avgDelay;

	public CarrierDelayEntity()
	{
	}

	public CarrierDelayEntity(String origin, String carrier, float avgDelay)
	{
		this.setOrigin(origin);
		this.setCarrier(carrier);
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

	public String getCarrier()
	{
		return carrier;
	}

	public void setCarrier(String carrier)
	{
		this.carrier = carrier;
	}

	public float getAvgDelay()
	{
		return avgDelay;
	}

	public void setAvgDelay(float avgDelay)
	{
		this.avgDelay = avgDelay;
	}

}
