package org.coursera.ccc.q21;

import java.io.Serializable;

public class CarrierDelay implements Serializable
{

	private String uniqueCarrier;

	private Double depDelayMinutes;

	public CarrierDelay(String uniqueCarrier, Double depDelayMinutes)
	{
		this.setUniqueCarrier(uniqueCarrier);
		this.setDepDelayMinutes(depDelayMinutes);
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

}
