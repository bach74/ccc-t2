package org.coursera.ccc.q23;

import java.io.Serializable;

public class RouteDelayEntity implements Serializable
{
	private String route;
	private String carrier;
	private Float avgDelay;

	public RouteDelayEntity()
	{
	}

	public RouteDelayEntity(String route, String carrier, Float avgDelay)
	{
		this.setRoute(route);
		this.setCarrier(carrier);
		this.setAvgDelay(avgDelay);
	}

	public String getRoute()
	{
		return route;
	}

	public void setRoute(String route)
	{
		this.route = route;
	}

	public String getCarrier()
	{
		return carrier;
	}

	public void setCarrier(String carrier)
	{
		this.carrier = carrier;
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
