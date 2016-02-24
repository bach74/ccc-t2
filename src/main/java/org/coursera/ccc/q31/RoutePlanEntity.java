package org.coursera.ccc.q31;

import java.io.Serializable;

public class RoutePlanEntity implements Serializable
{
	public RoutePlanEntity(String route, String carrier1, String carrier2, String date1, String date2, Integer delay)
	{
		this.route = route;
		this.carrier1 = carrier1;
		this.carrier2 = carrier2;
		this.date1 = date1;
		this.date2 = date2;
		this.delay = delay;
	}

	private String route;
	private String carrier1;
	private String carrier2;
	private String date1;
	private String date2;
	private Integer delay;

	public RoutePlanEntity()
	{
	}

	public String getRoute()
	{
		return route;
	}

	public void setRoute(String route)
	{
		this.route = route;
	}

	public String getCarrier1()
	{
		return carrier1;
	}

	public void setCarrier1(String carrier1)
	{
		this.carrier1 = carrier1;
	}

	public String getCarrier2()
	{
		return carrier2;
	}

	public void setCarrier2(String carrier2)
	{
		this.carrier2 = carrier2;
	}

	public String getDate1()
	{
		return date1;
	}

	public void setDate1(String date1)
	{
		this.date1 = date1;
	}

	public String getDate2()
	{
		return date2;
	}

	public void setDate2(String date2)
	{
		this.date2 = date2;
	}

	public Integer getDelay()
	{
		return delay;
	}

	public void setDelay(Integer delay)
	{
		this.delay = delay;
	}

}
