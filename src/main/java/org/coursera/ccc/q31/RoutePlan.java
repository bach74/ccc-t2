package org.coursera.ccc.q31;

import java.io.Serializable;
import java.time.LocalDate;

public class RoutePlan implements Serializable, Comparable<RoutePlan>
{

	private String carrier1;
	private String carrier2;
	private LocalDate date1;
	private LocalDate date2;
	private Integer delay1;
	private Integer delay2;

	@Override
	public int compareTo(RoutePlan o)
	{
		return ((this.delay1 + this.delay2) > (o.getDelay1() + o.getDelay2())) ? 1 : -1;
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

	public LocalDate getDate1()
	{
		return date1;
	}

	public void setDate1(LocalDate date1)
	{
		this.date1 = date1;
	}

	public LocalDate getDate2()
	{
		return date2;
	}

	public void setDate2(LocalDate date2)
	{
		this.date2 = date2;
	}

	public Integer getDelay1()
	{
		return delay1;
	}

	public void setDelay1(Integer delay1)
	{
		this.delay1 = delay1;
	}

	public Integer getDelay2()
	{
		return delay2;
	}

	public void setDelay2(Integer delay2)
	{
		this.delay2 = delay2;
	}

}
