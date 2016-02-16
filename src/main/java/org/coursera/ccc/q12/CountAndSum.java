package org.coursera.ccc.q12;

import java.io.Serializable;

public class CountAndSum implements Comparable<CountAndSum>, Serializable
{
	private Double sum;
	private Integer count;

	public CountAndSum(Double sum, Integer count)
	{
		super();
		this.sum = sum;
		this.count = count;
	}

	public Double getSum()
	{
		return sum;
	}

	public Integer getCount()
	{
		return count;
	}

	public void add(final Double sum, final Integer count)
	{
		this.sum += sum;
		this.count += count;
	}

	@Override
	public int compareTo(CountAndSum o)
	{
		if ((this.sum / this.count) > (o.getSum() / o.getCount())) {
			return 1;
		} else if ((this.sum / this.count) < (o.getSum() / o.getCount())) {
			return -1;
		}
		return 0;
	}

	@Override
	public String toString()
	{
		return String.format("Avg: %.2f, Sum: %.2f, Size: %d", this.sum / this.count, this.sum, this.count);
	}
}