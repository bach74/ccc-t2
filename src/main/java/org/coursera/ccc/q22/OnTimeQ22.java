package org.coursera.ccc.q22;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.primitives.Doubles;

/**
 * This class represents an OnTime CSV format. "Year","Month","DayofMonth","FlightDate","UniqueCarrier","Origin","Dest","DepDelayMinutes","DepDelayMinutes"
 * 2008,1,3,2008-01-03,"WN","HOU","LIT",18.00,16.00
 */
public class OnTimeQ22 implements Serializable
{

	// private static final String ONTIME_PATTERN =
	// "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(\\S+)\",\"(\\S+)\",\"(\\S+)\",(\\d+[\\.\\d+]?),(\\d+[\\.\\d+]?)";

	private static final String ONTIME_PATTERN = "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(.+)\",\"(\\S+)\",\"(\\S+)\",(\\d+\\.?\\d*),(\\d+\\.?\\d*)";

	private static final Pattern PATTERN = Pattern.compile(ONTIME_PATTERN);

	private String destination;

	private Double depDelayMinutes;

	private String origin;

	public OnTimeQ22(String destination, Double depDelayMinutes, String origin)
	{
		this.destination = destination;
		this.depDelayMinutes = depDelayMinutes;
		this.origin = origin;
	}

	public static OnTimeQ22 parseOneLine(String line)
	{
		Matcher m = PATTERN.matcher(line);
		if (!m.find()) {
			// LOGGER.log(Level.SEVERE, "Cannot parse on_time line" + line);
			// throw new RuntimeException("Error parsing on_time line" + line);
			return new OnTimeQ22("#N.A.", 99999999.0, "#N.A.");
		}

		String origin = m.group(6);
		String destination = m.group(7);
		String depDelayMinutes = m.group(8);
		Double delay = Doubles.tryParse(depDelayMinutes);
		if (delay == null) {
			delay = 0.0;
		}

		return new OnTimeQ22(destination, delay, origin);
	}

	@Override
	public String toString()
	{
		return String.format("%s %s %f", origin, destination, depDelayMinutes);
	}

	public String getDestination()
	{
		return destination;
	}

	public void setDestination(String destination)
	{
		destination = this.destination;
	}

	public Double getDepDelayMinutes()
	{
		return depDelayMinutes;
	}

	public void setDepDelayMinutes(Double depDelayMinutes)
	{
		this.depDelayMinutes = depDelayMinutes;
	}

	public String getOrigin()
	{
		return origin;
	}

	public void setOrigin(String origin)
	{
		this.origin = origin;
	}

	public String getKey()
	{
		return this.origin + "-" + this.destination;
	}

}