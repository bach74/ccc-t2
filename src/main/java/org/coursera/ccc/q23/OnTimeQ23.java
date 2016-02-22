package org.coursera.ccc.q23;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.primitives.Doubles;

/**
 * This class represents an OnTime CSV format.
 * "Year","Month","DayofMonth","FlightDate","UniqueCarrier","Origin","Dest","arrivalDelayMinutes","arrivalDelayMinutes"
 * 2008,1,3,2008-01-03,"WN","HOU","LIT",18.00,16.00
 */
public class OnTimeQ23 implements Serializable
{

	// private static final String ONTIME_PATTERN =
	// "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(\\S+)\",\"(\\S+)\",\"(\\S+)\",(\\d+[\\.\\d+]?),(\\d+[\\.\\d+]?)";

	private static final String ONTIME_PATTERN = "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(.+)\",\"(\\S+)\",\"(\\S+)\",(\\d+\\.?\\d*),(\\d+\\.?\\d*)";

	private static final Pattern PATTERN = Pattern.compile(ONTIME_PATTERN);

	private Double arrivalDelayMinutes;

	private String route;

	private String carrier;

	public OnTimeQ23(String route, Double arrivalDelayMinutes, String carrier)
	{
		this.route = route;
		this.arrivalDelayMinutes = arrivalDelayMinutes;
		this.setCarrier(carrier);
	}

	public static OnTimeQ23 parseOneLine(String line)
	{
		Matcher m = PATTERN.matcher(line);
		if (!m.find()) {
			// LOGGER.log(Level.SEVERE, "Cannot parse on_time line" + line);
			// throw new RuntimeException("Error parsing on_time line" + line);
			return new OnTimeQ23("#N.A.", 99999999.0, "#N.A.");
		}

		String carrier = m.group(5);
		String origin = m.group(6);
		String destination = m.group(7);
		String arrivalDelayMinutes = m.group(9);
		Double delay = Doubles.tryParse(arrivalDelayMinutes);
		if (delay == null) {
			delay = 0.0;
		}
		String route = origin + ":" + destination;

		return new OnTimeQ23(route, delay, carrier);
	}

	@Override
	public String toString()
	{
		return String.format("%s %s %f", route, carrier, arrivalDelayMinutes);
	}

	public String getRoute()
	{
		return route;
	}

	public void setRoute(String route)
	{
		this.route = route;
	}

	public Double getArrivalDelayMinutes()
	{
		return arrivalDelayMinutes;
	}

	public void setArrivalDelayMinutes(Double arrivalDelayMinutes)
	{
		this.arrivalDelayMinutes = arrivalDelayMinutes;
	}

	public String getKey()
	{
		return this.route + "-" + this.carrier;
	}

	public String getCarrier()
	{
		return carrier;
	}

	public void setCarrier(String carrier)
	{
		this.carrier = carrier;
	}

}