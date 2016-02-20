package org.coursera.ccc.q21;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.primitives.Doubles;

/**
 * This class represents an OnTime CSV format. "Year","Month","DayofMonth","FlightDate","UniqueCarrier","Origin","Dest","DepDelayMinutes","DepDelayMinutes"
 * 2008,1,3,2008-01-03,"WN","HOU","LIT",18.00,16.00
 */
public class OnTime implements Serializable
{
	// private static final Logger LOGGER = Logger.getLogger("Origin_Dest_CSV");

	// private static final String ONTIME_PATTERN =
	// "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(\\S+)\",\"(\\S+)\",\"(\\S+)\",(\\d+[\\.\\d+]?),(\\d+[\\.\\d+]?)";

	private static final String ONTIME_PATTERN = "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(\\S+)\",\"(\\S+)\",\"(\\S+)\",(\\d+\\.?\\d*),(\\d+\\.?\\d*)";

	private static final Pattern PATTERN = Pattern.compile(ONTIME_PATTERN);

	private String uniqueCarrier;

	private Double depDelayMinutes;

	private String origin;

	public OnTime(String uniqueCarrier, Double depDelayMinutes, String origin)
	{
		this.uniqueCarrier = uniqueCarrier;
		this.depDelayMinutes = depDelayMinutes;
		this.setOrigin(origin);
	}

	public static OnTime parseOneLine(String line)
	{
		Matcher m = PATTERN.matcher(line);
		if (!m.find()) {
			// LOGGER.log(Level.SEVERE, "Cannot parse on_time line" + line);
			// throw new RuntimeException("Error parsing on_time line" + line);
			return new OnTime("#N.A.", 99999999.0, "#N.A.");
		}

		String origin = m.group(6);
		String uniqueCarrier = m.group(5);
		String depDelayMinutes = m.group(8);
		Double delay = Doubles.tryParse(depDelayMinutes);
		if (delay == null) {
			delay = 0.0;
		}
		return new OnTime(uniqueCarrier, delay, origin);
	}

	@Override
	public String toString()
	{
		return String.format("%s %s %f", getOrigin(), uniqueCarrier, depDelayMinutes);
	}

	public String getUniqueCarrier()
	{
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier)
	{
		uniqueCarrier = this.uniqueCarrier;
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
}