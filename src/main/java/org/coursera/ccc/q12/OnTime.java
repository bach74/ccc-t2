package org.coursera.ccc.q12;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.primitives.Doubles;

/**
 * This class represents an OnTime CSV format. "Year","Month","DayofMonth","FlightDate","UniqueCarrier","Origin","Dest","DepDelayMinutes","ArrDelayMinutes"
 * 2008,1,3,2008-01-03,"WN","HOU","LIT",18.00,16.00
 */
public class OnTime implements Serializable
{
	private static final Logger LOGGER = Logger.getLogger("Origin_Dest_CSV");

	// Example Apache log line:
	// 127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
	private static final String ONTIME_PATTERN = "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(\\S+)\",\"(\\S+)\",\"(\\S+)\",(\\d+[\\.\\d+]?),(\\d+[\\.\\d+]?)";

	private static final Pattern PATTERN = Pattern.compile(ONTIME_PATTERN);

	private String uniqueCarrier;
	private Double arrDelayMinutes;

	public OnTime(String uniqueCarrier, Double arrDelayMinutes)
	{
		this.uniqueCarrier = uniqueCarrier;
		this.arrDelayMinutes = arrDelayMinutes;
	}

	public static OnTime parseOneLine(String line)
	{
		Matcher m = PATTERN.matcher(line);
		if (!m.find()) {
			LOGGER.log(Level.ALL, "Cannot parse logline" + line);
			throw new RuntimeException("Error parsing logline");
		}

		String uniqueCarrier = m.group(5);
		String arrDelayMinutes = m.group(9);
		Double delay = Doubles.tryParse(arrDelayMinutes);
		if (delay == null) {
			delay = 0.0;
		}
		return new OnTime(uniqueCarrier, delay);
	}

	@Override
	public String toString()
	{
		return String.format("%s %f", uniqueCarrier, arrDelayMinutes);
	}

	public String getUniqueCarrier()
	{
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier)
	{
		uniqueCarrier = this.uniqueCarrier;
	}

	public Double getArrDelayMinutes()
	{
		return arrDelayMinutes;
	}

	public void setArrDelayMinutes(Double arrDelayMinutes)
	{
		this.arrDelayMinutes = arrDelayMinutes;
	}
}