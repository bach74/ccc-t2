package org.coursera.ccc;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents an Origin Destination CSV format.
 */
public class OriginDestInput implements Serializable
{
	private static final Logger LOGGER = Logger.getLogger("Origin_Dest_CSV");

	private String origin;
	private String destination;

	private OriginDestInput(String origin, String destination)
	{
		this.origin = origin;
		this.destination = destination;
	}

	public String getOrigin()
	{
		return origin;
	}

	public String getDestination()
	{
		return destination;
	}

	public void setOrigin(String origin)
	{
		this.origin = origin;
	}

	public void setDestination(String destination)
	{
		this.destination = destination;
	}

	// Example Apache log line:
	// 127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
	private static final String LOG_ENTRY_PATTERN =
	// "1:ORIGIN","2:DESTINATION"
	"^\"(\\S+)\",\"(\\S+)\"";
	// 1:IP 2:client 3:user 4:date time 5:method 6:req 7:proto 8:respcode 9:size
	// "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";

	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	public static OriginDestInput parseFromLogLine(String logline)
	{
		Matcher m = PATTERN.matcher(logline);
		if (!m.find()) {
			LOGGER.log(Level.ALL, "Cannot parse logline" + logline);
			throw new RuntimeException("Error parsing logline");
		}

		return new OriginDestInput(m.group(1), m.group(2));
	}

	@Override
	public String toString()
	{
		return String.format("%s %s", origin, destination);
	}
}