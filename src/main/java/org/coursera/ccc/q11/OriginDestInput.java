package org.coursera.ccc.q11;

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

	private static final String LINE_PATTERN =
	// "1:ORIGIN","2:DESTINATION"
	"^\"(\\S+)\",\"(\\S+)\"";

	private static final Pattern PATTERN = Pattern.compile(LINE_PATTERN);

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

	public static OriginDestInput parseOneLine(String line)
	{
		Matcher m = PATTERN.matcher(line);
		if (!m.find()) {
			LOGGER.log(Level.ALL, "Cannot parse logline" + line);
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