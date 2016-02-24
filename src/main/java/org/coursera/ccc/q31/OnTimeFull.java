package org.coursera.ccc.q31;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Optional;
import com.google.common.primitives.Doubles;

/**
 * This class represents an OnTime CSV format.
 * Year,Month,DayofMonth,DayOfWeek,UniqueCarrier,Origin,Dest,CRSDepTime,DepDelayMinutes,CRSArrTime,ArrDelayMinutes,Cancelled,Diverted
 * 
 * 2008,1,3,4,"WN (1)","HOU","LIT","1325",18.00,"1435",16.00,0.00,0.00
 */
public class OnTimeFull implements Serializable
{

	// private static final String ONTIME_PATTERN =
	// "^(\\d+),(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2}),\"(\\S+)\",\"(\\S+)\",\"(\\S+)\",(\\d+[\\.\\d+]?),(\\d+[\\.\\d+]?)";

	private static final String ONTIME_PATTERN = 
	   "^(\\d+),(\\d+),(\\d+),(\\d+),\"(.+)\",\"(\\S+)\",\"(\\S+)\",\"(\\d+)\",(\\d+\\.?\\d*),\"(\\d+)\",(\\d+\\.?\\d*),(\\d+\\.?\\d*),(\\d+\\.?\\d*)";

	private static final Pattern PATTERN = Pattern.compile(ONTIME_PATTERN);

	private LocalDate flightDate;
	private String uniqueCarrier;
	private String origin;
	private String dest;
	private LocalTime crsDepTime;

	private Integer depDelayMinutes;
	private LocalTime crsArrTime;
	private Integer arrDelayMinutes;

	public OnTimeFull(LocalDate flightDate, String uniqueCarrier, String origin, String dest, LocalTime cRSDepTime, Integer depDelayMinutes,
			LocalTime cRSArrTime, Integer arrDelayMinutes)
	{
		this.setFlightDate(flightDate);
		this.setUniqueCarrier(uniqueCarrier);
		this.setOrigin(origin);
		this.setDest(dest);
		this.setCrsDepTime(cRSDepTime);
		this.setDepDelayMinutes(depDelayMinutes);
		this.setCrsArrTime(cRSArrTime);
		this.setArrDelayMinutes(arrDelayMinutes);
	}

	private OnTimeFull()
	{
		this.setFlightDate(LocalDate.of(1980, 1, 1));
		this.setUniqueCarrier("#N.A.");
		this.setOrigin("#N.A.");
		this.setDest("#N.A.");
		this.setCrsDepTime(LocalTime.of(0, 0));
		this.setDepDelayMinutes(0);
		this.setCrsArrTime(LocalTime.of(0, 0));
		this.setArrDelayMinutes(0);

	}

	public static OnTimeFull parseOneLine(String line)
	{
		Matcher m = PATTERN.matcher(line);
		if (!m.find()) {
			// LOGGER.log(Level.SEVERE, "Cannot parse on_time line" + line);
			// throw new RuntimeException("Error parsing on_time line" + line);
			return new OnTimeFull();
		}

		Integer year = Integer.parseInt(m.group(1));
		Integer month = Integer.parseInt(m.group(2));
		Integer dom = Integer.parseInt(m.group(3));
		//Integer dow = Integer.parseInt(m.group(4));
		String carrier = m.group(5);
		String origin = m.group(6);
		String destination = m.group(7);
		LocalTime crsDepTime = stringToLocalTime(m.group(8));
		String depDelayMinutes = m.group(9);
		Integer delayDeparture = Optional.fromNullable(Doubles.tryParse(depDelayMinutes).intValue()).or(0);
		LocalTime crsArrTime = stringToLocalTime(m.group(10));
		String arrivalDelayMinutes = m.group(11);
		Integer delayArrival = Optional.fromNullable(Doubles.tryParse(arrivalDelayMinutes).intValue()).or(0);
		LocalDate flightDate = LocalDate.of(year, month, dom);

		return new OnTimeFull(flightDate, carrier, origin, destination, crsDepTime, delayDeparture, crsArrTime, delayArrival);
	}

	protected static LocalTime stringToLocalTime(String time)
	{
		if ((time != null) && (time.length() == 4)) {
			int hour = Integer.parseInt(time.substring(0, 2));
			int min = Integer.parseInt(time.substring(2));
			return LocalTime.of(hour, min);
		}
		return null;
	}

	@Override
	public String toString()
	{
		return "OnTimeFull [flightDate=" + getFlightDate() + ", UniqueCarrier=" + getUniqueCarrier() + ", Origin=" + getOrigin() + ", Dest=" + getDest() + ", CRSDepTime="
				+ getCrsDepTime() + ", DepDelayMinutes=" + getDepDelayMinutes() + ", CRSArrTime=" + getCrsArrTime() + ", ArrDelayMinutes=" + getArrDelayMinutes() + "]";
	}

	public LocalDate getFlightDate()
	{
		return flightDate;
	}

	public void setFlightDate(LocalDate flightDate)
	{
		this.flightDate = flightDate;
	}

	public String getUniqueCarrier()
	{
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier)
	{
		this.uniqueCarrier = uniqueCarrier;
	}

	public String getOrigin()
	{
		return origin;
	}

	public void setOrigin(String origin)
	{
		this.origin = origin;
	}

	public String getDest()
	{
		return dest;
	}

	public void setDest(String dest)
	{
		this.dest = dest;
	}

	public LocalTime getCrsDepTime()
	{
		return crsDepTime;
	}

	public void setCrsDepTime(LocalTime crsDepTime)
	{
		this.crsDepTime = crsDepTime;
	}

	public Integer getDepDelayMinutes()
	{
		return depDelayMinutes;
	}

	public void setDepDelayMinutes(Integer depDelayMinutes)
	{
		this.depDelayMinutes = depDelayMinutes;
	}

	public LocalTime getCrsArrTime()
	{
		return crsArrTime;
	}

	public void setCrsArrTime(LocalTime crsArrTime)
	{
		this.crsArrTime = crsArrTime;
	}

	public Integer getArrDelayMinutes()
	{
		return arrDelayMinutes;
	}

	public void setArrDelayMinutes(Integer arrDelayMinutes)
	{
		this.arrDelayMinutes = arrDelayMinutes;
	}

}