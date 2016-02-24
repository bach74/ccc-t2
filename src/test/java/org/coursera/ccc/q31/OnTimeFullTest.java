package org.coursera.ccc.q31;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.time.LocalDate;
import java.time.LocalTime;

import org.junit.Test;

import com.google.common.base.Optional;

public class OnTimeFullTest
{

	@Test
	public void testFormatter3Chars()
	{
		LocalTime Duration = OnTimeFull.stringToLocalTime("0845");
		assertThat(Duration, is(LocalTime.of(8, 45)));
	}

	@Test
	public void testFormatter4Chars()
	{
		LocalTime Duration = OnTimeFull.stringToLocalTime("1235");
		assertThat(Duration, is(LocalTime.of(12, 35)));
	}

	@Test
	public void testOptional()
	{
		Integer ref = null;
		Integer i = Optional.fromNullable(ref).or(1);
		assertThat(i, is(1));

		ref = 12;
		i = Optional.fromNullable(ref).or(1);
		assertThat(i, is(12));
	}

	@Test
	public void testParse()
	{
		String line = "2008,1,3,4,\"WN (1)\",\"HOU\",\"LIT\",\"1325\",18.00,\"1435\",16.00,0.00,0.00";
		OnTimeFull result = OnTimeFull.parseOneLine(line);

		assertThat(result.getFlightDate(), is(LocalDate.of(2008, 1, 3)));
		assertThat(result.getCrsArrTime(), is(LocalTime.of(14, 35)));
		assertThat(result.getCrsDepTime(), is(LocalTime.of(13, 25)));
		assertThat(result.getDepDelayMinutes(), is(18));
		assertThat(result.getDest(), is("LIT"));
		assertThat(result.getArrDelayMinutes(), is(16));
		assertThat(result.getOrigin(), is("HOU"));
		assertThat(result.getUniqueCarrier(), is("WN (1)"));
	}
}
