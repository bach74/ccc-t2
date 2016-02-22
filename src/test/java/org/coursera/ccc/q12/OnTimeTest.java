package org.coursera.ccc.q12;

import static org.junit.Assert.*;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.*;

public class OnTimeTest
{

	@Test
	public void testParseOneLine() throws Exception
	{
		String line = "2008,1,3,2008-01-03,\"WN\",\"HOU\",\"LIT\",18.00,16.00";
		OnTime parseOneLine = OnTime.parseOneLine(line);
		assertThat(parseOneLine.getArrDelayMinutes(), is(16.00));
		assertThat(parseOneLine.getUniqueCarrier(), is("WN"));
	}

	@Test
	public void testParseOneLineq21() throws Exception
	{
		String line = "2008,1,3,2008-01-03,\"WN\",\"HOU\",\"LIT\",18.00,16.00";
		org.coursera.ccc.q21.OnTime parseOneLine = org.coursera.ccc.q21.OnTime.parseOneLine(line);
		assertThat(parseOneLine.getDepDelayMinutes(), is(18.00));
		assertThat(parseOneLine.getUniqueCarrier(), is("WN"));
	}

	
	@Test
	public void testParseOneLineWithParenthesis21() throws Exception
	{
		String line = "2008,1,3,2008-01-03,\"WN (1)\",\"HOU\",\"LIT\",18.00,16.00";
		org.coursera.ccc.q21.OnTime parseOneLine = org.coursera.ccc.q21.OnTime.parseOneLine(line);
		assertThat(parseOneLine.getDepDelayMinutes(), is(18.00));
		assertThat(parseOneLine.getUniqueCarrier(), is("WN (1)"));
	}
	
}
