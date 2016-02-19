package org.coursera.ccc.q21;

import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraTest
{
	@Test
	@Ignore
	public void connect()
	{
		try (Cluster cluster = Cluster.builder().addContactPoint("54.173.149.116").build(); Session session = cluster.connect("ccc")) {
			ResultSet results = session.execute("SELECT * FROM origin_airline WHERE origin='BCN'");
			for (Row row : results) {
				System.out.format("%s %s %f\n", row.getString("origin"), row.getString("carrier"), row.getFloat("avg_delay"));
			}

			System.out.println("--------------------------------------------------------------------");
			results = session.execute("SELECT * FROM origin_airline");
			for (Row row : results) {
				System.out.format("%s %s %f\n", row.getString("origin"), row.getString("carrier"), row.getFloat("avg_delay"));
			}
		}
	}

	@Test
	@Ignore
	public void save()
	{
		try (Cluster cluster = Cluster.builder().addContactPoint("54.173.149.116").build(); Session session = cluster.connect("ccc")) {
			// Insert one record into the users table
			session.execute("INSERT INTO origin_airline (origin, carrier, avg_delay) VALUES  ('ZGB', 'AUA', 10.00);");
		}
	}

	@Test
	@Ignore
	public void testPrepare()
	{

		try (Cluster cluster = Cluster.builder().addContactPoint("54.173.149.116").build(); Session session = cluster.connect("ccc")) {
			PreparedStatement statement = session.prepare("INSERT INTO origin_airline (origin, carrier, avg_delay) VALUES  (?,?,?);");

			BoundStatement boundStatement = new BoundStatement(statement);
			
			session.execute(boundStatement.bind("ZGB","JAT",new Double(1000.0).floatValue()));
		}
	}

}
