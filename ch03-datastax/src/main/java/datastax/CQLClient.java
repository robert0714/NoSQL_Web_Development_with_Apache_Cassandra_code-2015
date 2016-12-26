package datastax;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * Hello world!
 *
 */
public class CQLClient {

	private static Cluster cluster;
	private static Session session;

	public static void main(String[] args) {
		connection();
		createKeyspace();
		createTable();
		insert();
//		 select();
//		 dropIndex();
//		 createIndex();
//		 selectFilter();
//		 asyncQuery();
//		 preparedStmtQuery();
//		 update();
//		 delete();
		batch();
		// dropTable();
		// dropKeyspace();
		// closeConnection();
	}

	private static void connection() {
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		session = cluster.connect();
	}

	private static void createKeyspace() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS datastax WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};");
	}

	private static void createTable() {
		session.execute(
				"CREATE TABLE IF NOT EXISTS datastax.catalog (catalog_id text,journal text,publisher text, edition text,title text,author text,PRIMARY KEY(catalog_id, journal))");
		session.execute(
				"CREATE TABLE IF NOT EXISTS datastax.catalog2 (catalog_id text,journal text,publisher text, edition text,title text,author text,PRIMARY KEY(journal, catalog_id))");
		session.execute(
				"CREATE TABLE IF NOT EXISTS datastax.catalog3 (catalog_id text,journal text,publisher text, edition text,title text,author text,PRIMARY KEY(journal, catalog_id, publisher))");
	}

	private static void insert() {
		session.execute(
				"INSERT INTO datastax.catalog (catalog_id, journal, publisher,edition,title,author) VALUES ('catalog1','Oracle Magazine', 'Oracle Publishing','November-December 2013', 'Engineering as a Service','David A. Kelly') IF NOT EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog (catalog_id, journal, publisher, edition,title,author) VALUES ('catalog2','Oracle Magazine', 'Oracle Publishing','November-December 2013', 'Quintessential and Collaborative','Tom Haunert') IF NOT EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog (catalog_id, journal, publisher) VALUES ('catalog3', 'Oracle Magazine','Oracle Publishing') IF NOT EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog2 (catalog_id, journal, publisher, edition,title,author) VALUES ('catalog1','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', 'Engineering as a Service','David A. Kelly') IF NOT EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog2 (catalog_id, journal, publisher,	edition,title,author) VALUES ('catalog2','Oracle Magazine', 'Oracle Publishing','November-December 2013', 'Quintessential and Collaborative','Tom Haunert') IF NOT	EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog2 (catalog_id, journal, publisher)	VALUES ('catalog3', 'Oracle Magazine','Oracle Publishing') IF NOT EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog3 (catalog_id, journal, publisher,	edition,title,author) VALUES ('catalog1','Oracle Magazine', 'Oracle Publishing','November-December 2013', 'Engineering as a Service','David A. Kelly') IF NOT	EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog3 (catalog_id, journal, publisher,	edition,title,author) VALUES ('catalog2','Oracle Magazine', 'Oracle Publishing','November-December 2013', 'Quintessential and Collaborative','Tom Haunert') IF NOT	EXISTS");
		session.execute(
				"INSERT INTO datastax.catalog3 (catalog_id, journal, publisher)	VALUES ('catalog3', 'Oracle Magazine','Oracle Publishing') IF NOT EXISTS");
	}

	private static void select() {
		// ResultSet results =
		// session.execute("select * from datastax.catalog");
		// ResultSet results =
		// session.execute("select * from datastax.catalog2 WHERE
		// journal='Oracle Magazine' ORDER BY catalog_id DESC");
		// ResultSet results =
		// session.execute("select * from datastax.catalog3 WHERE
		// journal='Oracle Magazine' ORDER BY publisher");
		// generates error
		ResultSet results = session
				.execute("select * from datastax.catalog3 WHERE journal='Oracle Magazine' ORDER BY catalog_id");
		for (Row row : results) {
			System.out.println("Catalog Id: " + row.getString("catalog_id"));
			System.out.println("Journal: " + row.getString("journal"));
			System.out.println("Publisher: " + row.getString("publisher"));
			System.out.println("Edition: " + row.getString("edition"));
			System.out.println("Title: " + row.getString("title"));
			System.out.println("Author: " + row.getString("author"));
			System.out.println("\n");
			System.out.println("\n");
		}
	}

	private static void createIndex() {
		session.execute("CREATE INDEX titleIndex ON datastax.catalog (title)");
	}

	private static void selectFilter() {
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog2 WHERE
		 * journal='Oracle Magazine' AND catalog_id > 'catalog1'" ); for (Row
		 * row : results) { System.out.println("Catalog Id: " +
		 * row.getString("catalog_id")); System.out.println("Journal: " +
		 * row.getString("journal")); System.out.println("Publisher: " +
		 * row.getString("publisher")); System.out.println("Edition: " +
		 * row.getString("edition")); System.out.println("Title: " +
		 * row.getString("title")); System.out.println("Author: " +
		 * row.getString("author")); System.out.println("\n");
		 * System.out.println("\n"); }
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog WHERE
		 * catalog_id > 'catalog1'" ); for (Row row : results) {
		 * System.out.println("Catalog Id: " + row.getString("catalog_id"));
		 * System.out.println("Journal: " + row.getString("journal"));
		 * System.out.println("Publisher: " + row.getString("publisher"));
		 * System.out.println("Edition: " + row.getString("edition"));
		 * System.out.println("Title: " + row.getString("title"));
		 * System.out.println("Author: " + row.getString("author"));
		 * System.out.println("\n"); System.out.println("\n"); }
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog2 WHERE
		 * journal='Oracle Magazine' AND catalog_id >= 'catalog1'" ); for (Row
		 * row : results) { System.out.println("Catalog Id: " +
		 * row.getString("catalog_id")); System.out.println("Journal: " +
		 * row.getString("journal")); System.out.println("Publisher: " +
		 * row.getString("publisher")); System.out.println("Edition: " +
		 * row.getString("edition")); System.out.println("Title: " +
		 * row.getString("title")); System.out.println("Author: " +
		 * row.getString("author")); System.out.println("\n");
		 * System.out.println("\n"); }
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog WHERE
		 * title='Engineering as a Service'" ); for (Row row : results) {
		 * System.out.println("Journal: " + row.getString("journal"));
		 * System.out.println("Publisher: " + row.getString("publisher"));
		 * System.out.println("Edition: " + row.getString("edition"));
		 * System.out.println("Title: " + row.getString("title"));
		 * System.out.println("Author: " + row.getString("author"));
		 * System.out.println("\n"); System.out.println("\n");
		 *
		 * }
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog WHERE
		 * catalog_id='catalog2'" );
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog WHERE
		 * journal='Oracle Magazine' ALLOW FILTERING" );
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog WHERE
		 * journal='Oracle Magazine' AND catalog_id='catalog2'" ); for (Row row
		 * : results) { System.out.println("Journal: " +
		 * row.getString("journal")); System.out.println("Publisher: " +
		 * row.getString("publisher")); System.out.println("Edition: " +
		 * row.getString("edition")); System.out.println("Title: " +
		 * row.getString("title")); System.out.println("Author: " +
		 * row.getString("author")); System.out.println("\n");
		 * System.out.println("\n");
		 *
		 * }
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog WHERE
		 * catalog_id IN ('catalog2', 'catalog3')" );
		 */
		/*
		 * ResultSet results = session .execute( "SELECT catalog_id, journal,
		 * publisher, edition,title,author FROM datastax.catalog WHERE title IN
		 * ('Quintessential and Collaborative', 'Engineering as a Service')" );
		 */
		/*
		 * for (Row row : results) { System.out.println("Journal: " +
		 * row.getString("journal")); System.out.println("Publisher: " +
		 * row.getString("publisher")); System.out.println("Edition: " +
		 * row.getString("edition")); System.out.println("Title: " +
		 * row.getString("title")); System.out.println("Author: " +
		 * row.getString("author")); System.out.println("\n");
		 * System.out.println("\n"); }
		 */
	}

	private static void asyncQuery() {
		ResultSetFuture resultsFuture = session.executeAsync("Select * from datastax.catalog");
		try {
			ResultSet results = resultsFuture.getUninterruptibly(1000000, TimeUnit.MILLISECONDS);
			for (Row row : results) {
				System.out.println("Journal: " + row.getString("journal"));
				System.out.println("Publisher: " + row.getString("publisher"));
				System.out.println("Edition: " + row.getString("edition"));
				System.out.println("Title: " + row.getString("title"));
				System.out.println("Author: " + row.getString("author"));
				System.out.println("\n");
				System.out.println("\n");
			}
		} catch (TimeoutException e) {
			resultsFuture.cancel(true);
			System.out.println(e);
		}
	}

	private static void preparedStmtQuery() {
		PreparedStatement stmt = session.prepare(
				"SELECT catalog_id, journal, publisher, edition,title,author FROM datastax.catalog WHERE title=?");
		BoundStatement boundStmt = new BoundStatement(stmt);
		boundStmt.bind("Engineering as a Service");
		ResultSet results = session.execute(boundStmt);
		for (Row row : results) {
			System.out.println("Journal: " + row.getString("journal"));
			System.out.println("Publisher: " + row.getString("publisher"));
			System.out.println("Edition: " + row.getString("edition"));
			System.out.println("Title: " + row.getString("title"));
			System.out.println("Author: " + row.getString("author"));
			System.out.println("\n");
			System.out.println("\n");
		}
	}

	private static void update() {
		session.execute(
				"UPDATE datastax.catalog SET edition = '11/12 2013', author ='Kelley, David A.' WHERE catalog_id = 'catalog1' AND journal='Oracle Magazine' IF edition='November-December 2013'");
		ResultSet results = session.execute(
				"SELECT catalog_id, journal, publisher, edition,title,author FROM datastax.catalog WHERE catalog_id='catalog1'");
		for (Row row : results) {
			System.out.println("Journal: " + row.getString("journal"));
			System.out.println("Publisher: " + row.getString("publisher"));
			System.out.println("Edition: " + row.getString("edition"));
			System.out.println("Title: " + row.getString("title"));
			System.out.println("Author: " + row.getString("author"));
			System.out.println("\n");
			System.out.println("\n");
		}
	}

	private static void delete() {
		// session.execute("DELETE journal, publisher from datastax.catalog
		// WHERE catalog_id='catalog2'");//generates
		// error
		// session.execute("DELETE from datastax.catalog WHERE
		// catalog_id='catalog1'");
		// //equivalent
		// session.execute("DELETE from datastax.catalog WHERE journal='Oracle
		// Magazine'");//generates
		// error
		/*
		 * session.execute( "DELETE from datastax.catalog WHERE
		 * catalog_id='catalog1' AND journal='Oracle Magazine'" );//equivalent
		 * ResultSet results = session.execute("select * from datastax.catalog"
		 * ); for (Row row : results) { System.out.println("Catalog Id: " +
		 * row.getString("catalog_id")); System.out.println("Journal: " +
		 * row.getString("journal")); System.out.println("Publisher: " +
		 * row.getString("publisher")); System.out.println("Edition: " +
		 * row.getString("edition")); System.out.println("Title: " +
		 * row.getString("title")); System.out.println("Author: " +
		 * row.getString("author")); System.out.println("\n");
		 * System.out.println("\n");
		 *
		 * }
		 */
		/*
		 * Caused by: com.datastax.driver.core.exceptions.InvalidQueryException:
		 * Missing mandatory PRIMARY KEY part journal since publisher specified
		 */
		// session.execute("DELETE publisher, edition from datastax.catalog
		// WHERE catalog_id='catalog2'");//generates
		// error
		// session.execute("DELETE publisher, edition from datastax.catalog
		// WHERE catalog_id='catalog1' AND journal='Oracle Magazine'");
		// session.execute("DELETE from datastax.catalog WHERE
		// catalog_id='catalog1'");
		// session.execute("DELETE from datastax.catalog WHERE journal='Oracle
		// Magazine'");//generates
		// error
		// session.execute("DELETE publisher, edition from datastax.catalog
		// WHERE catalog_id='catalog2'");"+ "
		// session.execute("DELETE publisher, edition from datastax.catalog
		// WHERE catalog_id='catalog2' AND journal='Oracle Magazine'");
		ResultSet results = session.execute("select * from datastax.catalog");
		for (Row row : results) {
			System.out.println("Catalog Id: " + row.getString("catalog_id"));
			System.out.println("Journal: " + row.getString("journal"));
			System.out.println("Publisher: " + row.getString("publisher"));
			System.out.println("Edition: " + row.getString("edition"));
			System.out.println("Title: " + row.getString("title"));
			System.out.println("Author: " + row.getString("author"));
			System.out.println("\n");
			System.out.println("\n");
		}
	}

	private static void batch() { 
//		session.execute(
//				"BEGIN BATCH INSERT INTO datastax.catalog (catalog_id,journal, publisher, edition,title,author) VALUES ('catalog2','Oracle Magazine','Oracle Publishing', 'November-December 2013', 'Quintessential and Collaborative','Tom Haunert')IF NOT EXISTS INSERT INTO datastax.catalog(catalog_id, journal, publisher, edition,title,author) VALUES('catalog3','Oracle Magazine', 'Oracle Publishing', 'November-December 2013','','') IF NOT EXISTS INSERT INTO datastax.catalog (catalog_id, journal, publisher,edition,title,author) VALUES ('catalog4','Oracle Magazine', 'Oracle Publishing','November-December 2013', '','')IF NOT EXISTS APPLY BATCH");
		session.execute(
				"CREATE TABLE IF NOT EXISTS datastax.catalog4 (catalog_id text,journal text,publisher text, edition text,title text,author text,PRIMARY KEY(journal, catalog_id, publisher))");
		session.execute(
				"BEGIN BATCH INSERT INTO datastax.catalog4 (catalog_id,journal, publisher, edition,title,author) VALUES ('catalog1','Oracle Magazine','Oracle Publishing', 'November-December 2013', 'Quintessential and Collaborative','Tom Haunert') INSERT INTO datastax.catalog4 (catalog_id, journal,publisher, edition,title,author) VALUES ('catalog2','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', '','') INSERT INTO datastax.catalog4(catalog_id, journal, publisher, edition,title,author) VALUES('catalog3','Oracle Magazine', 'Oracle Publishing', 'November-December 2013','','') APPLY BATCH");
		ResultSet results = session.execute("select * from datastax.catalog4");
		for (Row row : results) {
			System.out.println("Catalog Id: " + row.getString("catalog_id"));
			System.out.println("Journal: " + row.getString("journal"));
			System.out.println("Publisher: " + row.getString("publisher"));
			System.out.println("Edition: " + row.getString("edition"));
			System.out.println("Title: " + row.getString("title"));
			System.out.println("Author: " + row.getString("author"));
			System.out.println("\n");
			System.out.println("\n");
		}
	}

	private static void dropIndex() {
		session.execute("USE datastax");
		session.execute("DROP INDEX IF EXISTS titleIndex");
	}

	private static void dropTable() {
		session.execute("DROP TABLE IF EXISTS datastax.catalog");
	}

	private static void dropKeyspace() {
		session.execute("DROP KEYSPACE IF EXISTS datastax");
	}

	private static void closeConnection() {
		cluster.close();
	}
}
