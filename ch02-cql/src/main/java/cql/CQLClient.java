package cql;

import java.util.Iterator;
import java.util.List;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

public class CQLClient {
	private static Cluster cluster;
	private static Keyspace keyspace;

	public static void main(String[] args) {
		cluster = HFactory.getOrCreateCluster("cql-cluster", "localhost:9160");
		/*
		 * Some of the method invocations in the main method have been commented
		 * out and should be uncommented as required to run individually or in
		 * sequence.
		 */
		createKeyspace();
		createCF();
		// insert();
		// select();
		// createIndex();
		// selectFilter();
		// update();
		// select();
		// batch();
		// select();
		// delete();
		// update2();
		// select();
		// updateCF();
		// select();
		// updateCF2();
		// dropCF();
		// dropKeyspace();
	}

	/* Creates a Cassandra keyspace */
	private static void createKeyspace() {
		keyspace = HFactory.createKeyspace("CQLKeyspace", cluster);
	}

	/* Drops a Cassandra keyspace */
	private static void dropKeyspace() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery("DROP KEYSPACE CQLKeyspace");
		cqlQuery.execute();
	}

	/* Creates an index */
	private static void createIndex() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery("CREATE INDEX titleIndex ON catalog (title)");
		cqlQuery.execute();
	}

	/* Creates a column family */
	private static void createCF() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery(
				"CREATE COLUMNFAMILY catalog (catalog_id text	PRIMARY KEY,journal text,publisher text,edition text,title text,author text) WITH	comparator=UTF8Type AND default_validation=UTF8Type AND caching=keys_only AND replicate_on_write=true");
		cqlQuery.execute();
		cqlQuery.setQuery(
				"CREATE COLUMNFAMILY catalog2 (KEY text PRIMARY	KEY,journal text,publisher text,edition text,title text,author text)");
		cqlQuery.execute();
	}

	/* Adds data to a column family */
	private static void insert() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery(
				"INSERT INTO catalog (catalog_id, journal,publisher, edition,title,author) VALUES ('catalog1','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', 'Engineering as a Service','David	A. Kelly')");
		cqlQuery.execute();
		cqlQuery.setQuery(
				"INSERT INTO catalog (catalog_id, journal,publisher, edition,title,author) VALUES ('catalog2','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', 'Quintessential and Collaborative','Tom Haunert')");
		cqlQuery.execute();
		cqlQuery.setQuery(
				"INSERT INTO catalog (catalog_id, journal,publisher, edition,title,author) VALUES ('catalog3','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', 'Engineering as a Service','David A. Kelly')");
		cqlQuery.execute();
		cqlQuery.setQuery(
				"INSERT INTO catalog (catalog_id, publisher,edition,title,author) VALUES ('catalog4', 'Oracle Publishing', 'November-December 2013', 'Engineering as a Service','David A. Kelly')");
		cqlQuery.execute();
		cqlQuery.setQuery(
				"INSERT INTO catalog2 (KEY, journal, publisher,edition,title,author) VALUES ('catalog1','Oracle Magazine', 'Oracle Publishing','November-December 2013', 'Engineering as a Service','David A. Kelly')");
		cqlQuery.execute();
	}

	/* Selects data from a column family */
	private static void select() {
		CqlQuery<String, String, String> cqlQuery = new CqlQuery<String, String, String>(keyspace,
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		cqlQuery.setQuery("select * from catalog");
		QueryResult<CqlRows<String, String, String>> result = cqlQuery.execute();
		System.out.println(result);
		Iterator iterator = result.get().iterator();
		while (iterator.hasNext()) {
			Row row = (Row) iterator.next();
			String key = (String) row.getKey();
			ColumnSlice columnSlice = row.getColumnSlice();
			List columnList = columnSlice.getColumns();
			Iterator iter = columnList.iterator();
			while (iter.hasNext()) {
				HColumn column = (HColumn) iter.next();
				System.out.println("Column name: " + column.getName() + " ");
				System.out.println("Column Value: " + column.getValue());
				System.out.println("\n");
			}
		}
	}

	/* Selects data from a column family using a WHERE clause */
	private static void selectFilter() {
		CqlQuery<String, String, String> cqlQuery = new CqlQuery<String, String, String>(keyspace,
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		// cqlQuery.setQuery("SELECT catalog_id, journal, publisher,
		// edition,title,author FROM catalog WHERE title='Engineering as a
		// Service'");
		cqlQuery.setQuery("SELECT journal, publisher, edition,title,author FROM catalog2 WHERE KEY='catalog1'");
		// cqlQuery.setQuery("SELECT catalog_id, journal, publisher,
		// edition,title,author FROM catalog WHERE
		// catalog_id='catalog1'");//Generates exception
		QueryResult<CqlRows<String, String, String>> result = cqlQuery.execute();
		System.out.println(result);
		Iterator iterator = result.get().iterator();
		while (iterator.hasNext()) {
			Row row = (Row) iterator.next();
			String key = (String) row.getKey();
			ColumnSlice columnSlice = row.getColumnSlice();
			List columnList = columnSlice.getColumns();
			Iterator iter = columnList.iterator();
			while (iter.hasNext()) {
				HColumn column = (HColumn) iter.next();
				System.out.println("Column name: " + column.getName() + " ");
				System.out.println("Column Value: " + column.getValue());
				System.out.println("\n");
			}
		}
	}

	/* Updates a row or rows of data in a column family */
	private static void update() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery(
				"UPDATE catalog USING CONSISTENCY ALL SET	'edition' = '11/12 2013', 'author' = 'Kelley, David A.' WHERE CATALOG_ID ='catalog1'");
		cqlQuery.execute();
	}

	/* Updates a row or rows of data in a column family */
	private static void update2() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery(
				"UPDATE catalog USING CONSISTENCY ALL SET	'edition' = 'November-December 2013', 'author' = 'Kelley, David A.' WHERE CATALOG_ID = 'catalog1'");
		cqlQuery.execute();
	}

	/* Deletes columns from a row or rows of data in a column family */
	private static void delete() {
		CqlQuery cqlQuery = new CqlQuery<String, String, String>(keyspace, StringSerializer.get(),
				StringSerializer.get(), StringSerializer.get());
		cqlQuery.setQuery("DELETE journal, publisher from catalog WHERE	catalog_id='catalog3'");
		cqlQuery.execute();
		cqlQuery.setQuery("DELETE from catalog WHERE catalog_id='catalog4'");
		cqlQuery.execute();
		cqlQuery.setQuery(
				"DELETE catalog_id, journal, publisher, edition,	title, author from catalog WHERE catalog_id='catalog4'");
		cqlQuery.execute();
	}

	/* Runs multiple statements in a batch */
	private static void batch() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery(
				"BEGIN BATCH USING CONSISTENCY QUORUM UPDATE	catalog SET 'edition' = '11/12 2013', 'author' = 'Haunert, Tom' WHERE CATALOG_ID ='catalog2' INSERT INTO catalog (catalog_id, journal, publisher, edition,title,	author) VALUES ('catalog3','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', '','') INSERT INTO catalog (catalog_id, journal, publisher,edition,title,author) VALUES ('catalog4','Oracle Magazine', 'Oracle Publishing','November-December 2013', '','') UPDATE catalog SET 'edition' = '11/12 2013' WHERE CATALOG_ID = 'catalog3' APPLY BATCH");
		cqlQuery.execute();
	}

	/* Updates a column family */
	private static void updateCF() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery("ALTER COLUMNFAMILY catalog ALTER edition TYPE int");
		cqlQuery.execute();
	}

	/* Updates a column family */
	private static void updateCF2() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());

		cqlQuery.setQuery("ALTER COLUMNFAMILY catalog ALTER edition TYPE text");
		cqlQuery.execute();
		cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		cqlQuery.setQuery("ALTER COLUMNFAMILY catalog ALTER journal TYPE	int");
		cqlQuery.execute();
		/* CF gets updated with column to a type different from column value */
		cqlQuery.setQuery(
				"INSERT INTO catalog (catalog_id, journal,publisher, edition,title,author) VALUES ('catalog5','Oracle Magazine', 'Oracle Publishing', 'November-December 2013', '','')");
		cqlQuery.execute();
	}

	/* Drops a column family */
	private static void dropCF() {
		CqlQuery cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
				StringSerializer.get());
		cqlQuery.setQuery("DROP COLUMNFAMILY catalog");
		cqlQuery.execute();
	}
}
