package org.sample.hector;

import java.util.Arrays; 
import me.prettyprint.cassandra.serializers.StringSerializer; 
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;  
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows; 
import me.prettyprint.hector.api.exceptions.HectorException; 
import me.prettyprint.hector.api.mutation.MutationResult; 
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery; 
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;

/**
 * Hello world!
 *
 */
public class HectorClient {

	private static Cluster cluster;
	private static Keyspace keyspace;
	private static ColumnFamilyTemplate<String, String> template;

	public static void main(String[] args) {
		cluster = HFactory.getOrCreateCluster("hector-cluster", "localhost:9160");
 		 
		
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("HectorKeyspace");
		if (keyspaceDef == null) {
			createSchema();
		}
		createKeyspace();
		createTemplate();
//		addTableData();
//		addTableDataColumn();
//		deleteTableDataColumn();
//		addTableDataColumn();
//		retrieveTableDataColumnQuery();
//		updateTableData();
//		deleteTableDataColumn();
//		retrieveTableDataColumnQuery();
//		deleteTableData();
//		retrieveTableData();
//		retrieveTableDataSliceQuery();
		retrieveTableDataMultigetSliceQuery();
	}

	public Cluster getOrCreateCluster(String clusterName, String hostIp) {
		Cluster cluster = HFactory.getOrCreateCluster(clusterName, new CassandraHostConfigurator(hostIp));
		return cluster;

	}

	protected static void createSchema() {
		int replicationFactor = 1;
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("HectorKeyspace", "catalog",
				ComparatorType.BYTESTYPE);
		KeyspaceDefinition keyspace = HFactory.createKeyspaceDefinition("HectorKeyspace",
				ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays.asList(cfDef));
		cluster.addKeyspace(keyspace, true);
	}

	private static void createKeyspace() {
		keyspace = HFactory.createKeyspace("HectorKeyspace", cluster);
	}

	private static void createTemplate() {
		template = new ThriftColumnFamilyTemplate<String, String>(keyspace, "catalog", StringSerializer.get(),
				StringSerializer.get());
	}

	private static void addTableData() {
		Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
		mutator.addInsertion("catalog1", "catalog", HFactory.createStringColumn("journal", "Oracle Magazine"))
				.addInsertion("catalog1", "catalog", HFactory.createStringColumn("publisher", "Oracle Publishing"))
				.addInsertion("catalog1", "catalog", HFactory.createStringColumn("edition", "November-December 2013"))
				.addInsertion("catalog1", "catalog",
						HFactory.createStringColumn("title", "Quintessential and Collaborative"))
				.addInsertion("catalog1", "catalog", HFactory.createStringColumn("author", "Tom Haunert"));
		mutator.addInsertion("catalog2", "catalog", HFactory.createStringColumn("journal", "Oracle	Magazine"))
				.addInsertion("catalog2", "catalog", HFactory.createStringColumn("publisher", "Oracle Publishing"))
				.addInsertion("catalog2", "catalog", HFactory.createStringColumn("edition", "November-December	2013"))
				.addInsertion("catalog2", "catalog", HFactory.createStringColumn("title", "Engineering as a Service"))
				.addInsertion("catalog2", "catalog", HFactory.createStringColumn("author", "David A. Kelly"));
		mutator.execute();
	}

	private static void retrieveTableData() {
		try {
			ColumnFamilyResult<String, String> res = template.queryColumns("catalog1");
			if (res.hasResults()) {
				String journal = res.getString("journal");
				String publisher = res.getString("publisher");
				String edition = res.getString("edition");
				String title = res.getString("title");
				String author = res.getString("author");
				System.out.println(journal);
				System.out.println(publisher);
				System.out.println(edition);
				System.out.println(title);
				System.out.println(author);
			}
			res = template.queryColumns("catalog2");
			if (res.hasResults()) {
				String journal = res.getString("journal");
				String publisher = res.getString("publisher");
				String edition = res.getString("edition");
				String title = res.getString("title");
				String author = res.getString("author");
				System.out.println(journal);
				System.out.println(publisher);
				System.out.println(edition);
				System.out.println(title);
				System.out.println(author);
			}
		} catch (HectorException e) {
		}
	}

	private static void retrieveTableDataColumnQuery() {
		ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspace);
		columnQuery.setColumnFamily("catalog").setKey("catalog3").setName("journal");
		//
		columnQuery.setColumnFamily("catalog").setKey("catalog1").setName("journal");
		QueryResult<HColumn<String, String>> result = columnQuery.execute();
		System.out.println(result.get());
	}

	private static void retrieveTableDataSliceQuery() {
		SliceQuery<String, String, String> query = HFactory
				.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
				.setKey("catalog2").setColumnFamily("catalog");
		ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query,
				"\u0000", "\uFFFF", false);
		while (iterator.hasNext()) {
			HColumn<String, String> column = iterator.next();
			System.out.println(column.getName());
			System.out.println(column.getValue());
		}
	}

	private static void addTableDataColumn() {
		Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
		MutationResult result = mutator.insert("catalog3", "catalog",
				HFactory.createStringColumn("journal", "Oracle  Magazine"));
		System.out.println(result);
	}

	private static void updateTableData() {
		ColumnFamilyUpdater<String, String> updater = template.createUpdater("catalog2");
		updater.setString("journal", "Oracle-Magazine");
		updater.setString("publisher", "Oracle-Publishing");
		updater.setString("edition", "11/12 2013");
		updater.setString("title", "Engineering as a Service");
		updater.setString("author", "Kelly, David A.");
		try {
			template.update(updater);
		} catch (HectorException e) {
		}
	}

	private static void deleteTableDataColumn() {
		Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
		mutator.delete("catalog3", "catalog", "journal", StringSerializer.get());
	}

	private static void deleteTableData() {
		Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
		mutator.addDeletion("catalog2", "catalog", "journal", StringSerializer.get())
				.addDeletion("catalog2", "catalog", "publisher", StringSerializer.get())
				.addDeletion("catalog2", "catalog", "edition", StringSerializer.get()).execute();
	}

	private static void retrieveTableDataMultigetSliceQuery() {
		MultigetSliceQuery<String, String, String> multigetSliceQuery = HFactory.createMultigetSliceQuery(keyspace,
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		multigetSliceQuery.setColumnFamily("catalog");
		multigetSliceQuery.setKeys("catalog1", "catalog2", "catalog3");
		// multigetSliceQuery.setRange("", "", false, 3);
		// multigetSliceQuery.setRange("", "", false, 2);
		multigetSliceQuery.setRange("", "", false, 5);
		QueryResult<Rows<String, String, String>> result = multigetSliceQuery.execute();
		System.out.println(result.get().getByKey("catalog1"));
		System.out.println(result.get().getByKey("catalog2"));
		System.out.println(result.get().getByKey("catalog3"));
	}

	private static void retrieveTableDataRangeSlicesQuery() {
		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace,
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		rangeSlicesQuery.setColumnFamily("catalog");
		rangeSlicesQuery.setKeys("catalog1", "catalog3");
		// rangeSlicesQuery.setRange("", "", false, 5);
		// rangeSlicesQuery.setRange("", "", false, 3);
		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		System.out.println(result.get().getByKey("catalog1"));
		System.out.println(result.get().getByKey("catalog2"));
		System.out.println(result.get().getByKey("catalog3"));
	}

	public static void setCluster(Cluster cluster) {
		HectorClient.cluster = cluster;
	}

	public static void setKeyspace(Keyspace keyspace) {
		HectorClient.keyspace = keyspace;
	}

	public static void setTemplate(ColumnFamilyTemplate<String, String> template) {
		HectorClient.template = template;
	}
	
}
