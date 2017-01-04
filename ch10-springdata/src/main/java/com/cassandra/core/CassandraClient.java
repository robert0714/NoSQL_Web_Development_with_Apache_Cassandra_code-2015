package com.cassandra.core;

import java.util.HashSet;
import java.util.Iterator;

import org.springframework.cassandra.core.RingMember;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.cassandra.config.SpringCassandraApplicationConfig;
import com.cassandra.model.Catalog;
import com.cassandra.repository.CatalogRepository;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

public class CassandraClient {
	static CassandraOperations ops;
	static CatalogRepository repository;

	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(SpringCassandraApplicationConfig.class);
		ops = context.getBean(CassandraOperations.class);
		repository = context.getBean(CatalogRepository.class);
		for (RingMember member : ops.describeRing()) {
			System.out.println(member.toString());
		}
		System.out.println("Table name: " + ops.getTableName(Catalog.class));

		// saveNew();
		// saveNewInBatch();
		// findAll();
		// findAllSpecifiedIds();
		// findById();
		// findAllByCql();
		// findOneByCql();
		// countRows();
		// exists();
		// update();
		// updateInBatch();
		// deleteById();
		// deleteByIdInBatch();
		// delete();
		// deleteInBatch();
	}

	private static void saveNew() {
		Catalog catalog1 = new Catalog("catalog1", "Oracle Magazine", "Oracle Publishing", "November-December 2013",
				"Engineering as a Service", "David A. Kelly");
		repository.save(catalog1);
		
	}

	private static void saveNewInBatch() {
		HashSet<Catalog> entities = new HashSet<Catalog>();
		Catalog catalog2 = new Catalog("catalog2", "Oracle Magazine", "Oracle Publishing", "November-December 2013",
				"Quintessential and Collaborative", "Tom Haunert");
		Catalog catalog3 = new Catalog("catalog3", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		Catalog catalog4 = new Catalog("catalog4", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		Catalog catalog5 = new Catalog("catalog5", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		Catalog catalog6 = new Catalog("catalog6", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		entities.add(catalog2);
		entities.add(catalog3);
		entities.add(catalog4);
		entities.add(catalog5);
		entities.add(catalog6);
		ops.insert(entities);
	}

	private static void countRows() {
		System.out.println("Number of rows: " + ops.count(Catalog.class));
	}
	 
	private static void exists() {
		Catalog catalog3 = new Catalog("catalog1", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		// System.out.println("The catalog3 entity exists: "+
		ops.exists(catalog3,"catalog1");
		// System.out.println("\n");
		System.out.println("The catalog entry with id catalog2 exists: " + ops.exists(Catalog.class, "catalog2"));
	}

	private static void findAll() {
		Iterator<Catalog> iter = repository.findAll().iterator();
		while (iter.hasNext()) {
			Catalog catalog = iter.next();
			System.out.println(catalog.getKey());
			System.out.println("\n");
			System.out.println(catalog.getJournal());
			System.out.println("\n");
			System.out.println(catalog.getPublisher());
			System.out.println("\n");
			System.out.println(catalog.getEdition());
			System.out.println("\n");
			System.out.println(catalog.getTitle());
			System.out.println("\n");
			System.out.println(catalog.getAuthor());
		}
	}

	private static void findAllSpecifiedIds() {
		HashSet<String> ids = new HashSet();
		ids.add("catalog1");

		ids.add("catalog2");
		Iterator<Catalog> iter = repository.findAll().iterator();
		while (iter.hasNext()) {
			Catalog catalog = iter.next();
			System.out.println(catalog.getKey());
			System.out.println("\n");
			System.out.println(catalog.getJournal());
			System.out.println("\n");
			System.out.println(catalog.getPublisher());
			System.out.println("\n");
			System.out.println(catalog.getEdition());
			System.out.println("\n");
			System.out.println(catalog.getTitle());
			System.out.println("\n");
			System.out.println(catalog.getAuthor());
		}

	}

	private static void findById() {
		Catalog catalog = repository.ById(Catalog.class, "catalog1");
		System.out.println(catalog.getKey());
		System.out.println("\n");
		System.out.println(catalog.getJournal());
		System.out.println("\n");
		System.out.println(catalog.getPublisher());
		System.out.println("\n");
		System.out.println(catalog.getEdition());
		System.out.println("\n");
		System.out.println(catalog.getTitle());
		System.out.println("\n");
		System.out.println(catalog.getAuthor());
	}

	private static void findAllByCql() {
		Iterator<Catalog> iter = ops.query(Catalog.class, "SELECT * FROM catalog").iterator();
		while (iter.hasNext()) {
			Catalog catalog = iter.next();
			System.out.println(catalog.getKey());
			System.out.println("\n");
			System.out.println(catalog.getJournal());
			System.out.println("\n");
			System.out.println(catalog.getPublisher());
			System.out.println("\n");
			System.out.println(catalog.getEdition());
			System.out.println("\n");
			System.out.println(catalog.getTitle());
			System.out.println("\n");
			System.out.println(catalog.getAuthor());
		}
	}

	private static void findOneByCql() {
		Catalog catalog = ops.findOne(Catalog.class, "SELECT * from catalog WHERE key='catalog1'");
		System.out.println(catalog.getKey());
		System.out.println("\n");
		System.out.println(catalog.getJournal());
		System.out.println("\n");
		System.out.println(catalog.getPublisher());
		System.out.println("\n");
		System.out.println(catalog.getEdition());
		System.out.println("\n");
		System.out.println(catalog.getTitle());
		System.out.println("\n");
		System.out.println(catalog.getAuthor());
	}

	private static void update() {
		Catalog catalog1 = new Catalog("catalog1", "Oracle Magazine", "Oracle-Publishing", "11/12 2013",
				"Engineering as a Service", "Kelly, David A.");
		ops.save(catalog1);
	}

	private static void updateInBatch() {
		HashSet<Catalog> entities = new HashSet();
		Catalog catalog2 = new Catalog("catalog2", "Oracle Magazine", "Oracle Publishing", "November-December 2013",
				"Quintessential and Collaborative", "Haunert, Tom");
		Catalog catalog3 = new Catalog("catalog3", "Oracle Magazine", "Oracle-Publishing", "Nov-Dec 2013", "", "");
		entities.add(catalog2);
		entities.add(catalog3);
		ops.saveInBatch(entities);
	}

	private static void deleteById() {
		ops.deleteById(Catalog.class, "catalog3");
	}

	private static void deleteByIdInBatch() {
		HashSet<String> ids = new HashSet();
		ids.add("catalog1");
		ids.add("catalog2");
		ops.deleteByIdInBatch(Catalog.class, ids);
	}

	private static void delete() {
		Catalog catalog4 = new Catalog("catalog4", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		ops.delete(catalog4);
	}

	private static void deleteInBatch() {
		HashSet<Catalog> entities = new HashSet<Catalog>();
		Catalog catalog5 = new Catalog("catalog5", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		Catalog catalog6 = new Catalog("catalog6", "Oracle Magazine", "Oracle Publishing", "November-December 2013", "",
				"");
		entities.add(catalog5);
		entities.add(catalog6);
		ops.deleteInBatch(entities);
	}
}
