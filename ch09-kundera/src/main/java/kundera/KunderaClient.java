package kundera;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceContextType;
import javax.persistence.PersistenceUnit;
 

public class KunderaClient {
	private static EntityManager em;
	private static EntityManagerFactory emf;

	public static void main(String[] args) {
		emf = Persistence.createEntityManagerFactory("kundera");
		em = emf.createEntityManager();
		create();
//		 findByClass();
		 query();
//		 update();
//		 delete();
	}

	private static void create() {
		Catalog catalog = new Catalog();
		catalog.setCatalogId("catalog1");
		catalog.setJournal("Oracle Magazine");
		catalog.setPublisher("Oracle Publishing");
		catalog.setEdition("November-December 2013");
		catalog.setTitle("Engineering as a Service");
		catalog.setAuthor("David A. Kelly");
		em.persist(catalog);
		catalog = new Catalog();
		catalog.setCatalogId("catalog2");
		catalog.setJournal("Oracle Magazine");
		catalog.setPublisher("Oracle Publishing");
		catalog.setEdition("November-December 2013");
		catalog.setTitle("Quintessential and Collaborative");
		catalog.setAuthor("Tom Haunert");
		em.persist(catalog);
		catalog = new Catalog();
		catalog.setCatalogId("catalog3");
		catalog.setJournal("Oracle Magazine");
		catalog.setPublisher("Oracle Publishing");
		catalog.setEdition("November-December 2013");
		catalog.setTitle("");
		catalog.setAuthor("");
		em.persist(catalog);
	}
	private static void findByClass() {
		Catalog catalog = em.find(Catalog.class, "catalog1");
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
	
	private static void query() {
		javax.persistence.Query query = em
		.createQuery("SELECT c FROM Catalog c");
		List<Catalog> results = query.getResultList();
			if(results != null) {
				for (Catalog catalog : results) {
					System.out.println(catalog.getCatalogId());
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
	}
	private static void update() {
		Catalog catalog = em.find(Catalog.class, "catalog1");
		catalog.setEdition("Nov-Dec 2013");
		em.persist(catalog);
		em.createQuery("UPDATE Catalog c SET c.journal = 'Oracle-Magazine'").executeUpdate();
		/*
		* em.createQuery(
		* "UPDATE Catalog c SET c.author = 'Kelly, David A.' WHERE
		c.catalogId='catalog1'"
		* ) .executeUpdate(); update with WHERE does not get applied.
		*/
		System.out.println("After updating");
		System.out.println("\n");
		query();
	}
	private static void delete() {
		Catalog catalog = em.find(Catalog.class, "catalog1");
		em.remove(catalog);
		catalog = em.find(Catalog.class, "catalog2");
		em.remove(catalog);
		catalog = em.find(Catalog.class, "catalog3");
		em.remove(catalog);
		System.out.println("After removing catalog3");
		query();
		/*
		* em.createQuery(
		* "DELETE FROM Catalog c WHERE c.title='Engineering As a Service'")
		* .executeUpdate(); System.out.println("\n"); //
		* System.out.println("After removing catalog1"); query();
		*/
		// DELETE with WHERE does not get applied.
		em.createQuery("DELETE FROM Catalog c").executeUpdate();
		System.out.println("\n");
		System.out.println("After removing all catalog entries");
		query();
	}

	private static void close() {
		em.close();
		// emf.close();
	}
}