package org.sample.hector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sample.hector.HectorClient;

import me.prettyprint.hector.api.Cluster;

public class HectorClientTest {
    private HectorClient component;
	@Before
	public void setUp() throws Exception {
		component = new HectorClient();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetOrCreateCluster() throws Exception {
		component.	getOrCreateCluster("hector-cluster", "localhost:9160");
		System.out.println("Hello World!");;
	}
	@Test
	public void testCreateSchema() throws Exception {
		final	Cluster cluster = component.	getOrCreateCluster("hector-cluster", "localhost:9160");
		component.setCluster(cluster);
		component.	createSchema();
		System.out.println("Hello World!");;
	}

}
