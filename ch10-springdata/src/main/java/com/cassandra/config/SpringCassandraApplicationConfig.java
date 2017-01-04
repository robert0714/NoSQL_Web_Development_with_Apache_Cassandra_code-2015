package com.cassandra.config;
 
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.java.AbstractCassandraConfiguration;
 
@Configuration
public class SpringCassandraApplicationConfig extends AbstractCassandraConfiguration {
	public static final String KEYSPACE = "springdata"; 
	@Override
	protected String getKeyspaceName() {
		return KEYSPACE;
	}

}
