package com.cassandra.repository;

import org.springframework.data.cassandra.repository.CassandraRepository; 

import com.cassandra.model.Catalog;

public interface CatalogRepository extends CassandraRepository<Catalog>{

}
