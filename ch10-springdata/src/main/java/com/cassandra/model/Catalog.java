package com.cassandra.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.Table; 

@Table( "catalog")
public class Catalog {
	@Id
	private String key;
	private String journal;
	private String publisher;
	private String edition;
	private String title;
	private String author;

	public Catalog() {
	}

	public Catalog(String key, String journal, String publisher, String edition, String title, String author) {
		this.key = key;
		this.journal = journal;
		this.publisher = publisher;
		this.edition = edition;
		this.title = title;
		this.author = author;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getJournal() {
		return journal;
	}

	public void setJournal(String journal) {
		this.journal = journal;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	public String getEdition() {
		return edition;
	}

	public void setEdition(String edition) {
		this.edition = edition;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}
	
}
