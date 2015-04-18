package com.datformers.storage;

import java.util.Date;

import org.w3c.dom.Document;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/*
 * Entity class of a fetched URL's contents
 */
@Entity
public class ParsedDocument {
	@PrimaryKey
	private String url; //url of file
	private String documentContents; //content
	private Date lastAccessedDate; //last accessed date
	public ParsedDocument() {
		
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDocumentContents() {
		return documentContents;
	}
	public void setDocumentContents(String documentContents) {
		this.documentContents = documentContents;
	}
	public Date getLastAccessedDate() {
		return lastAccessedDate;
	}
	public void setLastAccessedDate(Date lastAccessedDate) {
		this.lastAccessedDate = lastAccessedDate;
	}
}
