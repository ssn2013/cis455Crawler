package com.datformers.storage;

import java.math.BigInteger;
import java.util.ArrayList;
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
	BigInteger docID;
	private String url; //url of file
	private String documentContents; //content
	private Date lastAccessedDate; //last accessed date
	
	ArrayList<String> extractedUrls;
	//TODO: add hashed Doc Id (BigInteger)
	//TODO: list of extracted URLs
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
	public BigInteger getDocID() {
		return docID;
	}
	public void setDocID(BigInteger docID) {
		this.docID = docID;
	}
	public ArrayList<String> getExtractedUrls() {
		return extractedUrls;
	}
	public void setExtractedUrls(ArrayList<String> extractedUrls) {
		this.extractedUrls = extractedUrls;
	}
}
