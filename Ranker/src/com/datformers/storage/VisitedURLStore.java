package com.datformers.storage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/*
 * Entity class of a fetched URL's contents
 */
@Entity
public class VisitedURLStore {
	@PrimaryKey
	BigInteger url;

	public BigInteger getUrl() {
		return url;
	}

	public void setUrl(BigInteger url) {
		this.url = url;
	}

	// TODO: add hashed Doc Id (BigInteger)
	// TODO: list of extracted URLs
	public VisitedURLStore() {

	}
}
