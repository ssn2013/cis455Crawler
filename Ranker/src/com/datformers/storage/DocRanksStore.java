package com.datformers.storage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;


public class DocRanksStore {
	
	@PrimaryKey
	BigInteger docId;
	double rank;

	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}

	public BigInteger getDocId() {
		return docId;
	}

	public void setDocId(BigInteger docId) {
		this.docId = docId;
	}

	public DocRanksStore() {

	}

}

