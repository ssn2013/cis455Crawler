package storage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity(version=1)
public class WordIndexEntity {
	
	@PrimaryKey
	private String word;
	private List<BigInteger> docIds = new ArrayList<BigInteger>();
	private List<Double> rank = new ArrayList<Double>();
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public List<BigInteger> getDocIds() {
		return docIds;
	}
	public void setDocIds(List<BigInteger> docIds) {
		this.docIds = docIds;
	}
	public List<Double> getRank() {
		return rank;
	}
	public void setRank(List<Double> ranks) {
		this.rank = ranks;
	}

}
