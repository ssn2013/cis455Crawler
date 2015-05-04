package edu.upenn.cis455.mapreduce.job;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.List;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class RemoveSinks implements Job{

	public void map(String key, List<String> value, Context context) {

		for (String items : value) {
			context.write(key, items);
			System.out.println(key+"\t"+items);
		}

	}

	public void reduce(String key, String[] values, Context context) {
		
		String outlinks = "1";
		for(String items:values){
			outlinks = outlinks + "\t" + items;
		}
		context.write(key,outlinks.trim());
		System.out.println(key+"\t"+outlinks.trim());
	}

	public static BigInteger convertToBigInt(byte[] data) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			int halfbyte = (data[i] >>> 4) & 0x0F;
			int two_halfs = 0;
			do {
				if ((0 <= halfbyte) && (halfbyte <= 9))
					buf.append((char) ('0' + halfbyte));
				else
					buf.append((char) ('a' + (halfbyte - 10)));
				halfbyte = data[i] & 0x0F;
			} while (two_halfs++ < 1);
		}
		return new BigInteger(buf.toString(), 16);
	}

	/*
	 * 
	 * This function is used to calculate the SHA1 hash
	 */
	public static BigInteger SHA1(String text) {
		try {
			MessageDigest md;
			md = MessageDigest.getInstance("SHA-1");

			byte[] sha1hash = new byte[40];
			md.update(text.getBytes("iso-8859-1"), 0, text.length());
			sha1hash = md.digest();
			// String string=convertToHex(sha1hash);
			return convertToBigInt(sha1hash);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		// return new BigInteger(sha1hash);
	}

}
