package fetch;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;

import com.datformers.storage.DBWrapper;
import com.datformers.storage.ParsedDocument;
import com.sleepycat.persist.PrimaryIndex;




public class FetchDocs extends HttpServlet{
	
	private static final long serialVersionUID = 2L;
	private String dbDir;
	private DBWrapper wrapper = new DBWrapper();
	PrimaryIndex<BigInteger, ParsedDocument> indexDocuments;
	
	@Override
	public void init() throws javax.servlet.ServletException {
		
		
		 
		dbDir = getInitParameter("crawlerdb"); //fetch details of database directory
		DBWrapper.initialize(dbDir); //initialize DB environment
		indexDocuments = wrapper.getStore().getPrimaryIndex(BigInteger.class, ParsedDocument.class);
		
	}
	
	@Override
	public void destroy() {
		wrapper.close();
	}
	
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response){
		
		if(request.getServletPath().contains("fetchdocs")) {	
			System.out.println("fetch:starting fetch docs");
		String docs=request.getParameter("ids");
		String wordString=request.getParameter("query");
		if(docs==null || docs.equals("") || wordString==null || wordString.equals("")) {
			System.out.println("Empty Parameters!!");
			return;
		}
		String[] docsIDs;
		String[] words;
		if(docs.contains(";")) {
			docsIDs=docs.split(";");
		} else {
			docsIDs=new String[1];
			docsIDs[0]=wordString;
		}
		if(wordString.contains(";")) {
			words=wordString.split(";;;;;");
		} else {
			words=new String[1];
			words[0]=wordString;
		}
		
		JSONObject mainObj = new JSONObject();
		JSONArray ja = new JSONArray();
		try {
		for(String id:docsIDs) {	
		BigInteger hashUrl= new BigInteger(id);
		ParsedDocument gotDoc = indexDocuments.get(hashUrl);
		if(gotDoc==null) {
			System.out.println("DocId not present!!");
			continue;
		}
		
		JSONObject jo = new JSONObject();
		String content=gotDoc.getDocumentContents();
		String title=content.substring(content.indexOf("<title>")+("<title>").length(),content.indexOf("</title>"));
		if(title==null) title="";
		//System.out.println(title);
//		String snippet=findSnippet(content,words);
		String snippet=findSnippet1(content,words);
		jo.put("docID",id.toString());
		jo.put("title",title);
		jo.put("url",gotDoc.getUrl());
		jo.put("snippet",snippet);
		
		ja.put(jo);	
		
		}
		mainObj.put("results", ja);
		response.setContentType("text/plain");
			PrintWriter output = response.getWriter();
			output.write(mainObj.toString());
			output.flush();
			output.close();
		} catch (Exception e) {
			//e.printStackTrace();
		}
		System.out.println("fetch:sent back the results");
		}
		
	}
	
	public String findSnippet1(String content,String[] words) {
		//System.out.println("CONTENT- "+content);
		String lowerContent = content.toLowerCase();
		//System.out.println();
		String snippet="";
		for(int i=0;i<words.length;i++) {
			int beg=-1;
			int end=-1;
			beg=lowerContent.indexOf(words[i]);
			
			if(beg==-1) {
				System.out.println("in continue");
				
				continue;
			}
			end=beg+50;
			beg=beg-20;
			if(beg<0) beg=0;
			else snippet+="...";
			if(end>content.length()) end=content.length();
			snippet+=content.substring(beg, end);
			snippet+="...";
		}
		return html2text(snippet);
		
	}
	public String findSnippet(String content,String[] words) {
		if(content.contains("<body>")) {
		content=content.substring(content.indexOf("<body>"));
		if(content.contains("</body>")) {
			content=content.substring(0, content.indexOf("</body>"));
		}
		}
		HashMap<String,Integer> sentences=new HashMap<String,Integer>();
		try {
			InputStream is = new ByteArrayInputStream(content.getBytes());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(is));
            
            String word_re = words[0];   
            String str="";

            for (int i = 1; i < words.length; i++)
                word_re += "|" + words[i];
            word_re = "[^.]*\\b(" + word_re + ")\\b[^.]*[.]";
            while(br1.ready()) { str += br1.readLine(); }
            Pattern re = Pattern.compile(word_re, 
                    Pattern.MULTILINE | Pattern.COMMENTS | 
                    Pattern.CASE_INSENSITIVE);
            Matcher match = re.matcher(str);
            String sentenceString="";
            while (match .find()) {
                sentenceString = match.group(0);
                
                if(sentences.containsKey(sentenceString)) {
                	sentences.put(sentenceString,sentences.get(sentenceString)+1);
                }
                else sentences.put(sentenceString,1);
            }
            br1.close();
        } catch (Exception e) {}
		Map.Entry<String, Integer> maxEntry = null;
		String max_size="";
		for (Map.Entry<String, Integer> entry : sentences.entrySet())
		{
			if(entry.getKey().length()>max_size.length()) max_size= entry.getKey();
		    if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
		    {
		        maxEntry = entry;
		    }
		}
//		return html2text(maxEntry.getKey());
		return html2text(max_size);
//		return max_size;
		
	}
	public static String html2text(String input) {
		if(input==null) return null;
	    return Jsoup.parse(input).text();

	}	
}
