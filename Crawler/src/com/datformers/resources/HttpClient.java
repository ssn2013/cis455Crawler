package com.datformers.resources;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.HttpsURLConnection;

/*
 * Class handles all HTTP calls: requests and responses
 */
public class HttpClient {
	public String url;
	private HashMap<String, String> requestHeaders = new HashMap<String, String>();
	private HashMap<String, String> responseHeaders = new HashMap<String, String>();
	private OutputStream connectionOutputStream = null;
	private InputStream connectionInputStream = null;
	private InputStream bodyInputStream = null;
	private String host = null;
	public String method = "GET";
	public String version = "HTTP/1.1";
	public boolean isHtml;
	private int responseStatusCode = -1;
	private String body;
	private boolean isHTTPS = false; 
	private HttpsURLConnection HTTPSconnection = null;
	private HttpURLConnection HTTPconnection = null;

	/*
	 * Method to add and keep track of header requests
	 */
	public void addRequestHeader(String key, String value) {
		if(requestHeaders.containsKey(key)) {
			String headerValue = requestHeaders.get(key);
			headerValue += (", "+value);
			requestHeaders.put(key, headerValue);
		} else
			requestHeaders.put(key, value);
	}

	/*
	 *Constructor 
	 */
	public HttpClient() {

	}

	/*
	 * Method extracts host name for the given URL
	 */
	public String getHost() {
		return host;
	}

	public String getHost(String url) {
		if(url.startsWith("http")) 
			host = url.substring(url.indexOf('/')+2); //remove protocol part
		if(host.contains("/"))
			host = host.substring(0, host.indexOf('/'));//remove any additional paths
		return host;
	}

	/*
	 * Method constructs the entire request object with given headers and returns a string
	 */
	public String getRequestString() {
		StringBuffer requestBuffer = new StringBuffer(method+" "+url+" "+version+'\n'); //first line of request
		requestBuffer.append("Host: "+getHost()+"\n"); //Mandatory "host" header for Version 1.1
		for(String key: requestHeaders.keySet()) 
			requestBuffer.append(key+": "+requestHeaders.get(key)+"\n");
		return requestBuffer.toString();
	}

	public void clearResponseHeaders() {
		responseHeaders = new HashMap<String, String>();
	}

	/*
	 * Method used to add and store response headers
	 */
	public void addResponseHeaders(String key, String value) {
		if(responseHeaders.containsKey(key)) { //If key exists, append new value to the comma separated values of the header
			String headerValue = responseHeaders.get(key);
			headerValue += (", "+value);
			responseHeaders.put(key, headerValue);
		} else
			responseHeaders.put(key, value);
	}

	/*
	 * Method to fetch a particular response header value
	 */
	public String getResponseHeader(String key) {
		return responseHeaders.get(key);
	}

	/*
	 * Method to get all response headers
	 */
	public HashMap<String, String> getAllResponseHeaders() {
		return responseHeaders;
	}

	/*
	 * Method to fetch response status (i.e status code)
	 */
	public int getResponseStatusCode() {
		return responseStatusCode;
	}

	/*
	 * Method to fetch body of response as a string
	 */
	public String getBody() {
		return body;
	}

	/*
	 * Method parses the response into headers and body
	 */
	public void parseResponse() throws IOException {
		BufferedReader br = null;
		clearResponseHeaders();
		if(isHTTPS) { //https use a connection objects and handle headers and body separately
			Map<String, List<String>> headers = HTTPSconnection.getHeaderFields();
			for(String key: headers.keySet()) {
				for(String value: headers.get(key)) {
					addResponseHeaders(key, value);
				}
			}
			responseStatusCode = HTTPSconnection.getResponseCode();
			addResponseHeaders("Content-Type", HTTPSconnection.getContentType());
			if(getResponseHeader("Content-Length")==null)
				addResponseHeaders("Content-Length", ""+HTTPSconnection.getContentLength());
			if(connectionInputStream==null) //Input stream having body
				System.out.println("Connection null inside parseResponses");
			br = new BufferedReader(new InputStreamReader(connectionInputStream));
		} else { 
			Map<String, List<String>> headers = HTTPconnection.getHeaderFields();
			for(String key: headers.keySet()) {
				for(String value: headers.get(key)) {
					addResponseHeaders(key, value);
				}
			}
			responseStatusCode = HTTPconnection.getResponseCode();
			addResponseHeaders("Content-Type", HTTPconnection.getContentType());
			
			if(getResponseHeader("Content-Length")==null)
				addResponseHeaders("Content-Length", ""+HTTPconnection.getContentLength());
			if(connectionInputStream==null) //Input stream having body
				System.out.println("Connection null inside parseResponses");
			/*
			bodyInputStream = connectionInputStream;
			if(bodyInputStream==null)
				System.out.println("boyd in pustream null in parse response");
			 */
			br = new BufferedReader(new InputStreamReader(connectionInputStream));


		}

		//reading body
		StringBuffer bodyBuffer = new StringBuffer();
		String bodyLine = null;
		while((bodyLine = br.readLine())!=null) {
			bodyBuffer.append(bodyLine+'\n');
		}
		body = bodyBuffer.toString();
		
		
		bodyInputStream = new ByteArrayInputStream(body.getBytes()); //Create an InputStream of the body and return the same

		//Setting as HTML or not based on content-type
		String value = getResponseHeader("Content-Type");
		if(value == null)
			isHtml = true;
		else {
			if(value.contains("text/html"))
				isHtml = true;
		}
	}

	/*
	 * Method connects to the given host, makes a GET request, fetches the data and parses the response
	 * It returns an InputStream of the body that can be parsed 
	 */
	public InputStream makeGetRequest(String URL, int port, Map<String, String>  urlParams) {
		Socket socket = null;
		this.url = URL;

		//newly added
		if(URL.startsWith("https"))
			isHTTPS = true;
		else
			isHTTPS = false;
		if(!URL.startsWith("http"))
			this.url = "http://"+URL;

		try {
			//Add necessary parameters to URL and get request string
			if(urlParams!=null && !urlParams.isEmpty())
			{
				//Creating proper URL String
				StringBuffer buf = new StringBuffer(this.url);
				boolean first = true;
				for(String key: urlParams.keySet()) {
					if(first) {
						buf.append("?"+key.trim()+"="+urlParams.get(key).trim());
						first = false;
					} else {
						buf.append("&"+key.trim()+"="+urlParams.get(key).trim());
					}
				}
				this.url = buf.toString();
			}
			method = "GET";
			if(!isHTTPS) {
//				System.out.println(url);
				URL oracle = new URL(url);
				
				
				HTTPconnection = (HttpURLConnection)oracle.openConnection();
				//Write Headers
				HTTPconnection.setRequestMethod(method);
				for(Entry<String, String> entry: requestHeaders.entrySet()) {
					if(entry.getKey().equalsIgnoreCase("if-modified-since")) {
						//setIfModifiedSince(entry.getValue());
						continue;
					}
					HTTPconnection.setRequestProperty(entry.getKey(), entry.getValue());
				}
				HTTPconnection.setInstanceFollowRedirects(false);
				HTTPconnection.setDoOutput(false);
				//connectionOutputStream = HTTPconnection.getOutputStream(); //get output stream
				connectionInputStream = HTTPconnection.getInputStream(); //get input stream

				if(connectionInputStream==null)
					System.out.println("CONNECTION INPUT STREAM NULL");

			} else { //HTTPSUrlConnection used for https request
				URL oracle = new URL(url);
				HTTPSconnection = (HttpsURLConnection)oracle.openConnection();
				//Write Headers
				HTTPSconnection.setRequestMethod(method);
				for(Entry<String, String> entry: requestHeaders.entrySet()) {
					if(entry.getKey().equalsIgnoreCase("if-modified-since")) {
						setIfModifiedSince(entry.getValue());
						continue;
					}
					HTTPSconnection.setRequestProperty(entry.getKey(), entry.getValue());
				}
				HTTPSconnection.setInstanceFollowRedirects(false);
				HTTPSconnection.setDoOutput(false);
				connectionInputStream = HTTPSconnection.getInputStream(); //get input stream
				if(connectionInputStream==null)
					System.out.println("CONNECTION INPUT STREAM NULL");
			}
			parseResponse(); //Parsing the response, or fetched HTML/XML file
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(bodyInputStream==null)
				System.out.println("BODY INPUT STREAM: NULL");
			if(socket!=null && !socket.isClosed())
				try {
					socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			return bodyInputStream;
		}
	}
	public void setIfModifiedSince(String value) {


		SimpleDateFormat f = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
		Date d = null;
		try {
			d = f.parse(value);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long milliseconds = d.getTime();
		HTTPSconnection.setIfModifiedSince(milliseconds);
	}

	/*
	 * Method to make head request.
	 * Unlike the previous method it does not return an InputStream of the body
	 */
	public void makeHeadRequest(String URL, int port, Map<String, String>  urlParams) {
		Socket socket = null;
		this.url = URL;

		//newly added
		if(URL.startsWith("https"))
			isHTTPS = true;
		else
			isHTTPS = false;
		if(!URL.startsWith("http"))
			this.url = "http://"+URL;

		try {
			//Add necessary parameters to URL and get request string
			if(urlParams!=null && !urlParams.isEmpty())
			{
				//Creating proper URL String
				StringBuffer buf = new StringBuffer(this.url);
				boolean first = true;
				for(String key: urlParams.keySet()) {
					if(first) {
						buf.append("?"+key.trim()+"="+urlParams.get(key).trim());
						first = false;
					} else {
						buf.append("&"+key.trim()+"="+urlParams.get(key).trim());
					}
				}
				this.url = buf.toString();
			}
			method = "HEAD";
			if(!isHTTPS) {
				URL oracle = new URL(url);
				HTTPconnection = (HttpURLConnection)oracle.openConnection();
				//Write Headers
				HTTPconnection.setRequestMethod(method);
				for(Entry<String, String> entry: requestHeaders.entrySet()) {
					HTTPconnection.setRequestProperty(entry.getKey(), entry.getValue());
				}
				HTTPconnection.setInstanceFollowRedirects(false);
				HTTPconnection.setDoOutput(false);
				connectionInputStream = HTTPconnection.getInputStream(); //get input stream
				if(connectionInputStream==null)
					System.out.println("CONNECTION INPUT STREAM NULL");

			} else { //HTTPSUrlConnection used for https request
				URL oracle = new URL(url);
				HTTPSconnection = (HttpsURLConnection)oracle.openConnection();
				//Write Headers
				HTTPSconnection.setRequestMethod(method);
				for(Entry<String, String> entry: requestHeaders.entrySet()) {
					HTTPSconnection.setRequestProperty(entry.getKey(), entry.getValue());
				}
				HTTPSconnection.setInstanceFollowRedirects(false);
				HTTPSconnection.setDoOutput(false);
				connectionInputStream = HTTPSconnection.getInputStream(); //get input stream
				if(connectionInputStream==null)
					System.out.println("CONNECTION INPUT STREAM NULL");
			}
			parseResponse(); //Parsing the response, or fetched HTML/XML file
		} catch (UnknownHostException e) {
			//e.printStackTrace();
		} catch (IOException e) {
			//e.printStackTrace();
		} finally {
			if(bodyInputStream==null)
				//System.out.println("BODY INPUT STREAM: NULL");
			if(socket!=null && !socket.isClosed())
				try {
					socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	public InputStream makePostRequest(String URL, int port, String contentType, String body) {
		try {
			this.method = "POST";
			this.url = URL;
			this.host = getHost(URL);
			
			addRequestHeader("Content-Type", contentType); //details specific to request body
			addRequestHeader("Content-Length", ""+body.length());
			
			String requestString = body;
			

			//Open socket and do read and write
			URL oracle = new URL(url);
			HTTPconnection = (HttpURLConnection)oracle.openConnection();
			//Write Headers
			HTTPconnection.setRequestMethod(method);
			for(Entry<String, String> entry: requestHeaders.entrySet()) {
//				System.out.println("HTTPCLIET: REQUEST HEADER ADDED: "+entry.getKey()+" : "+entry.getValue());
				HTTPconnection.setRequestProperty(entry.getKey(), entry.getValue());
			}
//			System.out.println("HTTPCLIENT: REQUEST BODY SENT: "+requestString);
			HTTPconnection.setDoOutput(true);
			HTTPconnection.setDoInput(true);
			
			connectionOutputStream = HTTPconnection.getOutputStream();
			if(connectionOutputStream==null) {
				System.out.println("HTTPCLIENT: CONNECTION STREAM NULL");
			}
			
			//connectionOutputStream.write(requestString.getBytes());
			OutputStreamWriter writer  = new OutputStreamWriter(connectionOutputStream);
			writer.write(requestString);
			connectionOutputStream.flush();

			writer.close();
			connectionInputStream = HTTPconnection.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(connectionInputStream));
			String line = null;
			StringBuffer response = new StringBuffer(); 
			while((line = rd.readLine()) != null) {
				response.append(line);
				response.append('\n');
			}
			rd.close();

			parseResponse();

		} catch (Exception e) {

		} finally {
			return bodyInputStream;
		}
	}

}
