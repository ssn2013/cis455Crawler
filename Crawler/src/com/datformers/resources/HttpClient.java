package com.datformers.resources;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.HttpsURLConnection;

import org.w3c.dom.Document;

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
	private Socket socket; 
	private HttpsURLConnection connection = null;
	
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
		if(isHTTPS) { //https use a connection objects and handle headers and body separately
			Map<String, List<String>> headers = connection.getHeaderFields();
			for(String key: headers.keySet()) {
				for(String value: headers.get(key)) {
					addResponseHeaders(key, value);
				}
			}
			responseStatusCode = connection.getResponseCode();
			if(connectionInputStream==null) //Input stream having body
				System.out.println("Connection null inside parseResponses");
			/*
			bodyInputStream = connectionInputStream;
			if(bodyInputStream==null)
				System.out.println("boyd in pustream null in parse response");
				*/
			br = new BufferedReader(new InputStreamReader(connectionInputStream));
		} else { //For http calls
			br = new BufferedReader(new InputStreamReader(connectionInputStream));
			String line = null;
			while((line = br.readLine())!=null ) { //Read line by line and parse response
				if(line.equals("")) //End of header portion, marked with a new line
					break;
				if(line.contains(":")) { //Headers (headers contain :)
					String elements[] = line.split(":"); //splitting of header to key and value
					addResponseHeaders(elements[0].trim(), elements[1].trim());
				} else {
					String elements[] = line.split(" "); //First line of request
					responseStatusCode = Integer.parseInt(elements[1]); //Parsing and storing response status
				}
			}
			

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
		//System.out.println("IN PARSE RESPONSE: URL: "+url+" Content-type:-"+value);
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
		PrintWriter outWriter = null;
		Socket socket = null;
		this.url = URL;
		
		//newly added
		String host = getHost(URL);
		if(URL.startsWith("https"))
			isHTTPS = true;
		else
			isHTTPS = false;
		
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
			
			if(!isHTTPS) {
				socket = new Socket(host, port);
				if(port==80 && socket==null)
					socket = new Socket(host, 8080); //if connection to port 80 fails, try 8080
				connectionOutputStream = socket.getOutputStream();
				connectionInputStream = socket.getInputStream();
				outWriter = new PrintWriter(connectionOutputStream, true);
				method = "GET";
				 //Request as a string
				String requestString = getRequestString();
				outWriter.println(requestString); //making the request
				outWriter.flush();
			} else { //HTTPSUrlConnection used for https request
				URL oracle = new URL(url);
				connection = (HttpsURLConnection)oracle.openConnection();
				//Write Headers
				for(Entry<String, String> entry: requestHeaders.entrySet()) {
					connection.addRequestProperty(entry.getKey(), entry.getValue());
				}
				connection.setDoOutput(true);
				connectionOutputStream = connection.getOutputStream(); //get output stream
				connectionInputStream = connection.getInputStream(); //get input stream
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
	
	/*
	 * Method to make head request.
	 * Unlike the previous method it does not return an InputStream of the body
	 */
	public void makeHeadRequest(String URL, int port, Map<String, String>  urlParams) {
		PrintWriter outWriter = null;
		Socket socket = null;
		this.url = URL;
		
		//newly added
		String host = getHost(URL);
		if(URL.startsWith("https"))
			isHTTPS = true;
		else
			isHTTPS = false;
		
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
			
			if(!isHTTPS) {
				socket = new Socket(host, port);
				if(port==80 && socket==null)
					socket = new Socket(host, 8080); //if connection to port 80 fails, try 8080
				connectionOutputStream = socket.getOutputStream();
				connectionInputStream = socket.getInputStream();
				outWriter = new PrintWriter(connectionOutputStream, true);
				method = "HEAD";
				 //Request as a string
				String requestString = getRequestString();
				outWriter.println(requestString); //making the request
				outWriter.flush();
			} else { //HTTPSUrlConnection used for https request
				URL oracle = new URL(url);
				connection = (HttpsURLConnection)oracle.openConnection();
				//Write Headers
				for(Entry<String, String> entry: requestHeaders.entrySet()) {
					connection.addRequestProperty(entry.getKey(), entry.getValue());
				}
				connection.setDoOutput(true);
				connectionOutputStream = connection.getOutputStream(); //get output stream
				connectionInputStream = connection.getInputStream(); //get input stream
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
		}
	}
	
	public InputStream makePostRequest(String URL, int port, String contentType, String body) {
		try {
			this.method = "POST";
			this.url = URL;
			this.host = getHost(URL);
			addRequestHeader("Content-Type", contentType); //details specific to request body
			addRequestHeader("Content-Length", ""+body.length());
			String requestString = getRequestString();
			requestString += '\n'+body;
			
			//Open socket and do read and write
			Socket socket = new Socket(this.host.split(":")[0].trim(), port);
			connectionOutputStream = socket.getOutputStream();
			connectionInputStream = socket.getInputStream();
			connectionOutputStream.write(requestString.getBytes());
			connectionOutputStream.flush();
			parseResponse();
			//System.out.println("In HttpClient:makePostRequest: Request made: "+requestString+" Response: "+responseCode);
		} catch (Exception e) {

		} finally {
			return bodyInputStream;
		}
	}

}
