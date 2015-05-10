package edu.upenn.cis455.mapreduce.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Class to handle all HTTP requests and their responses
 */
public class HttpClient {
	private Map<String, String> requestHeaders = new HashMap<String, String>();
	private Map<String, String> responseHeaders = new HashMap<String, String>();
	private InputStream inputStream = null;
	private OutputStream outputStream = null;
	private InputStream responseBodyStream = null;
	private int responseCode;
	private String method;
	private String URL;
	private String hostName;
	public String getRequestHeader(String key) {
		return requestHeaders.get(key);
	}
	public String getResponseHeader(String key) {
		return responseHeaders.get(key);
	}
	public int getResponseCode() {
		return responseCode;
	}
	public void addRequestHeader(String key, String value) { //add request header
		if(requestHeaders.containsKey(key)) {
			String valueNew = requestHeaders.get(key)+", "+value.trim();
			requestHeaders.put(key.trim(), valueNew);
		} else {
			requestHeaders.put(key.trim(), value.trim());
		}
	}
	public void addResponseHeader(String key, String value) { //add response header (called while parsing response)
		if(responseHeaders.containsKey(key)) {
			String valueNew = responseHeaders.get(key)+", "+value.trim();
			responseHeaders.put(key.trim(), valueNew);
		} else {
			responseHeaders.put(key.trim(), value.trim());
		}
	}
	
	/*
	 * Method returns an HTTP request (string) of the headers. For POST requests the body has to be appended to this string
	 */
	private String getRequestHeadersString() {
		StringBuffer buf = new StringBuffer();
		buf.append(method+" "+URL+" HTTP/1.1\n");
		buf.append("Host: "+hostName+'\n');
		buf.append("Connection: close"+'\n'); 
		for(String key: requestHeaders.keySet()) {
			buf.append(key+": "+requestHeaders.get(key)+"\n");
		}
		return buf.toString();
	}
	/*
	 * Method to parse the response
	 */
	private void parseResponse() {
		//parse set of headers
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			while((line = br.readLine())!=null ) { //Read line by line and parse response
				if(line.equals("")) //End of header portion, marked with a new line
					break;
				if(line.contains(":")) { //Headers (headers contain :)
					String elements[] = line.split(":"); //splitting of header to key and value
					addResponseHeader(elements[0].trim(), elements[1].trim());
				} else {
					String elements[] = line.split(" "); //First line of request
					responseCode = Integer.parseInt(elements[1]); //Parsing and storing response status
				}
			}

			//reading body
			StringBuffer bodyBuffer = new StringBuffer();
			String bodyLine = null;
			while((bodyLine = br.readLine())!=null) {
				bodyBuffer.append(bodyLine+'\n');
			}
			String body = bodyBuffer.toString();
			if(body!=null && body.length()>0)
				responseBodyStream = new ByteArrayInputStream(body.getBytes()); //Create an InputStream of the body and return the same
			else
				responseBodyStream = null;
		} catch(IOException ie) {
			System.out.println("Parsing Response got exception: "+ie.getMessage());
		}

	}
	/*
	 * Method to make all requests except POST. The method takes the URL, port and parametes (as a map)
	 */
	public InputStream makeRequest(String URL, int port, Map<String, String>  urlParams) {
		try {
			this.method = "GET";
			this.URL = URL;
			this.hostName = extractHostName(); //Extract host name from URL

			//Creating proper URL String
			StringBuffer buf = new StringBuffer(this.URL);
			boolean first = true;
			for(String key: urlParams.keySet()) {
				if(first) {
					buf.append("?"+key.trim()+"="+urlParams.get(key).trim());
					first = false;
				} else {
					buf.append("&"+key.trim()+"="+urlParams.get(key).trim());
				}
			}
			this.URL = buf.toString();

			String requestString = getRequestHeadersString()+'\n'; //Form the request headers

			//OPen socket 
			Socket socket = new Socket(this.hostName.split(":")[0].trim(), port);
			outputStream = socket.getOutputStream();
			inputStream = socket.getInputStream();
			outputStream.write(requestString.getBytes()); //write request
			outputStream.flush();
			parseResponse();
		} catch (Exception e) {

		} finally {
			return responseBodyStream;
		}
	}
	/*
	 * Method to handle POST requests and their responses. It does the same functionality as the above method with the additional work of handling 
	 * the body
	 */
	public InputStream makePostRequest(String URL, int port, String contentType, String body) {
		try {
			this.method = "POST";
			this.URL = URL;
			this.hostName = extractHostName();
			addRequestHeader("Content-Type", contentType); //details specific to request body
			addRequestHeader("Content-Length", ""+body.length());
			String requestString = getRequestHeadersString();
			requestString += '\n'+body;

			//Open socket and do read and write
			Socket socket = new Socket(this.hostName.split(":")[0].trim(), port);
			outputStream = socket.getOutputStream();
			inputStream = socket.getInputStream();
			outputStream.write(requestString.getBytes());
			outputStream.flush();
			parseResponse();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			return responseBodyStream;
		}
	}
	/*
	 * Method to extract hostname from a URL
	 */
	private String extractHostName() {
		String host = URL.substring(URL.indexOf('/')+2); //remove protocol part
		if(host.contains("/"))
			host = host.substring(0, host.indexOf('/'));//remove any additional paths 
		return host;

	}
}
