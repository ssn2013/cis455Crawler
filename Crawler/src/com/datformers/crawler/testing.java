package com.datformers.crawler;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class testing {
	 public static void main(String[] args) {
		 

			    String os = System.getProperty("os.name").toLowerCase();

			    if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0) {   
			        NetworkInterface ni = null;
					try {
						ni = NetworkInterface.getByName("wlan0");
					} catch (SocketException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

			        Enumeration<InetAddress> ias = ni.getInetAddresses();

			        InetAddress iaddress;
			        do {
			            iaddress = ias.nextElement();
			        } while(!(iaddress instanceof Inet4Address));

			        System.out.println(iaddress);
			    }

			    try {
					System.out.println(InetAddress.getLocalHost());
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  // for Windows and OS X it should work well
			}
}