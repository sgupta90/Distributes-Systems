package edu.umn.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import edu.umn.interfaces.DHTNode;

public class HostUtility {
	public static int myId;
	public static int m;
	public static BigInteger NUM_KEYS;
	public static String superNodeIP;
	public static int superNodePort;
	public final static String NACK = "NACK";
	public static String myHostname;
	public static int myPort;
	public static DHTNode myNode;
	private static String configFile;
	private static Properties properties;

	static {
		try {
			configFile = System.getProperty("configFile");
			System.out.println("Loaded configFile: "+configFile);
			properties = new Properties();
			properties.load(new FileInputStream(configFile));

			// Loading Properties from file
			m = Integer.parseInt(properties.getProperty("m"));
			NUM_KEYS = BigInteger.valueOf((long) Math.pow(2, m));
			superNodeIP = properties.getProperty("superNodeIP");
			superNodePort = Integer.parseInt(properties.getProperty("superNodePort"));

			myHostname = InetAddress.getLocalHost().getHostName();
			myNode = new DHTNode();
			
			String myPortStr = System.getProperty("port");
			if ( myPortStr !=null) {
				myPort = Integer.parseInt(System.getProperty("port"));
			}	
			myNode.setIp(myHostname);
			myNode.setPort(myPort);
		} catch (UnknownHostException e) {
			System.out.println("Can't get my host");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
