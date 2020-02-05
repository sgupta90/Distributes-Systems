package edu.umn.controller;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.umn.interfaces.DHTNodeService;
import edu.umn.interfaces.SuperNodeService;
import edu.umn.util.HostUtility;

/**
 * @author gupta555
 * @author amuda005
 *
 */
public class Client {
	
	static TTransport transport;
	static TProtocol protocol;

	public static void main(String[] args) {
		//get supernode from configuration
		transport = new TSocket(HostUtility.superNodeIP, HostUtility.superNodePort);
		protocol = new TBinaryProtocol(new TFramedTransport(transport));
		try {
			SuperNodeService.Client superNodeClient = new SuperNodeService.Client(protocol);
			String nodeAddress = HostUtility.NACK;
			transport.open();

			while (nodeAddress.equals(HostUtility.NACK)) {
				nodeAddress = superNodeClient.getNode();
				System.out.println("Node address " + nodeAddress);

				if (nodeAddress.equals(HostUtility.NACK)) {
					System.out.println("Sleeping as received NACK");
					Thread.sleep(3000);
				}
			}
			transport.close();
			// Connecting to the DHTnode
			String[] nodeIPAndPort = nodeAddress.split(",");
			String nodeIP = nodeIPAndPort[0];
			int nodePort = Integer.parseInt(nodeIPAndPort[1]);

			transport = new TSocket(nodeIP, nodePort);
			protocol = new TBinaryProtocol(new TFramedTransport(transport));
			loadBooks();

		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private static void loadBooks() throws TException {
		int n = 0;
		long debugFlag = 1; //TODO Take as user input
		System.out.println("Debugging Enabled in system by default: Please Enter 0 to disable and 1 to enable");
		Scanner input = new Scanner(System.in);
		
		debugFlag = input.nextLong();
		input.nextLine(); //TODO scanning extra because of new line after nextInt
		
		while (true) {
			System.out.println(
					"\nEnter 1 - for setting bookTitle & genre \n\t  2 - for getting genre of a book title \n\t 3 - Exit\n");
			if (input.hasNext()) {
				n = input.nextInt();
				switch (n) {
				case 1:
					System.out.println("Enter bookTitle & Genre with ':' as a seperator, if you want to use a file then input FILE_PATH:Absolute file path\n");
					while (!input.hasNext());
					String line = input.nextLine(); //TODO scanning extra because of new line after nextInt
					
					line=input.nextLine();
					System.out.println("Received Input "+line);
					String[] split = line.split(":");
					
					String bookTitle;
					String genre;
					
					if(!transport.isOpen()) transport.open();
					DHTNodeService.Client dhtNodeClient = new DHTNodeService.Client(protocol);
					
					// Setting book title and Genre
					if (split[0].equals("FILE_PATH")) {
						String filePath = split[1];
						BufferedReader bufferedReader;
						try {
							bufferedReader = new BufferedReader(new FileReader(filePath));
							String readLine = bufferedReader.readLine();
							while(readLine != null) {
								System.out.println("setBook: "+ readLine);
								String[] readLineSplit = readLine.split(":");
								bookTitle = readLineSplit[0];
								genre = readLineSplit[1];
								dhtNodeClient.setBook(bookTitle, genre, debugFlag);
								readLine = bufferedReader.readLine();
							}
							System.out.println("setBook successful using file");
						} catch (IOException e) {
							e.printStackTrace();
							System.out.println("Please Provide the valid FILE_PATH");
						}
					}
					else {
						bookTitle = split[0];
						genre = split[1];
						dhtNodeClient.setBook(bookTitle, genre, debugFlag);
					}					
					transport.close();
					System.out.println("setBook completed successfully");
					break;
				case 2:
					System.out.println("Enter bookTitle to fetch");
					if(!transport.isOpen()) transport.open();
					DHTNodeService.Client dhtNodeClient1 = new DHTNodeService.Client(protocol);
					while (!input.hasNext());
					bookTitle = input.nextLine();
					bookTitle=input.nextLine();
					System.out.println("title ="+bookTitle);
					String genreResponse = dhtNodeClient1.getBook(bookTitle, debugFlag);
					// getting the book genre
					if (genreResponse.equals(HostUtility.NACK)) {
						System.out.println("getBook: book:" + bookTitle + ", BOOK NOT FOUND !!");
					}
					else {
						System.out.println("getBook: book:" + bookTitle + ", genre:" + genreResponse);
					}
					break;
				case 3:
					return;
				}
			}

		}

	}

}
