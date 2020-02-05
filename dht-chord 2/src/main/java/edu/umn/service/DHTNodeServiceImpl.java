package edu.umn.service;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.umn.interfaces.DHTNode;
import edu.umn.interfaces.DHTNodePath;
import edu.umn.interfaces.DHTNodeService;
import edu.umn.interfaces.DHTNodeService.Client;
import edu.umn.util.HostUtility;

/**
 * @author gupta555
 * @author amuda005
 *
 */
public class DHTNodeServiceImpl implements DHTNodeService.Iface {
	public static DHTNode[] fingerTable = new DHTNode[HostUtility.m];
	public static DHTNode predecessor;

	public static Map<Long, Map<String, String>> keys = new HashMap<Long, Map<String, String>>();

	@Override
	public String getBook(String bookTitle, long debugFlag) throws TException {
		long bookHash = generateBookKey(bookTitle);
		//System.out.println("getBook called for key:" + bookHash);
		if (bookHash == -1) {
			System.out.println("Error occurred while generating book title hash");
			return null;
		}
		// findSuccessor and make get call here
		DHTNodePath dhtNodePath = findSuccessor(bookHash, null);
		System.out.println("getBook: successor for key:" + bookHash + " is " + dhtNodePath.getNode().getId());
		
		if (debugFlag == 1) System.out.println("getBook: Visited Nodes: "); // All the visited nodes will be printed now
		if (dhtNodePath.getNode().getId() == HostUtility.myId) {
			//System.out.println("getBook:  "+bookTitle+" CHECK IF YOU HAVE THE BOOK");
			if (debugFlag == 1) System.out.println("getBook: traversed "+HostUtility.myId);
			Map<String, String> bookTitles = keys.get(bookHash);
			if (bookTitles == null) {	
				System.out.println("getBook: "+bookTitle+" is NULL NOT FOUND !!!");
				return HostUtility.NACK;
			}
			if (bookTitles.containsKey(bookTitle)) {
				System.out.println("getBook: "+bookTitle+"  FOUND");
				return bookTitles.get(bookTitle);
			}			
			// send appropriate error message to return here
			System.out.println("getBook: "+bookTitle+" NOT FOUND !!!");
			return HostUtility.NACK;
		}
		
		int traversedSize = dhtNodePath.visitedNodes.size();
		int count = 0;
		
		for (Iterator<DHTNode> iterator = dhtNodePath.visitedNodes.iterator(); iterator.hasNext();) {
			DHTNode dhtNode = (DHTNode) iterator.next();
			count++;
			if (debugFlag == 1 && count != traversedSize ) {
				System.out.println("getBook: recursive successor traversed "+dhtNode.id+"\n");
			}
		}
		TTransport transport = new TSocket(dhtNodePath.getNode().getIp(), (int) dhtNodePath.getNode().getPort());
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
		transport.open();
		DHTNodeService.Client dClient = new DHTNodeService.Client(protocol);
		String bookGenre = dClient.getBook(bookTitle, debugFlag);
		transport.close();
		if (debugFlag == 1) System.out.println("getBook: final successor traversed "+dhtNodePath.getNode().getId());
		return bookGenre;
	}

	@Override
	public void setBook(String bookTitle, String genre, long debugFlag) throws TException {
		long bookHash = generateBookKey(bookTitle);
		//System.out.println("setBook called for key:" + bookHash);
		if (bookHash == -1) {
			System.out.println("Error occurred while generating book title hash");
			return;
		}
		if (debugFlag == 1) {
			System.out.println("setBook: Visited Nodes: ");
			System.out.println("setBook: traversed "+HostUtility.myId);
		}	
		Map<String, String> bookTitles;
		if (keys.containsKey(bookHash)) {
			bookTitles = keys.get(bookHash);
			bookTitles.put(bookTitle, genre);
			keys.put(bookHash, bookTitles);
			System.out.println("setBook: successor for key:" + bookHash + " is " + HostUtility.myId);
			return;
		} else {
			// findSuccessor and make set call here findSuccessor(bookHash, visitedNodes);
			DHTNodePath dhtNodePath = findSuccessor(bookHash, null);
			System.out.println("setBook: successor for key:" + bookHash + " is " + dhtNodePath.getNode().getId());
			if (dhtNodePath.getNode().getId() == HostUtility.myId) {
				bookTitles = new HashMap<String, String>();
				bookTitles.put(bookTitle, genre);
				keys.put(bookHash, bookTitles);
				return;
			}
			
			int traversedSize = dhtNodePath.visitedNodes.size();
			int count = 0;
			
			for (Iterator<DHTNode> iterator = dhtNodePath.visitedNodes.iterator(); iterator.hasNext();) {
				DHTNode dhtNode = (DHTNode) iterator.next();
				count++;
				if (debugFlag == 1 && count != traversedSize ) {
					System.out.println("setBook: recursive traversed successor "+dhtNode.id);
				}
			}
			TTransport transport = new TSocket(dhtNodePath.getNode().getIp(), (int) dhtNodePath.getNode().getPort());
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			transport.open();
			DHTNodeService.Client dClient = new DHTNodeService.Client(protocol);
			dClient.setBook(bookTitle, genre, debugFlag);
			transport.close();
			if (debugFlag == 1) System.out.println("setBook: final traversed successor "+ dhtNodePath.getNode().getId()+"\n");
		}
	}

	@Override
	public void updateFingerTable(DHTNode newNode, int i, List<DHTNode> visitedNodes) {
		if (visitedNodes == null) {
			visitedNodes = new ArrayList<DHTNode>();
		}
		if (newNode.getId() == HostUtility.myId)
			return;
		visitedNodes.add(HostUtility.myNode);
		//System.out.println("updateFingerTable: newNode =" + newNode + " i=" + i + ", Visited nodes = " + visitedNodes);

		if (checkLeftClosedInterval(HostUtility.myId, fingerTable[i].getId(), newNode.getId())) {
			fingerTable[i] = newNode;
			printFingerTable();
			TTransport transport = new TSocket(predecessor.getIp(), (int) predecessor.getPort());
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			try {
				transport.open();
				Client dhtNodeClient = new DHTNodeService.Client(protocol);
				// calling my predecessor to update
				dhtNodeClient.updateFingerTable(newNode, i, visitedNodes);
			} catch (TTransportException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			} finally {
				transport.close();
			}

		}
	}

	@Override
	public void setPredecessor(DHTNode predecessor) throws TException {
		System.out.println("Setting my predecessor as " + predecessor.getId());
		DHTNodeServiceImpl.predecessor = predecessor;
	}

	@Override
	public DHTNodePath findPredecessor(long key, List<DHTNode> visitedNodes) throws TException {
		//System.out.println("\nfindPredecessor: key=" + key + ", Visited nodes = " + visitedNodes);
		DHTNode start = new DHTNode(HostUtility.myNode);
		if (visitedNodes == null) {
			visitedNodes = new ArrayList<DHTNode>();
		}
		visitedNodes.add(HostUtility.myNode);
		// checking if i am the successor of the key. If so return my predecessor
		if (checkRightClosedInterval(predecessor.getId(), HostUtility.myId, key)) {
			//System.out.println(predecessor.getId() + " is predecessor of key " + key);
			return new DHTNodePath(predecessor, visitedNodes);
		}
		// check
		if (!(checkOpenInterval(start.id, fingerTable[0].getId(), key) || (key == fingerTable[0].getId()))) {
			for (int i = HostUtility.m - 1; i >= 0; i--) {
				if (checkOpenInterval(HostUtility.myId, key, fingerTable[i].getId())) {
					start = fingerTable[i];
					TTransport transport = new TSocket(start.getIp(), (int) start.getPort());
					TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
					Client dhClient = new Client(protocol);
					if (!transport.isOpen())
						transport.open();
					DHTNodePath predDhtNodePath = dhClient.findPredecessor(key, visitedNodes);
					transport.close();
					//System.out.println("findPredecessor: " + predDhtNodePath.getNode().getId() + " is predecessor of key " + key);
					return predDhtNodePath;
				}
			}
		}
		return new DHTNodePath(start, visitedNodes);
	}

	@Override
	public DHTNodePath findSuccessor(long key, List<DHTNode> visitedNodes) throws TException {
		//System.out.println("\nfindSuccessor : key=" + key + " Visited nodes = " + visitedNodes);
		if (predecessor.getId() == HostUtility.myId || key == HostUtility.myId
				|| checkOpenInterval(predecessor.getId(), HostUtility.myId, key)) {
			if (visitedNodes == null) {
				visitedNodes = new ArrayList<DHTNode>();
			}
			visitedNodes.add(HostUtility.myNode);
			DHTNodePath dhtNodePath = new DHTNodePath(HostUtility.myNode, visitedNodes);
			//System.out.println("findSuccessor: Found successor of " + key + " as " + dhtNodePath.getNode().getId());

			return dhtNodePath;
		}
		if (checkRightClosedInterval(HostUtility.myId, fingerTable[0].getId(), key)) {
			if (visitedNodes == null) {
				visitedNodes = new ArrayList<DHTNode>();
			}
			visitedNodes.add(HostUtility.myNode);
			DHTNodePath dhtNodePath = new DHTNodePath(fingerTable[0], visitedNodes);
			//System.out.println("findSuccessor: Found successor of " + key + " as " + dhtNodePath.getNode().getId());
			return dhtNodePath;
		}
		// Getting
		DHTNodePath pred = findPredecessor(key, visitedNodes);
		TTransport transport = new TSocket(pred.getNode().getIp(), (int) pred.getNode().getPort());
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
		Client dhClient = new Client(protocol);
		if (!transport.isOpen())
			transport.open();
		DHTNodePath successorPath = dhClient.findSuccessor(key, pred.visitedNodes);
		transport.close();
		//System.out.println("findSuccessor: Found successor of " + key + " as " + successorPath.getNode().getId());
		return successorPath;
	}

	public boolean checkOpenInterval(long m, long l, long key) {
		// find if element is in left open and right closed interval
		if (m > l) {
			if (key > m || key < l) {
				return true;
			}
		} else if (key > m && key < l) {
			return true;
		}
		return false;
	}

	public boolean checkLeftClosedInterval(long m, long l, long key) {
		if (m == l)
			return true;
		if (m > l) {
			if (key >= m || key < l)
				return true;
			else
				return false;
		}
		if (key >= m && key < l)
			return true;

		return false;
	}

	public boolean checkRightClosedInterval(long m, long l, long key) {
		if (m == l)
			return true;
		if (m > l) {
			if (key > m || key <= l)
				return true;
			else
				return false;
		}
		if (key > m && key <= l)
			return true;

		return false;
	}

	// Genearate a new node id
	public static long generateBookKey(String bookTitle) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] hashBytes = md.digest(bookTitle.getBytes(StandardCharsets.UTF_8));
			BigInteger hashNum = new BigInteger(1, hashBytes);
			BigInteger bookTitleBigInt = hashNum.mod(HostUtility.NUM_KEYS);
			return bookTitleBigInt.longValue();
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Error occured while Hashing the booktitle");
			e.printStackTrace();
		}
		return -1;
	}

	public static void printFingerTable() {
		System.out.println("\nFingerTable of node:" + HostUtility.myId);
		for (int i = 0; i < HostUtility.m; i++) {
			System.out.println("Finger " + i + " :" + fingerTable[i]);
		}
		System.out.println();
	}

	@Override
	public Map<String, String> transferKeys(long predecessorId) throws TException {
		System.out.println("transferKeys: to " + predecessorId);
		//System.out.println("keys " + keys.entrySet());
		Map<String, String> transferMap = new HashMap<String, String>();
		
		Iterator<Entry<Long, Map<String, String>>> iterator = keys.entrySet().iterator();
		
		while(iterator.hasNext()) {
			Entry<Long, Map<String, String>> entry = iterator.next();
			//System.out.println("transferKey: check key=" + entry.getKey());
			if (!checkRightClosedInterval(predecessorId, HostUtility.myId, entry.getKey())) {
				System.out.println("transferKeys: key " + entry.getKey() + " values " + entry.getValue());
				transferMap.putAll(entry.getValue());
				iterator.remove();
			}
		}
		//System.out.println("transferKeys: transferring map: " + transferMap.keySet());
		//System.out.println("transferKeys: keys left with me: "+keys.keySet());
		return transferMap;
	}

}
