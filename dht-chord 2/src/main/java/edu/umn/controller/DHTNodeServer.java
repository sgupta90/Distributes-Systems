package edu.umn.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import edu.umn.interfaces.DHTNode;
import edu.umn.interfaces.DHTNodePath;
import edu.umn.interfaces.DHTNodeService;
import edu.umn.interfaces.DHTNodeService.Client;
import edu.umn.service.DHTNodeLocalService;
import edu.umn.service.DHTNodeServiceImpl;
import edu.umn.util.HostUtility;

/**
 * @author gupta555
 * @author amuda005
 *
 */
public class DHTNodeServer {
	private static TThreadPoolServer server;

	private static void init() {
		TServerTransport superNodeTransport;
		try {
			superNodeTransport = new TServerSocket(HostUtility.myPort);
			TTransportFactory factory = new TFramedTransport.Factory();
			DHTNodeService.Processor processor = new DHTNodeService.Processor(new DHTNodeServiceImpl());
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(superNodeTransport);
			args.processor(processor); // Set handler
			args.transportFactory(factory); // Set FramedTransport (for performance)
			server = new TThreadPoolServer(args);
			Runnable simple = new Runnable() {
				public void run() {
					simple();
				}
			};
			new Thread(simple).start();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		init();
		try {
			while (!server.isServing()) {
				Thread.sleep(100); // to avoid busy waiting
			}
			// call super node to join
			System.out.println("Calling super node for joining");
			DHTNodeLocalService dhtNodeLocalService = new DHTNodeLocalService();
			DHTNode nodeFromDHT = dhtNodeLocalService.joinDHT();
			// update DHT
			checkAndUpdate(nodeFromDHT);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	// Function to Update Self-FingerTable, Predecessor, Successor and predecessor
	// of your successor
	private static void checkAndUpdate(DHTNode nodeFromSuper) {
		System.out.println("Received random node from supernode " + nodeFromSuper.id);
		// check if nodeFromSuper is my node (I am the only node in the system)
		if (nodeFromSuper.getId() == HostUtility.myId) {
			// set full fingertable here
			for (int i = 0; i < HostUtility.m; i++) {
				DHTNodeServiceImpl.fingerTable[i] = nodeFromSuper;
			}
			// Set your Predecessor
			DHTNodeServiceImpl.predecessor = nodeFromSuper;
			DHTNodeServiceImpl.printFingerTable(); // Print finger table

			DHTNodeLocalService dhtNodeLocalService = new DHTNodeLocalService();
			dhtNodeLocalService.postJoin();
			return;
		}
		TTransport transport = null;
		try {
			// set up transport layer to call nodeFromSuper
			transport = new TSocket(nodeFromSuper.getIp(), (int) nodeFromSuper.getPort());
			// Get your Successor
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			Client dhtNodeClient = new DHTNodeService.Client(protocol);
			transport.open();
			DHTNodePath successorPath = dhtNodeClient.findSuccessor(HostUtility.myId, null); //  check for myid
			DHTNodeServiceImpl.fingerTable[0] = successorPath.getNode();
			System.out.println("Received successor " + successorPath.getNode().getId());
			transport.close();

			// Open connection with successor
			transport = new TSocket(successorPath.getNode().getIp(), (int) successorPath.getNode().getPort());
			protocol = new TBinaryProtocol(new TFramedTransport(transport));
			if (!transport.isOpen())
				transport.open();
			dhtNodeClient = new DHTNodeService.Client(protocol);

			// finding my predecessor
			DHTNodeServiceImpl.predecessor = dhtNodeClient.findPredecessor(successorPath.getNode().getId(), null)
					.getNode();
			System.out.println("checkAndUpdate: My predecessor: " + DHTNodeServiceImpl.predecessor);
			// Set myself as predecessor of my successor
			if (!transport.isOpen())
				transport.open();
			dhtNodeClient.setPredecessor(HostUtility.myNode);

			// update entries corresponding to successor
			long key;
			System.out.println("checkAndUpdate: Making my finger table " + HostUtility.myId);
			for (int i = 0; i < HostUtility.m - 1; i++) {
				key = (long) ((HostUtility.myId + Math.pow(2, i + 1)) % (HostUtility.NUM_KEYS.doubleValue()));
				if (key == DHTNodeServiceImpl.predecessor.getId()) {
					// if key is my predecessor
					//System.out.println("checkAndUpdate: key == predecessor " + key);
					DHTNodeServiceImpl.fingerTable[i + 1] = DHTNodeServiceImpl.predecessor;
				} else if (checkRightClosedInt(DHTNodeServiceImpl.predecessor.getId(), HostUtility.myId, key)) {
					// if I am successor of the key
					//System.out.println("checkAndUpdate: key == (predecessor,myId] " + key);
					DHTNodeServiceImpl.fingerTable[i + 1] = HostUtility.myNode;
				} else if (checkLeftClosedInt(HostUtility.myId, DHTNodeServiceImpl.fingerTable[i].getId(), key)) {
					// if previous entry of finger table is the successor
					//System.out.println("checkAndUpdate: key == [myId, fingerTable[index-1]=" + DHTNodeServiceImpl.fingerTable[i] + " key:" + key);
					DHTNodeServiceImpl.fingerTable[i + 1] = DHTNodeServiceImpl.fingerTable[i];
				} else {
					List<DHTNode> visitedNodes = new ArrayList<DHTNode>();
					visitedNodes.add(HostUtility.myNode);

					//System.out.println("checkAndUpdate: calling node " + successorPath.getNode().getId() + " to get key " + key);
					if (!transport.isOpen())
						transport.open();
					successorPath = dhtNodeClient.findSuccessor(key, visitedNodes);
					DHTNodeServiceImpl.fingerTable[i + 1] = successorPath.getNode();
				}
				//System.out.println("checkAndUpdate: finger[" + i + 1 + "]=" + DHTNodeServiceImpl.fingerTable[i + 1]);
			}
			DHTNodeServiceImpl.printFingerTable();
			updateOthers();
			Map<String, String> transferKeys = dhtNodeClient.transferKeys(HostUtility.myId);
			System.out.println("checkAndUpdate: Received keys from successor "+transferKeys.entrySet());
			for (Entry<String, String> entry : transferKeys.entrySet()) {
				Long hashTitle = DHTNodeServiceImpl.generateBookKey(entry.getKey());
				Map<String, String> existingKeys;
				if (DHTNodeServiceImpl.keys.containsKey(hashTitle)) {
					existingKeys = DHTNodeServiceImpl.keys.get(hashTitle);
				} else {
					existingKeys = new HashMap<String, String>();
				}
				existingKeys.put(entry.getKey(), entry.getValue());
				DHTNodeServiceImpl.keys.put(hashTitle, existingKeys);
			}
			DHTNodeLocalService dhtNodeLocalService = new DHTNodeLocalService();
			dhtNodeLocalService.postJoin();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			transport.close();
		}
	}

	private static boolean checkRightClosedInt(long m, long l, long key) {
		// find if element is in left open and right closed interval
		if (m == l)
			return true;
		if (m > l) {
			if (key > m || key <= l) {
				return true;
			}
		} else if (key > m && key <= l) {
			return true;
		}
		return false;
	}

	private static void updateOthers() {
		System.out.println("\nupdateOthers: init");
		long key;
		DHTNodePath predecessorPath;
		for (int i = 0; i < HostUtility.m; i++) {
			key = (long) ((HostUtility.myId - (long) Math.pow(2, i) + 1 + Math.pow(2, HostUtility.m)) // adding +1 to avoid 56 57
					% (Math.pow(2, HostUtility.m)));
			//System.out.println("\nupdateOthers: Update predecessors of key " + key);
			DHTNodeServiceImpl dhtNodeServiceImpl = new DHTNodeServiceImpl();
			try {
				predecessorPath = dhtNodeServiceImpl.findPredecessor(key, null);
				//System.out.println("udpateOthers: Received predecessor of key " + key + " as predecessor: " + predecessorPath.node.id);
				if (predecessorPath.getNode().getId() == HostUtility.myId) {
					continue;
				}
				TTransport transport = new TSocket(predecessorPath.getNode().getIp(),
						(int) predecessorPath.getNode().getPort());
				TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
				Client dhtNodeClient = new DHTNodeService.Client(protocol);
				transport.open();
				dhtNodeClient.updateFingerTable(HostUtility.myNode, i, null);
			} catch (TException e) {
				e.printStackTrace();
			}

		}
	}

	private static boolean checkLeftClosedInt(long m, long l, long key) {
		// find if element is in left open and right closed interval
		if (m == l)
			return true;
		if (m > l) {
			if (key >= m || key < l) {
				return true;
			}
		} else if (key >= m && key < l) {
			return true;
		}
		return false;
	}

	public static void simple() {
		try {
			// Run server as multi-threaded
			System.out.println("Starting the DHTNode mulithreaded server...");
			server.serve();
			System.out.println("... Server Reached End ...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
