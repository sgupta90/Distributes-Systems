/**
 * 
 */
package edu.umn.service;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;

import edu.umn.data.DHTNode;
import edu.umn.interfaces.SuperNodeService;
import edu.umn.util.HostUtility;

/**
 * @author gupta555
 * @author amuda005
 *
 */
public class SuperNodeServiceImpl implements SuperNodeService.Iface {

	private static AtomicBoolean isBusy = new AtomicBoolean(false);
	private static ConcurrentHashMap<Integer, DHTNode> nodeInfo = new ConcurrentHashMap<Integer, DHTNode>();
	private static DHTNode currentMachine = new DHTNode();

	@Override
	public String join(String ip, int port) throws TException {
		System.out.format("Received call for joining from host:%s, port:%d \n", ip, port);
		// Check if their existing node
		if (!isBusy.compareAndSet(false, true)) {
			return HostUtility.NACK;
		}

		Integer currentNodeId = generateNodeId(ip, port); // get new id for current node

		currentMachine.setId(currentNodeId); // Setting up the current node joining dht
		currentMachine.setIp(ip);
		currentMachine.setPort(port);

		String parentIp;
		int parentPort;
		int parentNodeId;

		if (!isMapEmpty()) {
			DHTNode parentDhtNode = getRandMachineFromMap();
			System.out.println("Parent Machine " + parentDhtNode);
			
			parentIp = parentDhtNode.getIp();
			parentPort = parentDhtNode.getPort();
			parentNodeId = parentDhtNode.getId();
		} else {
			parentIp = ip;
			parentPort = port;
			parentNodeId = currentNodeId;
		}

		String response = currentNodeId + "," + parentIp +"," + parentPort+ "," + parentNodeId;
		System.out.format("Returning response %s for the host %s\n", response, ip);

		return response;
	}

	@Override
	public void postJoin(String ip, int port) throws TException {
		System.out.format("Received call for postjoin from host:%s, port:%d \n", ip, port);
		// Current node has joined the DHT, adding it to the Node Map
		// checking if postjoin is called by correct node
		if (currentMachine.getIp().equals(ip) && currentMachine.getPort() == port) {
			System.out.println("Correct Machine called postjoin: " + ip +":"+ port);
			addKeyInMap();
		}
		
		// Allowing Other Nodes to join the DHT
		boolean releasedLock = isBusy.compareAndSet(true, false);
		System.out.println("Post Join Released Lock " + releasedLock);
	}

	@Override
	public String getNode() throws TException {
		if (isBusy.get()) {
			return HostUtility.NACK;
		}
		if (isMapEmpty()) {
			return HostUtility.NACK;
		}
		DHTNode machine = getRandMachineFromMap();
		String nodeIpAndPort = machine.getIp() + "," + machine.getPort();
		return nodeIpAndPort;
	}

	// Genearate a new node id
	private int generateNodeId(String ip, int port) {
		int nodeId = 0;
		try {
			String key = ip + port;
			MessageDigest md = null;
			boolean findKey = true;
			md = MessageDigest.getInstance("MD5");
			while (findKey) {
				byte[] hashBytes = md.digest(key.getBytes(StandardCharsets.UTF_8));
				BigInteger hashNum = new BigInteger(1, hashBytes);
				BigInteger nodeIdBigInt = hashNum.mod(HostUtility.NUM_KEYS);
				nodeId = nodeIdBigInt.intValue();
				if (!findKeyInMap(nodeId)) {
					findKey = false;
				} else {
					key = hashNum.toString();
				}
			}
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Error occured while Hashing the hostname and port");
			e.printStackTrace();
		}

		return nodeId;
	}
	

	private synchronized boolean findKeyInMap(Integer nodeId) {
		return nodeInfo.containsKey(nodeId);
	}

	private synchronized boolean isMapEmpty() {
		return nodeInfo.isEmpty();
	}

	// Function to Return a Random Node to join
	private synchronized DHTNode getRandMachineFromMap() {
		List<Integer> keysAsArray = new ArrayList<Integer>(nodeInfo.keySet());
		Random rand = new Random();
		Integer nodeId = keysAsArray.get(rand.nextInt(keysAsArray.size()));
		return nodeInfo.get(nodeId);
	}

	private synchronized void addKeyInMap() {
		DHTNode dhtNode = new DHTNode(currentMachine.getIp(), currentMachine.getPort(), currentMachine.getId());
		Integer dhtNodeId = currentMachine.getId();
		nodeInfo.put(dhtNodeId, dhtNode);
		
		System.out.println("Current Nodes in the system");
		for (Map.Entry<Integer, DHTNode> entry:nodeInfo.entrySet()) {
			System.out.println("key:"+entry.getKey()+": "+entry.getValue());
		}
	}
}
