package edu.umn.service;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.umn.interfaces.DHTNode;
import edu.umn.interfaces.SuperNodeService;
import edu.umn.util.HostUtility;

public class DHTNodeLocalService {
	private SuperNodeService.Client superNodeClient;
	private TTransport transport;
	private DHTNode dhtNode;
	public DHTNodeLocalService() {
		transport = new TSocket(HostUtility.superNodeIP, HostUtility.superNodePort);
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
		superNodeClient = new SuperNodeService.Client(protocol);
	}

	public DHTNode joinDHT() {
		String joinResponse = HostUtility.NACK;
		try {
			while (joinResponse.equals(HostUtility.NACK)) {
				if(!transport.isOpen())	transport.open();
				joinResponse = superNodeClient.join(HostUtility.myHostname, HostUtility.myPort);
				if (joinResponse.equals(HostUtility.NACK)) {
					System.out.println("Sleeping as received NACK");
					Thread.sleep(3000);
				}
			}
			String[] split = joinResponse.split(",");
			HostUtility.myId = Integer.parseInt(split[0]);
			HostUtility.myNode.setId(HostUtility.myId);
			dhtNode = new DHTNode(split[1],Integer.parseInt(split[2]),Integer.parseInt(split[3]));
			System.out.println("Response receive from superNode: id: "+HostUtility.myId+" random Node "+dhtNode.toString());

		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			transport.close();
		}
		return dhtNode;
	}
	
	public void postJoin() {
		try {
			transport.open();
			superNodeClient.postJoin(HostUtility.myHostname, HostUtility.myPort);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			transport.close();
		}
	}
}
