/**
 * 
 */
package edu.umn.controller;

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import edu.umn.interfaces.SuperNodeService;
import edu.umn.service.SuperNodeServiceImpl;
import edu.umn.util.HostUtility;

/**
 * @author gupta555
 * @author amuda005
 *
 */
public class SuperNodeServer {
	public static void main(String[] args) {
		Runnable simple = new Runnable() {
            public void run() {
                simple(HostUtility.superNodePort);
            }
        };

        new Thread(simple).start();
	}
    public static void simple(int port) {
        try {
        	 //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(port);
            TTransportFactory factory = new TFramedTransport.Factory();
            SuperNodeService.Processor processor = new SuperNodeService.Processor(new SuperNodeServiceImpl());
            
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
            args.processor(processor);  //Set handler
            args.transportFactory(factory);  //Set FramedTransport (for performance)

            //Run server as multi-threaded
            TThreadPoolServer server = new TThreadPoolServer(args);
            System.out.println("Starting the SuperNode mulithreaded server...");
            server.serve();           
            System.out.println("... Server Reached End ...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
