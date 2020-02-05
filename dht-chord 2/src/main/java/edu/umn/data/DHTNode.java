package edu.umn.data;

/**
 * @author gupta555
 * @author amuda005
 *
 */

public class DHTNode {

	private String ip;
	private int port;
	private Integer id;

	public DHTNode() {
	}

	public DHTNode(String ip, int port, Integer id) {
		this.ip = ip;
		this.port = port;
		this.id = id;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "Machine [ip=" + ip + ", port=" + port + ", id=" + id + "]";
	}

}
