import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Client implements Watcher{

	public static final Logger LOG = LoggerFactory.getLogger(Client.class);
	
	String hostPort;
	ZooKeeper zk;
			
	@Override
	public void process(WatchedEvent e) {
		System.out.println(e);
	}
	
	public Client(String hostPort) {
		this.hostPort = hostPort;
	}
	
	void startZK(){
		try {
			zk = new ZooKeeper(hostPort, 15000, this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	void stopZK() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	String queueCommand(String command){
		while(true) {
			try {
				String name = zk.create("/tasks/task-", command.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				return name;
			} catch (NodeExistsException e) {
			} catch(KeeperException e) {
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String args[]) throws InterruptedException {
		Client c = new Client(args[0]);
		c.startZK();
		
		String name = c.queueCommand(args[1]);
		LOG.info("Created " + name);
	}
}
