import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
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


public class AdminClient implements Watcher{

	public static final Logger LOG = LoggerFactory.getLogger(AdminClient.class);
	
	String hostPort;
	ZooKeeper zk;
			
	@Override
	public void process(WatchedEvent e) {
		System.out.println(e);
	}
	
	public AdminClient(String hostPort) {
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
	
	void listState() throws KeeperException{
		try {
			Stat stat = new Stat();
			byte[] masterData = zk.getData("/master", false, stat);
			Date startDate = new Date(stat.getCtime()); 
			LOG.info("Master : " + new String(masterData) + " since " + startDate.toString());
		}catch (NoNodeException e) {
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {
			ArrayList<String> workers = (ArrayList<String>) zk.getChildren("/workers", false);
			for(String worker : workers) {
				byte[] data = zk.getData("/workers/" + worker,false, null);
				String state = new String(data);
				LOG.info("Worker - " + worker + " " + state);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			ArrayList<String> tasks = (ArrayList<String>) zk.getChildren("/tasks", false);
			for(String task : tasks) {
				LOG.info("Task - " + task);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static void main(String args[]) throws InterruptedException {
		AdminClient ac = new AdminClient(args[0]);
		ac.startZK();
		try {
			ac.listState();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
