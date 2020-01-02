import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Worker implements Watcher{

	public static final Logger LOG = LoggerFactory.getLogger(Worker.class);
	
	String hostPort;
	ZooKeeper zk;
	private String serverId;
	private String status;
	private String name;
	
			
	@Override
	public void process(WatchedEvent e) {
		System.out.println(e);
	}
	
	public Worker(String hostPort) {
		this.hostPort = hostPort;
		Random random = new Random();
		serverId = Integer.toString(random.nextInt());
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
	
	void register() {
		zk.create("/workers/worker-"+serverId, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
	}
	
	StringCallback createWorkerCallback = new StringCallback() {
		
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				register();	
				break;
			case OK:
				LOG.info("Registered successfully - " + serverId );
				setName(name);
				break;
			case NODEEXISTS:
				LOG.warn("Already registered - " + serverId);
				break;
			default:
				LOG.error("DANG it !! something went wrong when registering worker - " + serverId);	
				break;
			}
		}
	};
	
	StatCallback statusUpdateCallback = new StatCallback() {
		
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				updateStatus((String)ctx);
			default:
				break;
			}
		}

	};


	synchronized private void updateStatus(String status) {
		if(status.equals(this.status)) {
			zk.setData("/workers/" + name, status.getBytes(), -1,	statusUpdateCallback, status);
		}
		
	}
	
	void setStatus(String status){
		this.status = status;
		updateStatus(status);
	}
	
	void setName(String name) {
		this.name = name;
	}
	
	
	public static void main(String args[]) throws InterruptedException {
		Worker w = new Worker(args[0]);
		w.startZK();
		
		w.register();
		Thread.sleep(60000);
	}
}
