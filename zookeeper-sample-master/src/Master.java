import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.internal.Log;

public class Master implements Watcher{
	ZooKeeper zk;
	String hostPort;
	String serverId;
	static boolean isLeader = false;
	public static final Logger LOG = LoggerFactory.getLogger(Master.class);
	MasterStates state = MasterStates.NOTELECTED;
	
	Master(String hostPort){
		this.hostPort = hostPort;
		Random random = new Random();
		serverId = Integer.toString(random.nextInt()); 
	}
	
	StringCallback masterCreateCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case OK:
				state = MasterStates.ELECTED;
				break;
			case NODEEXISTS:
				state = MasterStates.NOTELECTED;
				masterExists();
				break;
			default:
				isLeader = false;
				break;
			}
			LOG.info("I'm am (" + (isLeader?"":"not") + " the LEADER");
		}
		
	};
	
	DataCallback masterCheckCallback = new DataCallback() {
		
		@Override
		public void processResult(int rc, String path, Object arg2, byte[] data, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				runForMaster();
				return;
			default:
				break;
			}
		
		}
	};
	
	StringCallback parentCreateCallback = new StringCallback() {
		
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				createParent(path, (byte[]) ctx);	
				break;
			case OK:
				LOG.info("Parent Created");
				break;
			case NODEEXISTS:
				LOG.warn("Parent Exists" + path);
				break;
			default:
				LOG.error("Something went wrong..run for the hills");
			}
		}
	};
	private StatCallback masterExistsCallback = new StatCallback() {
		
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				masterExists();
				break;
			case OK:
				if(stat == null) {
					state = MasterStates.RUNNING;
					runForMaster();
				}
				break;
			default:
				checkMaster();
				break;
			}
		}
	};
	private Watcher masterExistWatcher = new Watcher() {

		@Override
		public void process(WatchedEvent e) {
			if(e.getType() == EventType.NodeDeleted) {
				if("/master".equals(e.getPath())) {
					runForMaster();
				}
			}
		}
		
	};
	
	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);
	}
	
	void createParent(String path, byte data[]) {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, parentCreateCallback, data);
	}
	
	void checkMaster(){
		zk.getData("/master", false, masterCheckCallback, null);
	}
	
	void runForMaster(){
		zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
	}
	
	void startZK() {
		try {
			zk = new ZooKeeper(hostPort, 15000, this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	void stopZK() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
		
	@Override
	public void process(WatchedEvent e) {
		LOG.info(e.toString());		
	}
	
	void masterExists() {
		zk.exists("/master", masterExistWatcher , masterExistsCallback, null);
	}
	
	boolean isLeader(){
		return isLeader;
	}
	
	public static void main(String args[]) 
	throws Exception {
		
		Master m = new Master(args[0]);
		m.startZK();
		m.bootstrap();
		m.runForMaster();
	}		

}
