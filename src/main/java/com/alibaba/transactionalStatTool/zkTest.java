package com.alibaba.transactionalStatTool;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;


/**
 * alibaba.cdo.com
 * transactionalStatTool
 * 2012-12-13 下午4:33:08
 */
/**
 * TODO
 * @author xiaoqing.yangxq
 */
public class zkTest implements Watcher {
	private static final int SESSION_TIMEOUT = 3000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public void connect(String hosts) throws IOException, InterruptedException{
		zk = new ZooKeeper(hosts,SESSION_TIMEOUT,this);
		connectedSignal.await();
	}
	
	public void process(WatchedEvent event){
		if(event.getState() == KeeperState.SyncConnected){
			connectedSignal.countDown();
		}
	}
	
	public void createPath(String path) throws KeeperException, InterruptedException{
		Stat s = zk.exists(path, false);
		if(s == null){
			zk.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	public byte[] getData(String path) throws KeeperException, InterruptedException{
		Stat s = zk.exists(path, false);
		return zk.getData(path, null, s);
	}
	
	public void setData(String path,String data) throws KeeperException, InterruptedException{
		Stat s = zk.exists(path, false);
		zk.setData(path, data.getBytes(), s.getVersion());
	}
	
	public void close() throws InterruptedException{
		zk.close();
	}
	
	
	

	public static void main(String[] args)  throws IOException, InterruptedException, KeeperException{
		int consnum = Integer.valueOf(args[0]);
		final String hosts = args[1];
		int    dataLen = Integer.valueOf(args[2]);
		final String operation = args[3];

//		for(int i =0 ;i<500;i++){
//			System.out.println(i);
//			ZooKeeper zkp = new ZooKeeper("10.235.161.64:2181,10.235.161.63:2181,10.235.161.62:2181", TIMEOUT, wh);
//		}
		
//		zkTest zktest = new zkTest();
//		zktest.connect(hosts);
		final StringBuilder sb = new StringBuilder();
		for(int i =0;i<dataLen;i++){
			sb.append("a");
		}
		
		for( int i =0 ;i<consnum;i++){
			System.out.println(i);
			String hostname = InetAddress.getLocalHost().getHostName();
			final String path="/test/"+hostname+"_"+i;
			Runnable r = new Runnable(){
				
				public void run() {
					// TODO Auto-generated method stub
					zkTest zktest = new zkTest();
					try {
						zktest.connect(hosts);			
						zktest.createPath(path);
//						if(operation.toLowerCase().equals("get") ){
//							byte[] gdata= zktest.getData(path);
//							System.out.println(Thread.currentThread().getName()+new String(gdata));
//						}else if(operation.toLowerCase().equals("set")){
//							zktest.setData(path, sb.toString());
//						}
						long start_time = System.currentTimeMillis();
						if(operation.equals("set")){
							for(int j = 0;j<100000;j++){
								zktest.setData(path, sb.toString());
								if(j%1000 == 0){
									long end_time = System.currentTimeMillis();
									System.out.println(Thread.currentThread().getName()+"==========consume time:"+(end_time-start_time));
									start_time = System.currentTimeMillis();;
								}
							}
						}
						if(operation.equals("get")){
							for(int j = 0;j<100000;j++){
							
							byte[] gdata= zktest.getData(path);
								if(j%1000 == 0){
									long end_time = System.currentTimeMillis();
									System.out.println(Thread.currentThread().getName()+"==========consume time:"+(end_time-start_time));
									start_time = System.currentTimeMillis();;
								}
							}
						}
						
						if(operation.equals("all")){
							for(int j = 0;j<100000;j++){
								zktest.setData(path, sb.toString());
								byte[] gdata= zktest.getData(path);
									if(j%1000 == 0){
										long end_time = System.currentTimeMillis();
										System.out.println(Thread.currentThread().getName()+"==========consume time:"+(end_time-start_time));
										start_time = System.currentTimeMillis();;
									}
								}
						}
						
						Thread.sleep(10*60*1000);
						zktest.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			};
			Thread t = new Thread(r);  
			t.start(); 
//			Thread.sleep(5000);
		}
		Thread.sleep(10*60*1000);
	}

	
}
