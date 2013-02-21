package com.alibaba.transactionalStatTool;

/**
 * alibaba.cdo.com
 * trasactionStatTool
 * 2012-11-15 下午2:34:23
 */
/**
 * TODO
 * @author xiaoqing.yangxq
 */

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import backtype.storm.serialization.KryoValuesDeserializer;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.transactional.state.RotatingTransactionalState;
import backtype.storm.transactional.state.TransactionalState;

public class statTool implements Watcher {
    private static final int TIMEOUT = 3000;
    
   
//    private static BigInteger getStoredCurrTransaction(TransactionalState state) {
//        BigInteger ret = (BigInteger) state.getData("currtx");
//        if(ret==null) return BigInteger.ONE;
//        else return ret;
//    }
    
    static Watcher wh=new Watcher(){
    	            public void process(org.apache.zookeeper.WatchedEvent event)
    	            {
    	                   System.out.println(event.toString());
    	            }
    	     };
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
    	 	
    	 	public void setData(String path,byte[] data) throws KeeperException, InterruptedException{
    	 		Stat s = zk.exists(path, false);
    	 		zk.setData(path, data, s.getVersion());
    	 	}
    	 	
    	 	public void close() throws InterruptedException{
    	 		zk.close();
    	 	}
    	 	
    public List<String> getChildren(String path) throws KeeperException, InterruptedException{
			return zk.getChildren(path,null);
    }
    	 	
//    
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    	String oper = args[0];
    	String zkConn = args[1];
    	String topologyId = args[2];
    	
    	
    	HashMap conf = new HashMap<String,Object>();
    	conf.put("storm.zookeeper.port",2181);
//    	conf.put("storm.zookeeper.root", "/storm");
    	conf.put("storm.zookeeper.session.timeout",20000);
    	conf.put("storm.zookeeper.retry.times",5);
    	conf.put("storm.zookeeper.retry.interval",1000);
    	conf.put("transactional.zookeeper.root", "/transactional");


    	conf.put("transactional.zookeeper.port",2181);
    	
    	conf.put("topology.fall.back.on.java.serialization", true);
    	conf.put("topology.skip.missing.kryo.registrations",false);
//    	
    	HashMap<String,List> componentConfig = new HashMap<String,List>();
    	ArrayList l1 = new ArrayList<String>();
//    	l1.add(BatchMeta.class);
    	l1.add("com.alibaba.transactionalStatTool.BatchMeta");
    	componentConfig.put("topology.kryo.register",l1);

    	
    	conf.putAll(componentConfig);
    	
    	KryoValuesDeserializer _des = new KryoValuesDeserializer(conf);
    	KryoValuesSerializer  _ser = new KryoValuesSerializer(conf);   	
    	
    	statTool stattool = new statTool();
    	stattool.connect(zkConn);
    	
    	
//    	ZooKeeper zkp = new ZooKeeper("10.20.174.136:2181,10.20.174.137:2181,10.20.174.138:2181", TIMEOUT, wh);
//    	ZooKeeper zkp = new ZooKeeper(zkConn, TIMEOUT, wh);
    	if(oper.toLowerCase().equals("get")){

        	String currtxPath="/transactional/"+topologyId+"/coordinator/currtx";
        	System.out.println(currtxPath);
        	byte[] data1 = stattool.getData(currtxPath);
        	Object currtx = _des.deserializeObject(data1);
        	System.out.println(currtx);
        	
        	String metaPath = "/transactional/"+topologyId+"/coordinator/meta";
        	System.out.println(metaPath);
            List<String> children = stattool.getChildren(metaPath);
            for(String id : children){
            	System.out.print(id + "  ");
            }
            System.out.println();
            
            String userPath="/transactional/"+topologyId+"/user";
            System.out.println(userPath);
            List<String> partions = stattool.getChildren(userPath);
            for(String p : partions){
            	System.out.println("parition: "+p);
            	List<String> txids = stattool.getChildren(userPath+"/"+p);
            	for(String txid : txids){
            		byte[] bmeta = stattool.getData(userPath+"/"+p+"/"+txid);
            		System.out.println("txid :"+txid);
            		BatchMeta meta = (BatchMeta)_des.deserializeObject(bmeta);
            		System.out.println("offset:" + meta.offset + "  "+ "nextoffset:" + meta.nextOffset);
            		
            	}
            	
            }

            stattool.close();
        
    	}else if(oper.toLowerCase().equals("set")){
    		int partition = Integer.valueOf(args[3]);
    		long offset   = Long.valueOf(args[4]);
    		long nextOffset = Long.valueOf(args[5]);
    		BatchMeta meta = new BatchMeta();
    		meta.offset = offset;
    		meta.nextOffset = nextOffset;
    		
    		String userPath="/transactional/"+topologyId+"/user";
    		
    		List<String> txids = stattool.getChildren(userPath+"/"+partition);
        	for(String txid : txids){
        		String setpath = userPath+"/"+partition+"/"+txid;
        		System.out.println("set "+setpath+"  to offset:" + meta.offset + "  "+ "nextoffset:" + meta.nextOffset);
        		byte[] ser = _ser.serializeObject(meta);
        		stattool.setData(setpath, ser);
        		
        		System.out.println("offset:" + meta.offset + "  "+ "nextoffset:" + meta.nextOffset);
        		
        	}
    	}
    		
    	}
    }
    
    
    
    