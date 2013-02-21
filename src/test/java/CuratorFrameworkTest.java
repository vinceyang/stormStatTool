import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;


/**
 * alibaba.cdo.com
 * transactionalStatTool
 * 2012-11-20 上午11:40:31
 */
/**
 * TODO
 * @author xiaoqing.yangxq
 */
public class CuratorFrameworkTest {
	public static void main(String[] args)  { 
		try{
	     String path = "/test_path";  
	     final CuratorFramework client = CuratorFrameworkFactory.builder().connectString("10.235.161.64:2181,10.235.161.63:2181,10.235.161.62:2181").namespace("/brokers").retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).connectionTimeoutMs(50000).build();  
	     // 启动 上面的namespace会作为一个最根的节点在使用时自动创建  
	     client.start();  
	  
	     if(client.checkExists().forPath(path) != null){
//	      创建一个节点  
	    	 client.create().forPath("/test", new byte[0]);  
	     }
//	  
//	     // 异步地删除一个节点  
//	     client.delete().inBackground().forPath("/head");  
//	  
//	     // 创建一个临时节点  
//	     client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/head/child", new byte[0]);  
//	  
//	     // 取数据  
//	     client.getData().watched().inBackground().forPath("/test");  
//	  
//	     // 检查路径是否存在  
//	     client.checkExists().forPath(path);  
//	  
//	     // 异步删除  
//	     client.delete().inBackground().forPath("/head");  
	  
	     // 注册观察者，当节点变动时触发 
	     client.getData().usingWatcher(new Watcher() {  
	         public void process(WatchedEvent event) {  
	             System.out.println("node is changed");  
	         }
	     }).inBackground().forPath("/test");
	     
	     Thread.sleep(Long.MAX_VALUE);
	     // 结束使用  
	     client.close();  
		}
	 catch(Exception e){
		 e.printStackTrace();
	 }
	 }  
}
