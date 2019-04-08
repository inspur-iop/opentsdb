package net.opentsdb.tsd;

/**201809 create by wf 
 * create znode store tsd's info and monitor tsd's status
 * use curator to elect tsdb's worker, aggregate the metirc's datapoint to the aggregation table
**/

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import org.quartz.Scheduler;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

public final class ZkClientWorker {
  private static final Logger LOG = LoggerFactory.getLogger(ZkClientWorker.class);
  public TSDB tsdb;
  public Config config;
  public static CuratorFramework client;
  private final String parentZnode;
  private final String tsdParentZnode;
  private final String sencondlyMetricInfoZnode;
  private final String minutelyMetricInfoZnode;
  private final String hourlyMetricInfoZnode;
  private String tsdZkQuorum;
  private final JSONObject znodeJson = new JSONObject();
  private final String dataContent;
  private MetricQuartz metricQuartz;
  private Scheduler scheduler;
  public ZkClientWorker(final TSDB tsdb) {
    this.tsdb = tsdb;
    config = tsdb.getConfig();
    
    parentZnode = config.getString("tsd.monitor.parent.znode");
    tsdParentZnode = parentZnode + "/tsds";
    sencondlyMetricInfoZnode = parentZnode + "/metric-info/secondlyMetric";
    minutelyMetricInfoZnode = parentZnode + "/metric-info/minutelyMetric";
    hourlyMetricInfoZnode = parentZnode + "/metric-info/hourlyMetric";
    
    tsdZkQuorum = config.getString("tsd.monitor.zk_quorum");
    int tsdNetPort = config.getInt("tsd.network.port");
    InetAddress addr = null;
	try {
		addr = InetAddress.getLocalHost();
	} catch (UnknownHostException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	String ip = "0.0.0.0";
	if (addr != null) {
		ip = addr.getHostAddress().toString();
	}
	dataContent = ip + ":" + tsdNetPort;
    znodeJson.put("url", dataContent);
  }
  
  public void execute() {
    try {
      createZkClient(tsdZkQuorum);
      createMetricInfoZknode();
      createZknode();    
      nodeMonitor();
    } catch (Exception e){
      e.printStackTrace();
    }
  }

  //create connection to zookeeper server
  private void createZkClient(String connectionInfo) {
      RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client =
          CuratorFrameworkFactory.builder()
                  .connectString(connectionInfo)
                  .sessionTimeoutMs(5000)
                  .connectionTimeoutMs(5000)
                  .retryPolicy(retryPolicy)
                  .build();
        client.start();
  }
  
  /**create zknode for store tsd's info
   * znode list:/tsdb/tsds/tsdxxxxxxxxxx
   * tsdxxxxxxxxxx's info is tsd's ip+portNum
   * tsds's info is worker tsd's ip+portNum
   * @param protNum  The TCP port TSD should use for communications
   * @throws Exception
   */
  private void createZknode() throws Exception {   
    //parent znode '/tsdb/tsds' is created or not
    if (null == client.checkExists().forPath(tsdParentZnode)) {
      client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(tsdParentZnode, initParentPathContent().toString().getBytes());
    }
    //create children znode   
    //if tsd is shutdown the znode will be deleted
    client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(tsdParentZnode + "/tsd-",znodeJson.toString().getBytes());       
    childrenMonitor();
  }
  
  //children znode monitor
  private void childrenMonitor() throws Exception {
    PathChildrenCache tasksCache = new PathChildrenCache(client, tsdParentZnode, true);
    
    PathChildrenCacheListener tasksCacheListener = new PathChildrenCacheListener() {
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        //parent znode's content
        String workNode = getContentFromZknode(tsdParentZnode).getString("znode");
        //get min children znode
        String currentMin = getMinZnode();
        //update parent znode's content
        if (!workNode.equals(currentMin)) {
          setContentToParentPath(currentMin);
        }
        isWorkNode(getContentFromZknode(tsdParentZnode).getString("url"));
      }
    };
    tasksCache.getListenable().addListener(tasksCacheListener);
    tasksCache.start();
  }
  
  //parent znode monitor
  private void nodeMonitor() throws Exception {
    final NodeCache nodeCache = new NodeCache(client, tsdParentZnode);
    
    NodeCacheListener nodeCacheListener = new NodeCacheListener() {
      public void nodeChanged() throws Exception {
        //parent znode is exist or not
        if (null != nodeCache.getCurrentData()) {
          isWorkNode(getContentFromZknode(tsdParentZnode).getString("url"));
        } else {
          System.out.println("parent node not found ,create parent node first");
          createZknode();
          isWorkNode(getContentFromZknode(tsdParentZnode).getString("url"));
        }
      }
    };
    nodeCache.getListenable().addListener(nodeCacheListener);
    nodeCache.start();
  }
  
  //get the min znode from children node list
  private String getMinZnode() throws Exception {
    List<String> childrens = new ArrayList<String>();
    String minZnode = new String();
    childrens = client.getChildren().forPath(tsdParentZnode);
    minZnode = Collections.min(childrens);
    return minZnode;
  }
  //if znode is the work node, do the metirc aggregate job
  private void  isWorkNode(String urldata) throws Exception {
    if (urldata.equals(dataContent)) {
      LOG.info("work node "+dataContent+" will do the metirc aggregate job");
      if(metricQuartz == null) {
    	  metricQuartz = new MetricQuartz(tsdb, client);
    	  scheduler = metricQuartz.execute();
    	  scheduler.start();
      }else {
    	  if(scheduler != null && scheduler.isShutdown()) {
    		  scheduler.start();
    	  }    	  
      }
    } else {
    	if(metricQuartz != null && scheduler.isStarted()){
    		scheduler.shutdown();
    	}
    }
  }
  
  private String getContentFromChildrenPath(String znode) throws Exception {
    String tmpStr = new String(client.getData().forPath(tsdParentZnode + "/" + znode));
    JSONObject tmpjson = new JSONObject(tmpStr);
    return tmpjson.getString("url");
  }
  
  private void setContentToParentPath(String znode) throws Exception {
    JSONObject json = new JSONObject();
    json.put("znode", znode);
    json.put("url", getContentFromChildrenPath(znode));
    client.setData().forPath(tsdParentZnode, json.toString().getBytes());
  }
  
  //get content from zknode
  public JSONObject getContentFromZknode(String zknodePath) throws Exception {
      JSONObject tmpjson = null;
      if (null != client.checkExists().forPath(zknodePath)) {
          String tmpStr = new String(client.getData().forPath(zknodePath));
          tmpjson = new JSONObject(tmpStr);
        
      }
      return tmpjson;
  }
  
  private JSONObject initParentPathContent() {
      JSONObject json = new JSONObject();
      json.put("znode", "");
      json.put("url", "");
      return json;
  }
  
  //create zknode for metric info store
  private void createMetricInfoZknode() throws Exception {
      if (null == client.checkExists().forPath(sencondlyMetricInfoZnode)) {
          client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(sencondlyMetricInfoZnode, initMetricInfoZknode().toString().getBytes());
      }
      if (null == client.checkExists().forPath(minutelyMetricInfoZnode)) {
          client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(minutelyMetricInfoZnode, initMetricInfoZknode().toString().getBytes());
      }
      if (null == client.checkExists().forPath(hourlyMetricInfoZnode)) {
          client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(hourlyMetricInfoZnode, initMetricInfoZknode().toString().getBytes());
      }
  }

  private JSONObject initMetricInfoZknode() {
      JSONObject json = new JSONObject();
      json.put("time", "");
      return json;
  }
}
