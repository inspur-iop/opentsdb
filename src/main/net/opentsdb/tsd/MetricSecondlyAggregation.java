package net.opentsdb.tsd;
import org.apache.curator.framework.CuratorFramework;
import org.json.JSONException;
import org.json.JSONObject;
/**
 * secondly job for aggregate write.count datapoint
 * Created by wf 201809.
 */
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

public class MetricSecondlyAggregation implements Job {
	private TSDB tsdb;
	private CuratorFramework zkClient;
	long startTime;
	long endTime;
    private final String writeCountMetric = "write.count";
    private String parentZnode;
    private String sencondlyMetricInfoZnode;
    
	public void execute(JobExecutionContext context) throws JobExecutionException {		
        JobDetail detail = context.getJobDetail();
        tsdb = (TSDB)detail.getJobDataMap().get("tsdb");
        Config config = tsdb.getConfig();
        parentZnode = config.getString("tsd.monitor.parent.znode");
        sencondlyMetricInfoZnode = parentZnode + "/metric-info/secondlyMetric";
        
        zkClient = (CuratorFramework)detail.getJobDataMap().get("zkClient");
        String startTimeStr = getStartTime();
      //get the start time in second
        startTime = Long.parseLong(startTimeStr);
        if ((startTime & Const.SECOND_MASK) != 0) {
        	startTime = startTime/1000;
        }
        
        QueryRawMetrics queryRawMetrics = new QueryRawMetrics(tsdb, zkClient, startTime, "1s-sum");
        queryRawMetrics.execute(writeCountMetric);
    }
	
	//get starttime from znode,if no content in znode, starttime is now
	public String  getStartTime(){
		JSONObject tmpjson = null;
	    try {
			if (null != zkClient.checkExists().forPath(sencondlyMetricInfoZnode)) {
			    String tmpStr = new String(zkClient.getData().forPath(sencondlyMetricInfoZnode));
			    tmpjson = new JSONObject(tmpStr);
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String startTime = "";
		if (tmpjson != null && !("").equals(tmpjson.getString("time"))) {
			startTime = tmpjson.getString("time");
		}		
		if (startTime == null || startTime.isEmpty()) {
			long startTimetmp = System.currentTimeMillis()/1000;
			startTimetmp = startTimetmp - 1;
			startTime = Long.toString(startTimetmp);
		}
		return startTime;
	}
}
