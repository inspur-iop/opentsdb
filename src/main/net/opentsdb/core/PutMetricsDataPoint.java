package net.opentsdb.core;

/**
 * put the metrics's datapoint into the follow metrics tables
 * tsdb-raw-metrics tsdb-metrics-second tsdb-metrics-minute tsdb-metrics-hour
 * Created by wf 201809.
 */

import net.opentsdb.core.IncomingDataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;

public class PutMetricsDataPoint {

    private static final Logger LOG = LoggerFactory.getLogger(PutMetricsDataPoint.class);
    private TSDB tsdb;
    private Config config;
    private HBaseClient client;
    private String metricsTable;
    private Map<String, String> tags = new HashMap<String, String>();
    private long timestamp = 0;
    private static final String tagkey = "tsd";
    private static final String COLUMN_FAMILY = "c";
    private static final String METRICS_QUAL = "metrics";
    private static short METRICS_WIDTH = 3;
    private UniqueId metrics;
    private long base_time;

    public PutMetricsDataPoint(TSDB tsdb, Boolean flg) throws UnknownHostException {
    	this.tsdb = tsdb;
        config = tsdb.getConfig();
        client = tsdb.getClient();
        
        if(flg) {
        	metricsTable = config.getString("tsd.storage.hbase.metrics_table");     
            metrics = new UniqueId(tsdb, metricsTable.getBytes(), METRICS_QUAL, METRICS_WIDTH, true);
        }
        
        //get tags
        int tsdNetPort = config.getInt("tsd.network.port");
        InetAddress addr = InetAddress.getLocalHost();  
        String ip = addr.getHostAddress().toString();
        String tagV = ip + ":" + tsdNetPort;        
        tags.put(tagkey, tagV);
        
    }

    //put the raw data into raw table
    public void putRawMetrics(String metricStr, int count) {
    	getBaseTime(0);
    	final byte[] row = IncomingDataPoints.rowKeyTemplate(tsdb, metricStr, tags);
        final byte[] qualifier = Internal.buildQualifier(timestamp, (short) 0);
        long value = (int)count;
        final byte[] v = getVal(value); 
        Bytes.setInt(row, (int) base_time, metrics.width() + Const.SALT_WIDTH());
        RowKey.prefixKeyWithSalt(row);
        tsdb.scheduleForCompaction(row, (int) base_time);
        PutRequest point = RequestBuilder.buildPutRequest(config, metricsTable.getBytes(), row, COLUMN_FAMILY.getBytes(), qualifier, v, timestamp);
        client.put(point);
    }
       
    //put the counts of aggregation datapoints  into aggregated table
    public void putAggregatedMetrics(long timestampTmp,int writeCount, String metricStr, String tableStr) {
    	getBaseTime(timestampTmp);
    	final byte[] row = IncomingDataPoints.rowKeyTemplate(tsdb, metricStr, tags);
        final byte[] qualifier = Internal.buildQualifier(timestamp, (short) 0);
        long value = (int)writeCount;
        final byte[] v = getVal(value);
        UniqueId metricsAgg = new UniqueId(tsdb, tableStr.getBytes(), METRICS_QUAL, METRICS_WIDTH, true);
        Bytes.setInt(row, (int) base_time, metricsAgg.width() + Const.SALT_WIDTH());
        RowKey.prefixKeyWithSalt(row);
        tsdb.scheduleForCompaction(row, (int) base_time);
        PutRequest point = RequestBuilder.buildPutRequest(config, tableStr.getBytes(), row, COLUMN_FAMILY.getBytes(), qualifier, v, timestamp);        
        client.put(point);
	}
    //get base time
    private long getBaseTime(long timestamp) {
    	//when put metrics's data point in raw table,timestamp is now
    	if (timestamp == 0) {
        	timestamp = Instant.now().toEpochMilli();       	
    	}
    	this.timestamp = timestamp;
    	
    	if ((timestamp & Const.SECOND_MASK) != 0) {
            // drop the ms timestamp to seconds to calculate the base timestamp
            base_time = ((timestamp / 1000) -
                ((timestamp / 1000) % Const.MAX_TIMESPAN));
          } else {
            base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
        }
    	return base_time;
    }
    
    //get the value in byte[]
    private byte[] getVal(long value) {
    	final byte[] v;
        if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
          v = new byte[] { (byte) value };
        } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
          v = Bytes.fromShort((short) value);
        } else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
          v = Bytes.fromInt((int) value);
        } else {
          v = Bytes.fromLong(value);
        }
        return v;
    }
}
