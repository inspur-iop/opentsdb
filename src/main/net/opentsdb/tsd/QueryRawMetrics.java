package net.opentsdb.tsd;

/**
 * query the tsdb-raw-metrics to get the raw metric datapoint for doing the aggregate action 
 * put the aggregated datapoint to the aggregate table 
 * logic and some function is copy from class QueryRpc 
 * Created by wf 201809.
 */
import com.fasterxml.jackson.core.JsonGenerator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.PutMetricsDataPoint;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.apache.curator.framework.CuratorFramework;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryRawMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRawMetrics.class);
    private TSDB tsdb;
    private CuratorFramework zkClient;
    private Config config;
    private long startTime;
    private long endTime;
    private final String dowmsampleStr;
    private String targetMertirc;
    private String metricsTable;
    private final String parentZnode;
    private final String sencondlyMetricInfoZnode;
    private final String minutelyMetricInfoZnode;
    private final String hourlyMetricInfoZnode;
    
    public QueryRawMetrics(final TSDB tsdb,  final CuratorFramework zkClient, long startTime, String dowmsampleStr) {
        this.tsdb = tsdb;
        this.zkClient = zkClient;
        this.config = tsdb.getConfig();
        this.startTime = startTime;
        this.dowmsampleStr = dowmsampleStr;
        
        parentZnode = config.getString("tsd.monitor.parent.znode");
        sencondlyMetricInfoZnode = parentZnode + "/metric-info/secondlyMetric";
        minutelyMetricInfoZnode = parentZnode + "/metric-info/minutelyMetric";
        hourlyMetricInfoZnode = parentZnode + "/metric-info/hourlyMetric";
    }
    public void execute(String targetMetric) {
        this.targetMertirc = targetMetric;
        //get the endtime, if starttime equal to endtime, endtime + 1(s/m/h)
        endTime = System.currentTimeMillis()/1000;
        if (dowmsampleStr.equals("1s-sum")) {
            if (startTime == endTime) {
            	startTime = startTime - 1;
            }
        }else if (dowmsampleStr.equals("1m-sum")) {
        	endTime = endTime -(endTime%60);
        	if (startTime == endTime) {
        		startTime = startTime - 60;
            }
        }else {
        	endTime = endTime -(endTime%3600);
        	if (startTime == endTime) {
        		startTime = startTime - 3600;
            }
        }
        
        //get the queries
        final TSQuery data_query = new TSQuery();
        TSSubQuery subQuery = new TSSubQuery();
        data_query.setStart(Long.toString(startTime));
        data_query.setEnd(Long.toString(endTime));
        subQuery.setMetric(targetMertirc);
        subQuery.setAggregator("sum");
        subQuery.setDownsample(dowmsampleStr);
        subQuery.setIndex(0);
        List<TSSubQuery> subQueryList = new ArrayList<TSSubQuery>();
        subQueryList.add(subQuery);
        data_query.setQueries(subQueryList);
        // validate and then compile the queries
        try {
          LOG.debug(data_query.toString());
          data_query.validateAndSetQuery();
        } catch (Exception e) {
          throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
              e.getMessage(), data_query.toString(), e);
        }
        
        final int nqueries = data_query.getQueries().size();
        final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(nqueries);
        final List<Annotation> globals = new ArrayList<Annotation>();
        
        /** This has to be attached to callbacks or we may never respond to clients */
        class ErrorCB implements Callback<Object, Exception> {
          public Object call(final Exception e) throws Exception {
            Throwable ex = e;
            try {
              LOG.error("Query exception: ", e);
              if (ex instanceof DeferredGroupException) {
                ex = e.getCause();
                while (ex != null && ex instanceof DeferredGroupException) {
                  ex = ex.getCause();
                }
                if (ex == null) {
                  LOG.error("The deferred group exception didn't have a cause???");
                }
              }
            } catch (RuntimeException ex2) {
              LOG.error("Exception thrown during exception handling", ex2);
            }
            return null;
          }
        }
        //put the datapoint which query from metric raw table to metric aggregated table
        class SendIt implements Callback<Object, StringWriter> {
        	private String metricName;
        	private long endTime;
            public SendIt(String metricName, long endTime) {
            	this.metricName = metricName;
            	this.endTime = endTime;
            }
            public Object call(final StringWriter str) throws Exception {
                String result = str.toString();
                LOG.debug(result);
                JSONObject json = new JSONObject(result);
                JSONObject jsonDps = new JSONObject(json.get("dps").toString());
                PutMetricsDataPoint putMetrics = new PutMetricsDataPoint(tsdb, false);
                if (dowmsampleStr.equals("1s-sum")) {
                	getMetricsTable("secondly");
                }else if (dowmsampleStr.equals("1m-sum")) {
                	getMetricsTable("minutely");
                }else {
                	getMetricsTable("hourly");
                }
//                long maxTimestamp = 0;
                JSONObject znodeMetricTime = new JSONObject();
                @SuppressWarnings("unchecked")
                Iterator<String> it = jsonDps.keys(); 
                while(it.hasNext()){
                    String key = it.next().toString();
                    int value = jsonDps.getInt(key);
                    long timestamp = Long.parseLong(key);
//                    if (timestamp > maxTimestamp) {
//                    	maxTimestamp = timestamp;
//                    }
                    if(value != 0) {
                    	putMetrics.putAggregatedMetrics(timestamp, value, metricName, metricsTable);
                    }
                }
                
                //put the timestamp which the max putted into aggregate table in this time
                //to metirc info znode
                znodeMetricTime.put("time", Long.toString(endTime));
                if (dowmsampleStr.equals("1s-sum")) {
                	zkClient.setData().forPath(sencondlyMetricInfoZnode, znodeMetricTime.toString().getBytes());
                }else if (dowmsampleStr.equals("1m-sum")) {
                	zkClient.setData().forPath(minutelyMetricInfoZnode, znodeMetricTime.toString().getBytes());
                }else {
                	zkClient.setData().forPath(hourlyMetricInfoZnode, znodeMetricTime.toString().getBytes());
                }
                return null;
            }
        }
        /**
         * After all of the queries have run, we get the results in the order given
         * and add dump the results in an array
         */
        class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
          private String metricName;
          private long endTime;
          public QueriesCB(String metricName, long endTime) {
        	  this.metricName = metricName;
        	  this.endTime = endTime;
          }
          public Object call(final ArrayList<DataPoints[]> query_results) 
            throws Exception {
              results.addAll(query_results);             
              if (results.size() == 0 || results.get(0).length == 0) {
            	  LOG.info("No content to get");
            	  return null;
              }else {
            	  
            	  formatQueryAsync(data_query, results, 
                              globals).addCallback(new SendIt(metricName, endTime)).addErrback(new ErrorCB()); 
                  return null;
              }              
          }
        }
        /**
         * Callback executed after we have resolved the metric, tag names and tag
         * values to their respective UIDs. This callback then runs the actual 
         * queries and fetches their results.
         */
        class BuildCB implements Callback<Deferred<Object>, Query[]> {
          public Deferred<Object> call(final Query[] queries) {
            
            final ArrayList<Deferred<DataPoints[]>> deferreds = 
                new ArrayList<Deferred<DataPoints[]>>(queries.length);
            for (final Query query : queries) {
            	deferreds.add(query.runAsyncForMetric("raw"));
            }
            return Deferred.groupInOrder(deferreds).addCallback(new QueriesCB(targetMertirc, endTime));
          }
        }
        
        data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB())
            .addErrback(new ErrorCB());
    }
       
 
    //get the query result in StringWriter
    public Deferred<StringWriter> formatQueryAsync(final TSQuery data_query, 
          final List<DataPoints[]> results, final List<Annotation> globals) 
              throws IOException {
        final StringWriter sw = new StringWriter();
        final JsonGenerator json = JSON.getFactory().createGenerator(sw);
//        json.writeStartArray();
        final Deferred<Object> cb_chain = new Deferred<Object>();
               
        class DPsResolver implements Callback<Deferred<Object>, Object> {
          /** Has to be final to be shared with the nested classes */
          final StringBuilder metric = new StringBuilder(256);
          /** Resolved tags */
          final Map<String, String> tags = new HashMap<String, String>();
          /** Resolved aggregated tags */
          final List<String> agg_tags = new ArrayList<String>();
          /** A list storing the metric and tag resolve calls */
          final List<Deferred<Object>> resolve_deferreds = 
              new ArrayList<Deferred<Object>>();
          /** The data points to serialize */
          final DataPoints dps;
          
          public DPsResolver(final DataPoints dps) {
            this.dps = dps;
          }
        
        
          public Deferred<Object> call(final Object obj) throws Exception {
            
            resolve_deferreds.add(dps.metricNameAsync()
                .addCallback(new MetricResolver()));
            resolve_deferreds.add(dps.getTagsAsync()
                .addCallback(new TagResolver()));
            resolve_deferreds.add(dps.getAggregatedTagsAsync()
                .addCallback(new AggTagResolver()));
            return Deferred.group(resolve_deferreds)
                .addCallback(new WriteToBuffer(dps));
          }

          /** Resolves the metric UID to a name*/
          class MetricResolver implements Callback<Object, String> {
            public Object call(final String metric) throws Exception {
              DPsResolver.this.metric.append(metric);
              return null;
            }
          }
          
          /** Resolves the tag UIDs to a key/value string set */
          class TagResolver implements Callback<Object, Map<String, String>> {
            public Object call(final Map<String, String> tags) throws Exception {
              DPsResolver.this.tags.putAll(tags);
              return null;
            }
          }
          
          /** Resolves aggregated tags */
          class AggTagResolver implements Callback<Object, List<String>> {
            public Object call(final List<String> tags) throws Exception {
              DPsResolver.this.agg_tags.addAll(tags);
              return null;
            }
          }
          
          class WriteToBuffer implements Callback<Object, ArrayList<Object>> {
            final DataPoints dps;
            
            /**
             * Default ctor that takes a data point set
             * @param dps Datapoints to print
             */
            public WriteToBuffer(final DataPoints dps) {
              this.dps = dps;
            }
            
            /**
             * Handles writing the data to the output buffer. The results of the
             * deferreds don't matter as they will be stored in the class final
             * variables.
             */
            public Object call(final ArrayList<Object> deferreds) throws Exception {

              final TSSubQuery orig_query = data_query.getQueries()
                  .get(dps.getQueryIndex());
              
              json.writeStartObject();
              json.writeStringField("metric", metric.toString());
              
              json.writeFieldName("tags");
              json.writeStartObject();
              if (dps.getTags() != null) {
                for (Map.Entry<String, String> tag : tags.entrySet()) {
                  json.writeStringField(tag.getKey(), tag.getValue());
                }
              }
              json.writeEndObject();
              json.writeFieldName("aggregateTags");
              json.writeStartArray();
              if (dps.getAggregatedTags() != null) {
                for (String atag : agg_tags) {
                  json.writeString(atag);
                }
              }
              json.writeEndArray();
              
              json.writeFieldName("dps");
              json.writeStartObject();
              for (final DataPoint dp : dps) {
                if (dp.timestamp() < (data_query.startTime()) || 
                    dp.timestamp() > (data_query.endTime())) {
                  continue;
                }
                final long timestamp = data_query.getMsResolution() ? 
                    dp.timestamp() : dp.timestamp() / 1000;
                    if (dp.isInteger()) {
                        json.writeNumberField(Long.toString(timestamp), dp.longValue());
                      } else {
                        // Report missing intervals as null or NaN.
                        final double value = dp.doubleValue();
                        if (Double.isNaN(value) && 
                            orig_query.fillPolicy() == FillPolicy.NULL) {
                          json.writeNumberField(Long.toString(timestamp), null);
                        } else {
                          json.writeNumberField(Long.toString(timestamp), dp.doubleValue());
                        }
                    }
              }
              json.writeEndObject();
              json.writeEndObject();
              return null;
            }
          }
        }

        for (DataPoints[] separate_dps : results) {
          for (DataPoints dps : separate_dps) {
            try {
              cb_chain.addCallback(new DPsResolver(dps));
            } catch (Exception e) {
              throw new RuntimeException("Unexpected error durring resolution", e);
            }
          }
        }
        
        class FinalCB implements Callback<StringWriter, Object> {
            public StringWriter call(final Object obj)
                throws Exception {
              json.close();
              return sw;
            }
        }
        cb_chain.callback(null);
        return cb_chain.addCallback(new FinalCB());
    }

    //according agg job to get tsdb table
    private void getMetricsTable(String flg) {
    	if (flg.equals("secondly")) {
        	metricsTable = config.getString("tsd.storage.hbase.metrics_table_sencondly");
        }else if(flg.equals("minutely")) {
        	metricsTable = config.getString("tsd.storage.hbase.metrics_table_minutely");
        }else if(flg.equals("hourly")) {
        	metricsTable = config.getString("tsd.storage.hbase.metrics_table_hourly");
        }
    }
}
