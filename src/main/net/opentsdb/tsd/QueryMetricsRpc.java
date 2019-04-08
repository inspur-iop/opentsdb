package net.opentsdb.tsd;
/**
 * 201809 create by wf
 * use for query metrics datapoint
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.HBaseException;
import org.hbase.async.RpcTimedOutException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.PutMetricsDataPoint;
import net.opentsdb.core.Query;
import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.uid.NoSuchUniqueName;

final class QueryMetricsRpc implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(QueryMetricsRpc.class);
  
  /** Various counters and metrics for reporting query stats */
  static final AtomicLong query_invalid = new AtomicLong();
  static final AtomicLong query_exceptions = new AtomicLong();
  static final AtomicLong query_success = new AtomicLong();
  String tableFlg;
  
  /**
   * Implements the /api/query endpoint to fetch data from OpenTSDB.
   * @param tsdb The TSDB to use for fetching data
   * @param query The HTTP query for parsing and responding
   */
  @Override
  public void execute(final TSDB tsdb, final HttpQuery query) 
    throws IOException {
    
    // only accept GET/POST/DELETE
    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST &&
        query.method() != HttpMethod.DELETE) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    if (query.method() == HttpMethod.DELETE && 
        !tsdb.getConfig().getBoolean("tsd.http.query.allow_delete")) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
               "Bad request",
               "Deleting data is not enabled (tsd.http.query.allow_delete=false)");
    }
    
    handleQuery(tsdb, query);
    
    PutMetricsDataPoint putMetrics = new PutMetricsDataPoint(tsdb, true);
    putMetrics.putRawMetrics("read.request", 1);
  }

  /**
   * Processing for a data point query
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to parse/respond
   */
  private void handleQuery(final TSDB tsdb, final HttpQuery query) {
    final TSQuery data_query;
    switch (query.apiVersion()) {
    case 0:
    case 1:
      data_query = query.serializer().parseQueryV1();
      tableFlg = data_query.getResolution();
//      tableFlg = "m";
      break;
    default:
      query_invalid.incrementAndGet();
      throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
          "Requested API version not implemented", "Version " + 
          query.apiVersion() + " is not implemented");
    }   
    
    // validate and then compile the queries
    try {
      LOG.debug(data_query.toString());
      data_query.validateAndSetQuery();
    } catch (Exception e) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          e.getMessage(), data_query.toString(), e);
    }
    
    if (tsdb.getAuth() != null && tsdb.getAuth().authorization() != null) {
      if (query.channel().getAttachment() == null || 
          !(query.channel().getAttachment() instanceof AuthState)) {
        throw new BadRequestException(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
            "Authentication was enabled but the authentication state for "
            + "this channel was not set properly");
      }
      final AuthState state = tsdb.getAuth().authorization().allowQuery(
          (AuthState) query.channel().getAttachment(), data_query);
      switch (state.getStatus()) {
      case SUCCESS:
        // cary on :)
        break;
      case UNAUTHORIZED:
        throw new BadRequestException(HttpResponseStatus.UNAUTHORIZED, 
            state.getMessage());
      case FORBIDDEN:
        throw new BadRequestException(HttpResponseStatus.FORBIDDEN, 
            state.getMessage());
      default:
        throw new BadRequestException(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
            state.getMessage());
      }
    }
    
    // if the user tried this query multiple times from the same IP and src port
    // they'll be rejected on subsequent calls
    final QueryStats query_stats = 
        new QueryStats(query.getRemoteAddress(), data_query, 
            query.getPrintableHeaders());
    data_query.setQueryStats(query_stats);
    query.setStats(query_stats);
    
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

          if (ex instanceof RpcTimedOutException) {
            query_stats.markSerialized(HttpResponseStatus.REQUEST_TIMEOUT, ex);
            query.badRequest(new BadRequestException(
                HttpResponseStatus.REQUEST_TIMEOUT, ex.getMessage()));
            query_exceptions.incrementAndGet();
          } else if (ex instanceof HBaseException) {
            query_stats.markSerialized(HttpResponseStatus.FAILED_DEPENDENCY, ex);
            query.badRequest(new BadRequestException(
                HttpResponseStatus.FAILED_DEPENDENCY, ex.getMessage()));
            query_exceptions.incrementAndGet();
          } else if (ex instanceof QueryException) {
            query_stats.markSerialized(((QueryException)ex).getStatus(), ex);
            query.badRequest(new BadRequestException(
                ((QueryException)ex).getStatus(), ex.getMessage()));
            query_exceptions.incrementAndGet();
          } else if (ex instanceof BadRequestException) {
            query_stats.markSerialized(((BadRequestException)ex).getStatus(), ex);
            query.badRequest((BadRequestException)ex);
            query_invalid.incrementAndGet();
          } else if (ex instanceof NoSuchUniqueName) {
            query_stats.markSerialized(HttpResponseStatus.BAD_REQUEST, ex);
            query.badRequest(new BadRequestException(ex));
            query_invalid.incrementAndGet();
          } else {
            query_stats.markSerialized(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
            query.badRequest(new BadRequestException(ex));
            query_exceptions.incrementAndGet();
          }
          
        } catch (RuntimeException ex2) {
          LOG.error("Exception thrown during exception handling", ex2);
          query_stats.markSerialized(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex2);
          query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
              ex2.getMessage().getBytes());
          query_exceptions.incrementAndGet();
        }
        return null;
      }
    }
    
    /**
     * After all of the queries have run, we get the results in the order given
     * and add dump the results in an array
     */
    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
      public Object call(final ArrayList<DataPoints[]> query_results) 
        throws Exception {
        
    	results.addAll(query_results);
        
        /** Simply returns the buffer once serialization is complete and logs it */
        class SendIt implements Callback<Object, ChannelBuffer> {
          public Object call(final ChannelBuffer buffer) throws Exception {
            query.sendReply(buffer);
            query_success.incrementAndGet();
            return null;
          }
        }

        switch (query.apiVersion()) {
        case 0:
        case 1:
            query.serializer().formatQueryAsyncV1(data_query, results, 
               globals).addCallback(new SendIt()).addErrback(new ErrorCB());
          break;
        default: 
          query_invalid.incrementAndGet();
          throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
              "Requested API version not implemented", "Version " + 
              query.apiVersion() + " is not implemented");
        }
        return null;
      }
    }
    
    /**
     * Callback executed after we have resolved the metric, tag names and tag
     * values to their respective UIDs. This callback then runs the actual 
     * queries and fetches their results.
     */
    class BuildCB implements Callback<Deferred<Object>, Query[]> {
      @Override
      public Deferred<Object> call(final Query[] queries) {
        final ArrayList<Deferred<DataPoints[]>> deferreds = 
            new ArrayList<Deferred<DataPoints[]>>(queries.length);
        for (final Query query : queries) {
          // call different interfaces basing on whether it is a percentile query
          deferreds.add(query.runAsyncForMetric(tableFlg));//201809 change point
        }
        return Deferred.groupInOrder(deferreds).addCallback(new QueriesCB());
      }
    }
 
    data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB())
        .addErrback(new ErrorCB());
  }  
}

