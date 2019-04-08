package net.opentsdb.tsd;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.User;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by yuanxiaolong on 2018/1/23.
 */
public class QueryUserRpc implements TelnetRpc, HttpRpc {

    private static final Logger LOG = LoggerFactory.getLogger(QueryUserRpc.class);

    private Config config;

    private HBaseClient client;

    private Set<String> roles;

    //用户表名称
    private String userTableName;

//    private static final String COLUMN_FAMILY = "u";


    @Override
    public void execute(TSDB tsdb, HttpQuery query) throws IOException {
        config = tsdb.getConfig();
        client = tsdb.getClient();

        userTableName = config.getString("tsd.core.user.table");
        if(Strings.isNullOrEmpty(userTableName)){
            userTableName = "tsdb-user";
        }

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
    }

    /**
     *
     * @param tsdb
     * @param query
     */
    private void handleQuery(final TSDB tsdb, final HttpQuery query) {
        final String username = query.getQueryStringParam("username");
//        String clientId = query.getQueryStringParam("client_id");

        if (Strings.isNullOrEmpty(username)
//                || Strings.isNullOrEmpty(clientId)
                ) {
//            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
//                    "Bad request",
//                    "username or client_id is empty!");
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Bad request",
                    "username is empty!");
        }

//        String rowKey = username + "|" + clientId +"|1";
        String rowKey = username + "";

        GetRequest request = new GetRequest(userTableName, rowKey);

        Deferred<User> deferred =  client.get(request)
            .addCallback(new Callback<User, ArrayList<KeyValue>>() {
            @Override
            public User call(ArrayList<KeyValue> list) throws Exception {
                User user = new User();
                for (KeyValue kv : list){
                    user.setUsername(username);
//                    user.setClientId(clientId);
                    String qualifier = new String(kv.qualifier());
                    if("phone_num".equals(qualifier)){
                        user.setPhoneNum(new String(kv.value()));
                    }else if("email".equals(qualifier)){
                        user.setEmail(new String(kv.value()));
                    }
                }
                return user;
            }
        });

        try {
            User user = deferred.join();
            String json = JSON.serializeToString(user);
            query.sendReply(json);
        }catch (Exception e){
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Bad request",
                    "query hbase user info error!");
        }
    }


    @Override
    public Deferred<Object> execute(TSDB tsdb, Channel chan, String[] command) {
        return null;
    }
}
