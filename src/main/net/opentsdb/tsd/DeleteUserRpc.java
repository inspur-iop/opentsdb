package net.opentsdb.tsd;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 *
 * Created by yuanxiaolong on 2018/1/23.
 */
public class DeleteUserRpc implements HttpRpc {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteUserRpc.class);

    private Config config;

    private HBaseClient client;

    //用户表名称
    private String userTableName;

//    private static final String COLUMN_FAMILY = "u";

    private static final String CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_LW = "content-type";

    private static final String FORM_URLENCODED="application/x-www-form-urlencoded";

    @Override
    public void execute(TSDB tsdb, HttpQuery query) throws IOException {
        config = tsdb.getConfig();
        client = tsdb.getClient();

        userTableName = config.getString("tsd.core.user.table");
        if(Strings.isNullOrEmpty(userTableName)){
            userTableName = "tsdb-user";
        }

        // only accept GET/POST/DELETE
        if (query.method() != HttpMethod.DELETE) {
            throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
                    "Method not allowed", "The HTTP method [" + query.method().getName() +
                    "] is not permitted for this endpoint");
        }
        handleQuery(query);
    }

    private void handleQuery(final HttpQuery query) {
        final String username = query.getQueryStringParam("username");
//        final String clientId = query.getQueryStringParam("client_id");
//        final String clientSecret = query.getQueryStringParam("client_secret");

        if (Strings.isNullOrEmpty(username)
//                || Strings.isNullOrEmpty(clientId)
//                || Strings.isNullOrEmpty(clientSecret)
                ) {
//            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
//                    "Bad request",
//                    "username, password, client_id,client_secret  is empty!");
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Bad request",
                    "username is empty!");
        }

//        String rowKey = username + "|" + clientId;
        String rowKey = username ;

        DeleteRequest request = new DeleteRequest(userTableName, rowKey);

        Deferred<Boolean> deferred =  client.delete(request)
                .addCallback(new Callback<Boolean, Object>() {
                    @Override
                    public Boolean call(Object arg) throws Exception {
                        return true;
                    }
                });
            try {
                if(deferred.join()){
                    query.sendReply("deleted");
                }
            }catch (Exception e){
                throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                        "Bad request",
                        "delete user in hbase error!");
            }
    }

}
