package net.opentsdb.tsd;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.TokenCache;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.oltu.oauth2.as.issuer.MD5Generator;
import org.apache.oltu.oauth2.as.issuer.OAuthIssuer;
import org.apache.oltu.oauth2.as.issuer.OAuthIssuerImpl;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 *
 * Created by yuanxiaolong on 2018/1/23.
 */
public class TokenRpc implements HttpRpc {

    private static final Logger LOG = LoggerFactory.getLogger(PutUserRpc.class);

    private static final Long _1_HOUR = 3600000L;

    private static final String COLUMN_FAMILY = "u";

    private static final String CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_LW = "content-type";

    private static final String FORM_URLENCODED="application/x-www-form-urlencoded";

    private Config config;

    private HBaseClient client;

    //用户表名称
    private String userTableName;

    @Override
    public void execute(TSDB tsdb, HttpQuery query) throws IOException {
        config = tsdb.getConfig();
        client = tsdb.getClient();

        userTableName = config.getString("tsd.core.user.table");
        if(Strings.isNullOrEmpty(userTableName)){
            userTableName = "tsdb-user";
        }

        // only accept GET/POST/DELETE
        if (query.method() != HttpMethod.POST) {
            throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
                    "Method not allowed", "The HTTP method [" + query.method().getName() +
                    "] is not permitted for this endpoint");
        }
        Map<String, String> headers = query.getHeaders();
        if((headers.containsKey(CONTENT_TYPE)
                &&FORM_URLENCODED.equals(headers.get(CONTENT_TYPE))
            ||(headers.containsKey(CONTENT_TYPE_LW)
                &&FORM_URLENCODED.equals(headers.get(CONTENT_TYPE_LW))))){
            handleQuery(tsdb, query);
        }else {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Bad request",
                    "Request Content-Type must be application/x-www-form-urlencoded");
        }
    }

    private void handleQuery(final TSDB tsdb, final HttpQuery query) {
        final String username = query.getQueryStringParam("username");
        final String password = query.getQueryStringParam("password");

        if (Strings.isNullOrEmpty(username)
                || Strings.isNullOrEmpty(password)
                ) {
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Bad request",
                    "username or password is empty!");
        }

        String rowKey = username;
        GetRequest request = new GetRequest(userTableName, rowKey);

        String token = TokenCache.getToken(rowKey);
        if(!Strings.isNullOrEmpty(token)) {
            JSONObject json = new JSONObject();
            json.put("token", token);
            query.sendReply(json.toString());
            return;
        }
        Deferred<TokenTuple3> deferred =  client.get(request)
                .addCallback(new Callback<TokenTuple3, ArrayList<KeyValue>>() {
                    @Override
                    public TokenTuple3 call(ArrayList<KeyValue> list) throws Exception {
                        String uSalt = null;
                        byte[] _password = null;
                        String token = null;
                        Long timestamp = null;
                        for (KeyValue kv : list){
                            String qualifer = new String(kv.qualifier());
                            if("u_salt".equals(qualifer))
                                uSalt = new String(kv.value());
                            else if("password".equals(qualifer))
                                _password = kv.value();
                            else if("token".equals(qualifer)){
                                token = new String(kv.value());
                                timestamp = kv.timestamp();
                            }
                        }
                        Boolean authentication = Arrays.equals(_password, DigestUtils.sha1(password+uSalt));
                        return new TokenTuple3(token, timestamp, authentication);
                    }
                });
            try {
                TokenTuple3 tokenTuple3 = deferred.join();
                if(tokenTuple3.getAuthentication()){
                    token = tokenTuple3.getToken();
                    if(Strings.isNullOrEmpty(tokenTuple3.getToken())
                            || tokenTuple3.getTimestamp() < System.currentTimeMillis()){
                        //生成Access Token
                        OAuthIssuer oauthIssuerImpl = new OAuthIssuerImpl(new MD5Generator());
                        token = oauthIssuerImpl.accessToken();

                        //保存此Token 到HBase
                        Long timestamp = System.currentTimeMillis() + _1_HOUR;
                        this.saveToken2HBase(rowKey, token, timestamp);
                        TokenCache.put2TokenRowKey(token, rowKey, timestamp);
                    }
                    JSONObject json = new JSONObject();
                    json.put("token", token);
                    query.sendReply(json.toString());
                }else {
                    String unAuthentication = "authentication failed!";
                    query.sendReply(HttpResponseStatus.BAD_REQUEST, unAuthentication.getBytes());
                }
            }catch (Exception e){
                throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                        "Bad request",
                        "query userinfo in hbase error!");
            }
    }

    private Boolean saveToken2HBase(String rowKey, String token, Long timestamp){
        PutRequest request = new PutRequest(userTableName.getBytes(),
                rowKey.getBytes(), COLUMN_FAMILY.getBytes(), "token".getBytes(), token.getBytes(),
                timestamp);

        Deferred<Boolean> deferred = client.put(request)
                .addCallback(new Callback<Boolean, Object>() {
                    @Override
                    public Boolean call(Object arg) throws Exception {
                        LOG.info("success: {}", arg);
                        return true;
                    }
                }).addErrback(new Callback<Boolean, Exception>() {
                    @Override
                    public Boolean call(Exception arg) throws Exception {
                        LOG.info("fail: {}", arg);
                        return false;
                    }
                });
        try {
            return deferred.join();
        }catch (Exception e){
            throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
                    "Bad request",
                    "write token info to hbase error!");
        }
    }

    class TokenTuple3{
        String token;
        Long timestamp;
        Boolean authentication;

        public TokenTuple3(final String token, final Long timestamp, final Boolean authentication) {
            this.token = token;
            this.timestamp = timestamp;
            this.authentication = authentication;
        }

        public String getToken() {
            return token;
        }

        public TokenTuple3 withToken(String token) {
            this.token = token;
            return this;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public TokenTuple3 withTimestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Boolean getAuthentication() {
            return authentication;
        }

        public TokenTuple3 withAuthentication(Boolean authentication) {
            this.authentication = authentication;
            return this;
        }
    }
}
