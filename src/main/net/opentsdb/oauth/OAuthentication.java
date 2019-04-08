package net.opentsdb.oauth;

import com.google.common.base.Strings;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authorization;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.HttpRpcPluginQuery;
import net.opentsdb.utils.Config;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuanxiaolong on 2018/1/24.
 */
public class OAuthentication {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthentication.class);

    private final TSDB tsdb;

    //配置
    private Config config;

    public OAuthentication(final TSDB tsdb) {
        if (tsdb == null) {
            throw new IllegalArgumentException("TSDB object cannot be null");
        }
        if (tsdb.getAuth() == null) {
            throw new IllegalArgumentException("Attempted to instantiate an "
                    + "authentication handler but it was not configured in TSDB.");
        }

        this.tsdb = tsdb;
    }

    //认证
    public OAuthState authenticateHTTP(Channel channel, HttpRequest httpRequest) {
        LOG.info("--------------Authentication authenticateHTTP------------");
        LOG.info("--------------Authentication authenticateHTTP:{}------------", httpRequest.getClass());
        LOG.info("--------------Authentication authenticateHTTP:{}------------", httpRequest.getUri());
        String user = null;
        String token = null;
        OAuthState state = null;
        if (httpRequest.getUri().startsWith("/api/user")){
            System.out.println("/api/user");
            state = new OAuthState("", "", token, OAuthState.AuthStatus.SUCCESS);
        }else {
            HttpRpcPluginQuery query = new HttpRpcPluginQuery(tsdb, httpRequest, channel);
            if(!Strings.isNullOrEmpty(query.getQueryStringParam("username"))){
                user = query.getQueryStringParam("username");
            }
            if(!Strings.isNullOrEmpty(query.getQueryStringParam("token"))){
                token = query.getQueryStringParam("token");
            }

            if(Strings.isNullOrEmpty(user)){
                String content = query.getContent();
                System.out.println(content);
            }else {
                System.out.println(user);
            }
            state = new OAuthState("", user, token, OAuthState.AuthStatus.SUCCESS);
        }
        state.setChannel(channel);

        return state;
    }

    public OAuthState authenticateTelnet(Channel channel, String[] strings) {
        OAuthState state = new OAuthState("", "", "", OAuthState.AuthStatus.FORBIDDEN);
        return state;
    }

}
