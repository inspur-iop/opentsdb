package net.opentsdb.oauth;

import com.google.common.base.Strings;
import net.opentsdb.auth.AuthState;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * 自定义基于OAuth2的API权限类
 * Created by yuanxiaolong on 2018/1/24.
 */
public class OAuthChannelHandler extends SimpleChannelHandler {
    private static final Logger LOG = LoggerFactory.getLogger(
            OAuthChannelHandler.class);

    public static final String TELNET_AUTH_FAILURE = "AUTH_FAIL\r\n";
    public static final String TELNET_AUTH_SUCCESS = "AUTH_SUCCESS\r\n";

    private OAuthentication oAuthentication;

    public OAuthChannelHandler(final TSDB tsdb) {
       if (tsdb == null) {
            throw new IllegalArgumentException("TSDB object cannot be null");
        }
        if (tsdb.getAuth() == null) {
            throw new IllegalArgumentException("Attempted to instantiate an "
                    + "authentication handler but it was not configured in TSDB.");
        }

        oAuthentication = new OAuthentication(tsdb);

        LOG.info("Set up AuthenticationChannelHandler: " + oAuthentication);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent authEvent) throws Exception {
        try {
            final Object authCommand = authEvent.getMessage();
            // Telnet Auth
            if (authCommand instanceof String[]) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Authenticating Telnet command from channel: "
                            + authEvent.getChannel());
                }
                String auth_response = TELNET_AUTH_FAILURE;
                final OAuthState state = oAuthentication.authenticateTelnet(
                        authEvent.getChannel(), (String[]) authCommand);
                if (state.getStatus() == OAuthState.AuthStatus.SUCCESS) {
                    auth_response = TELNET_AUTH_SUCCESS;
                    ctx.getPipeline().remove(this);
                    authEvent.getChannel().setAttachment(state);
                    state.setChannel(authEvent.getChannel());
                }
                authEvent.getChannel().write(auth_response);

                // HTTTP Auth
            } else if (authCommand instanceof HttpRequest) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Authenticating HTTP request from channel: "
                            + authEvent.getChannel());
                }
                HttpResponseStatus status;
                final OAuthState state = oAuthentication.authenticateHTTP(
                        authEvent.getChannel(), (HttpRequest) authCommand);
                if (state.getStatus() == OAuthState.AuthStatus.SUCCESS) {
                    ctx.getPipeline().remove(this);
                    authEvent.getChannel().setAttachment(state);
                    state.setChannel(authEvent.getChannel());
                    // pass it down!
                    super.messageReceived(ctx, authEvent);
                } else if (state.getStatus() == OAuthState.AuthStatus.REDIRECTED) {
                    // do nothing here as the plugin sent the redirect answer. We want to
                    // keep auth inline so the next call can process the authentication.
                } else {
                    switch (state.getStatus()) {
                        case UNAUTHORIZED:
                            status = HttpResponseStatus.UNAUTHORIZED;
                            break;
                        case FORBIDDEN:
                            status = HttpResponseStatus.FORBIDDEN;
                            break;
                        default:
                            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                            break;
                    }

                    final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
                    if (!Strings.isNullOrEmpty(state.getMessage())) {
                        // TODO - JSONify or something
                        response.setContent(ChannelBuffers.copiedBuffer(
                                state.getMessage(), Const.UTF8_CHARSET));
                    }
                    authEvent.getChannel().write(response);
                }
                // Unknown Authentication. Log and close the connection.
            } else {
                LOG.error("Unexpected message type " + authCommand.getClass() + ": "
                        + authCommand + " from channel: " + authEvent.getChannel());
                authEvent.getChannel().close();
            }
        } catch (Throwable t) {
            LOG.error("Unexpected exception caught while serving channel: "
                    + authEvent.getChannel(), t);
            authEvent.getChannel().close();
        }
        super.messageReceived(ctx, authEvent);
    }
}
