package net.opentsdb.oauth;

import org.jboss.netty.channel.Channel;

/**
 * Created by yuanxiaolong on 2018/1/19.
 */
public class OAuthState{
    /**
     * A list of various authentication and authorization states.
     */
    public enum AuthStatus {
        SUCCESS,
        UNAUTHORIZED,
        FORBIDDEN,
        REDIRECTED,
        ERROR,
        REVOKED
    }

    private Channel channel;

    private AuthStatus status;

    private String message;

    private String token;

    private String user;

    private Exception e;

    public OAuthState(String message, String user, String token,
                      AuthStatus status) {
        this.message = message;
        this.user = user;
        this.token = token;
        this.status = status;
    }

    public OAuthState withException(Exception e){
        this.e = e;
        return this;
    }

    public String getUser() {
        return user;
    }

    public AuthStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getException() {
        return e;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public byte[] getToken() {
        return token.getBytes();
    }
}
