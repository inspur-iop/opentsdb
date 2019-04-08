package net.opentsdb.core;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yuanxiaolong on 2018/1/22.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class User {
    private static final Logger LOG = LoggerFactory.getLogger(User.class);

    //用户名称
    private String username;

    //用户密码
    private String password;

    //客户端id
    private String clientId;

    //客户端secret
    private String clientSecret;

    //Token
    private String token;
    //Token的有效期，时间戳
    private Long tokenTs;

    //角色
    private String roles;

    //手机号码
    private String phoneNum;

    //用户邮箱
    private String email;

    //用户密码的盐值
    private String uSalt;

    //客户端的盐值
    private String cSalt;

    /**
     * Empty constructor necessary for some de/serializers
     */
    public User() {

    }

    public User(String username, String password,
                String clientId, String clientSecret,
                String uSalt, String cSalt) {
        this.username = username;
        this.password = password;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.uSalt = uSalt;
        this.cSalt = cSalt;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getRoles() {
        return roles;
    }

    public void setRoles(String roles) {
        this.roles = roles;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getuSalt() {
        return uSalt;
    }

    public void setuSalt(String uSalt) {
        this.uSalt = uSalt;
    }

    public String getcSalt() {
        return cSalt;
    }

    public void setcSalt(String cSalt) {
        this.cSalt = cSalt;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Long getTokenTs() {
        return tokenTs;
    }

    public void setTokenTs(Long tokenTs) {
        this.tokenTs = tokenTs;
    }

    @Override
    public String toString() {
        return "User{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", clientId='" + clientId + '\'' +
                ", clientSecret='" + clientSecret + '\'' +
                ", roles='" + roles + '\'' +
                ", phoneNum='" + phoneNum + '\'' +
                ", email='" + email + '\'' +
                ", uSalt='" + uSalt + '\'' +
                ", cSalt='" + cSalt + '\'' +
                '}';
    }
}
