package net.opentsdb.tsd;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.User;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.commons.codec.digest.DigestUtils;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 新增数据库用户接口
 * Created by yuanxiaolong on 2018/1/22.
 */
public class PutUserRpc implements HttpRpc  {

    private static final Logger LOG = LoggerFactory.getLogger(PutUserRpc.class);

    private Config config;

    private HBaseClient client;

    private Set<String> roles;

    //用户表名称
    private String userTableName;

    private static final String COLUMN_FAMILY = "u";

    //写入成功的记录数
    protected final AtomicLong SUCC_STORED = new AtomicLong();

    //写入失败的记录数
    protected final AtomicLong FAIL_STORED = new AtomicLong();

    @Override
    public void execute(TSDB tsdb, HttpQuery query) throws IOException {
        config = tsdb.getConfig();
        client = tsdb.getClient();

        loadRoles(config.getString("tsd.core.role.permissions.info"));

        userTableName = config.getString("tsd.core.user.table");
        if(Strings.isNullOrEmpty(userTableName)){
            userTableName = "tsdb-user";
        }

        // only accept POST
        if (query.method() != HttpMethod.POST) {
            throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
                    "Method not allowed", "The HTTP method [" + query.method().getName() +
                    "] is not permitted for this endpoint");
        }

        final List<User> users;

        try {
            users = query.serializer()
                    .parseUserPutV1(User.class, HttpJsonSerializer.TR_DB_USER);
        } catch (BadRequestException e) {
            LOG.error("Parse User info exception:{}",e);
            throw e;
        }

        processUser(query, users);
    }

    /**
     *
     * @param query
     * @param users
     * @param <T>
     */
    public <T extends User> void processUser(final HttpQuery query, final List<T> users) {
        if (users.size() < 1) {
            throw new BadRequestException("No user info found in content");
        }

        for (User user : users) {
            if (Strings.isNullOrEmpty(user.getUsername())
//                    || Strings.isNullOrEmpty(user.getClientId())
//                    || Strings.isNullOrEmpty(user.getClientSecret())
                ){
                FAIL_STORED.incrementAndGet();
//                LOG.error("username, password, clientId, clientSecret is not null");
                LOG.error("username must be not null");
                continue;
            }
            //            String rowKey = user.getUsername()+"|"+user.getClientId();
            String rowKey = user.getUsername();

            ArrayList<Deferred<Boolean>> workers = new ArrayList<Deferred<Boolean>>();

            try {
                if (!Strings.isNullOrEmpty(user.getPassword())) {
                    //判断密码是否为空，不为空，则使用非空新值覆盖所有字段
                    String uSalt = user.getuSalt();
                    if(Strings.isNullOrEmpty(uSalt)) uSalt = getRandomString(30);
                    workers.add(deferred(newRequest(client, rowKey, "u_salt", uSalt)));
                    workers.add(deferred(newRequest(client, rowKey, "password", DigestUtils.sha1(user.getPassword()+uSalt))));

                    if(!Strings.isNullOrEmpty(user.getToken())
                            && null != user.getTokenTs() && user.getTokenTs() > System.currentTimeMillis()){
                        workers.add(deferred(newRequest(client, rowKey, "token", user.getToken(), user.getTokenTs())));
                    }
//                String cSalt = user.getcSalt();
//                if(Strings.isNullOrEmpty(cSalt)) cSalt = getRandomString(30);
//                workers.add(deferred(newRequest(client, rowKey, "c_salt", cSalt)));
//                workers.add(deferred(newRequest(client, rowKey, "client_secret", DigestUtils.sha1(user.getClientSecret()+cSalt))));
                }else {
                    //判断密码为空，然后判断数据库中是否有此用户信息，如果没有直接退出
                    // 如果有只做添加角色以及更新用户信息的操作
                    GetRequest request = new GetRequest(userTableName, rowKey, COLUMN_FAMILY);
                    Deferred<Boolean> exists =  client.get(request)
                            .addCallback(new Callback<Boolean, ArrayList<KeyValue>>() {
                                @Override
                                public Boolean call(ArrayList<KeyValue> list) throws Exception {
                                    return null == list? false:list.size() > 0;
                                }
                            });
                    if(!exists.join()){
                        FAIL_STORED.incrementAndGet();
                        LOG.error("password must be not null");
                        continue;
                    }
                }

                if(!Strings.isNullOrEmpty(user.getRoles())){
                    String value = "";
                    StringTokenizer tokenizer = new StringTokenizer(user.getRoles(), ",");
                    while (tokenizer.hasMoreTokens()){
                        String role = tokenizer.nextToken().trim();
                        //角色校验
                        if(roles.contains(role)) value += role + ",";
                    }
                    if(value.endsWith(","))
                        workers.add(deferred(newRequest(client, rowKey, "roles", value.substring(0, value.length()-1))));
                }

                if(!Strings.isNullOrEmpty(user.getPhoneNum()))
                    workers.add(deferred(newRequest(client, rowKey, "phone_num", user.getPhoneNum())));
                if(!Strings.isNullOrEmpty(user.getEmail()))
                    workers.add(deferred(newRequest(client, rowKey, "email", user.getEmail())));

                Deferred<ArrayList<Boolean>> deferred = Deferred.group(workers);
                //线程阻塞直到返回所有查询结果
                ArrayList<Boolean> results = deferred.join();
                boolean flag = true;
                for (Boolean r : results) {
                    if(!r){flag=false;break;}
                }
                if(flag)SUCC_STORED.incrementAndGet();
                else FAIL_STORED.incrementAndGet();
            }catch (Throwable t){
                FAIL_STORED.incrementAndGet();
                LOG.error("put user: {} exception:{}", user, t);
            }
        }

        try {
            JSONObject result = new JSONObject();
            result.put("success", SUCC_STORED.get());
            result.put("failed", FAIL_STORED.get());
            query.sendReply(new StringBuilder(result.toString()));
        }catch (JSONException e){
            query.sendReply(new StringBuilder().append("{success:").append(SUCC_STORED.get())
                    .append(",failed:").append(FAIL_STORED.get()).append("}"));
        }finally {
            SUCC_STORED.set(0);
            FAIL_STORED.set(0);
        }

    }

    /**
     *
     * @param request
     * @return
     * @throws Throwable
     */
    private Deferred<Boolean> deferred(PutRequest request) throws Throwable{
        return client.put(request)
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
    }

    /**
     *
     * @param client
     * @param rowkey
     * @param qualifier
     * @param value
     * @return
     * @throws Throwable
     */
    private PutRequest newRequest(HBaseClient client, String rowkey,String qualifier, String value) throws Throwable{
        return new PutRequest(userTableName.getBytes(),
                rowkey.getBytes(), //row-key
                COLUMN_FAMILY.getBytes(),  //column family
                qualifier.getBytes(),  //qualifier
                value.getBytes()
        );
    }

    /**
     *
     * @param client
     * @param rowkey
     * @param qualifier
     * @param value
     * @return
     * @throws Throwable
     */
    private PutRequest newRequest(HBaseClient client, String rowkey, String qualifier, byte[] value) throws Throwable{
        return new PutRequest(userTableName.getBytes(),
                rowkey.getBytes(), //row-key
                COLUMN_FAMILY.getBytes(),  //column family
                qualifier.getBytes(),  //qualifier
                value
        );
    }

    /**
     *
     * @param client
     * @param rowkey
     * @param qualifier
     * @param value
     * @param timestamp
     * @return
     * @throws Throwable
     */
    private PutRequest newRequest(HBaseClient client, String rowkey,String qualifier, String value, Long timestamp) throws Throwable{
        return new PutRequest(userTableName.getBytes(),
                rowkey.getBytes(), //row-key
                COLUMN_FAMILY.getBytes(),  //column family
                qualifier.getBytes(),  //qualifier
                value.getBytes(),
                timestamp
        );
    }


    /**
     * 加载roles列表
     * @param roleUrisMapping
     */
    private void loadRoles(String roleUrisMapping){
        LOG.info("--------------load defined roles------------");
        try {
            JSONArray array = new JSONArray(roleUrisMapping);
            roles = new HashSet<String>();
            for (int i = 0; i < array.length(); i++) {
                String role = array.getJSONObject(i).getString("role");
                roles.add(role);
            }
        }catch (JSONException e){
            throw new IllegalArgumentException("role and uri format error ");
        }
    }

    /**
     *  生成N位随机字符串
     * @param length
     * @return
     */
    private  String getRandomString(int length) { //length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-=+_|ABCDEFGHIJKLMNOPQRSTUVWXYZ:,.?";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }
}
