package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component //spring 维护

public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 方法1：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //* 方法2：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        //由于需要设置逻辑过期时间，所以我们需要用到RedisData
        RedisData redisdata = new RedisData();
        //redisData的data就是传进来的value对象
        redisdata.setData(value);
        //逻辑过期时间就是当前时间加上传进来的参数时间，用TimeUnit可以将时间转为秒，随后与当前时间相加
        redisdata.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        //由于是逻辑过期，所以这里不需要设置过期时间，只存一下key和value就好了，同时注意value是ridisData类型
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisdata));
    }

    //方法3：根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
    //返回任意类型不确定，使用泛型
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){//keyPrefix前缀,Function<ID, R> dbFallback:ID参数类型，R返回值类型
        String key = keyPrefix + id;
        //1. 先从Redis中查商铺缓存，这里的常量值是固定的前缀 + 店铺id
        String json = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3. 如果不为空（查询到了），则直接返回
            return JSONUtil.toBean(json, type);
        }
        ////如果查询到的是空字符串，则说明是我们缓存的空数据（缓存穿透相关）
        if(json != null){//不等于null，就是空值
            return null;
        }
        //4. 不存在，否则去数据库中查
        R r = dbFallback.apply(id);
        //5.不存在，返回错误，查不到返回一个错误信息或者返回空都可以，根据自己的需求来
        if (r == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6. 并存入redis
        this.set(key, r, time, unit);
        //7. 最终把信息返回
        return r;
    }



    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){//缓存击穿 包装了原来的queryById
        String key = keyPrefix + id;
        //1. 先从Redis中查商铺缓存，这里的常量值是固定的前缀 + 店铺id
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //2.todo 判断是否存在 是否命中
        if (StrUtil.isBlank(json)) { //3. 如果为空 //不存在 未命中
            //直接返回
            return null;
        }
        //todo  4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        //todo 4.1 将data转为Shop对象
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        //todo 4.2 获取过期时间
        LocalDateTime expireTime = redisData.getExpireTime();

        //todo 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){//过期时间是不是在当前时间之后
            //todo  5.1.未过期，直接返回店铺信息
            return r;
        }
        // 5.2.已过期，需要缓存重建
        //todo 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean flag = tryLock(lockKey);

        //todo  6.2.判断是否获取锁成功
        if(flag){
            //todo 6.3 成功，开始独立线程
            CACHE_REBUILD_EXECUTOR.submit(() -> {

                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 重建缓存
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
                //返回商铺信息
            });
        }
        // 6.4 失败 返回过期的商铺信息
        return r;
    }

    private boolean tryLock(String key){
        //setIfAbsent() 方法的语义如下：
        //如果 Redis 中已经存在指定的键，则不执行任何操作，返回 false。
        //如果 Redis 中不存在指定的键，则将指定的键值对存储到 Redis 中，并返回 true。
        final Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //不要直接返回，会拆箱；
        return BooleanUtil.isTrue(flag);
    }

    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }


}
