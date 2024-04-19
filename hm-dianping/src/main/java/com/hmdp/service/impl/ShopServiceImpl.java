package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.CreditCodeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import jdk.nashorn.internal.ir.CallNode;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        //todo 工具类演示缓存穿透
        //Function<ID, R> dbFallback :
        //id2 -> getById(id2):函数式编程中的 Lambda 表达式，它将 id2 作为参数传递给 getById 方法，并返回调用 getById 方法后的结果。
        //this::getById 是 Java 中的方法引用（Method Reference），它引用了当前对象（使用 this 关键字）的 getById 方法。 通常情况下，方法引用可以用来简化 Lambda 表达式。
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //todo 缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //todo 互斥锁解决缓存击穿
       // Shop shop = queryWithMutex(id);

        //todo 工具类演示缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //todo 逻辑过期
       // Shop shop = queryWithLogicalExpire(id);


        if(shop == null){
            return Result.fail("店铺不存在！！");
        }
        //8. 最终把查询到的商户信息返回给前端
        return Result.ok(shop);
    }

    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
/*
    public Shop queryWithLogicalExpire(Long id){//缓存穿透 包装了原来的queryById
        String key = CACHE_SHOP_KEY + id;
        //1. 先从Redis中查商铺缓存，这里的常量值是固定的前缀 + 店铺id
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2.todo 判断是否存在 是否命中
        if (StrUtil.isBlank(shopJson)) { //3. 如果为空 //不存在 未命中
            //直接返回
            return null;
        }
        //todo  4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        //todo 4.1 将data转为Shop对象
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        //todo 4.2 获取过期时间
        LocalDateTime expireTime = redisData.getExpireTime();

        //todo 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){//过期时间是不是在当前时间之后
            //todo  5.1.未过期，直接返回店铺信息
            return shop;
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
                    //重建缓存
                    this.saveShop2Redis(id, LOCK_SHOP_TTL);
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
        return shop;

    }
 */


    public Shop queryWithMutex(Long id){//缓存击穿 包装了原来的queryById
        String key = CACHE_SHOP_KEY + id;
        //1. 先从Redis中查商铺缓存，这里的常量值是固定的前缀 + 店铺id
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3. 存在，直接返回
            // 如果不为空（查询到了），则转为Shop类型直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        // 如果查询到的是空字符串，则说明是我们缓存的空数据（缓存穿透相关）
        if(shopJson != null){
            return null;
        }
        //todo 4.实现缓存重建
        //todo 4.1 获取互斥锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);

            //todo 4.2 判断否获取成功
            if(!isLock){
                //todo 4.3 失败，则休眠重试
                Thread.sleep(50);
                //重试
                queryWithMutex(id);

            }
            //4.4 获取互斥锁成功，根据id查询数据库
            shop = getById(id);
            //5.不存在，返回错误，查不到返回一个错误信息或者返回空都可以，根据自己的需求来
            if (shop == null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6. 查到了则转为json字符串
            String jsonStr = JSONUtil.toJsonStr(shop);
            //7. 并存入redis
            stringRedisTemplate.opsForValue().set(key, jsonStr, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
            //todo 8.释放互斥锁
            //try/catch/finally包裹到这里，然后把释放锁的操作放到finally里
            //释放互斥锁
        }finally {
            unlock(lockKey);
        }
        //9. 最终把查询到的商户信息返回给前端
        return shop;
    }



/*
    public Shop queryWithPassThrough(Long id){//缓存穿透 包装了原来的queryById
        String key = CACHE_SHOP_KEY + id;
        //1. 先从Redis中查商铺缓存，这里的常量值是固定的前缀 + 店铺id
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3. 如果不为空（查询到了），则转为Shop类型直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        ////如果查询到的是空字符串，则说明是我们缓存的空数据（缓存穿透相关）
        if(shopJson != null){
            return null;
        }
        //4. 不存在，否则去数据库中查
        Shop shop = getById(id);
        //5.不存在，返回错误，查不到返回一个错误信息或者返回空都可以，根据自己的需求来
        if (shop == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6. 查到了则转为json字符串
        String jsonStr = JSONUtil.toJsonStr(shop);
        //7. 并存入redis
        stringRedisTemplate.opsForValue().set(key, jsonStr, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //8. 最终把查询到的商户信息返回给前端
        return shop;

    }
 */


    //获取锁
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



    //存储店铺数据和逻辑过期时间
    public void saveShop2Redis(Long id, Long expireSeconds){
        //1.查询店铺数据
        Shop shop = getById(id);

        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    public Result update(Shop shop) {
        //首先先判一下空
        if (shop.getId() == null){
            return Result.fail("店铺id不能为空！！");
        }
        //1.更新数据库
        updateById(shop);
        //2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
