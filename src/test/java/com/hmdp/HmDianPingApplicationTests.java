package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Test
    void testSaveShop() throws InterruptedException {
        shopService.saveShop2Redis(1L,10L);
    }

    @Test
    void loadShopData(){
        //1.查询店铺信息
        List<Shop> list = shopService.list();

        //2.把店铺分组，按照typeId分组，id一致的放到一个集合                         等价于 shop -> shop.getTypeId
        Map<Long,List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        //3.分批完成写入Redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //3.1 获取类型id
            Long typeId = entry.getKey();
            String key = "shop:geo:" + typeId;

            //3.2 获取同类型的店铺的集合
            List<Shop> value = entry.getValue();

            //3.3 写入redis GEOADD key 经度 纬度 member
            for (Shop shop : value) {
                stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(), shop.getY()),shop.getId().toString());
            }
        }

    }
}
