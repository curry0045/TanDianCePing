package com.hmdp.controller;


import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/blog")
public class BlogController {

    @Resource
    private IBlogService blogService;


    @PostMapping
    public Result saveBlog(@RequestBody Blog blog) {
        return blogService.saveBlog(blog);
    }

    //点赞功能
    @PutMapping("/like/{id}")
    public Result likeBlog(@PathVariable("id") Long id) {
        // 修改点赞数量 update tb_blog set liked = liked + 1 where id = ?
//        blogService.update()
//                .setSql("liked = liked + 1").eq("id", id).update();
        return blogService.likeBlog(id);
    }

    @GetMapping("/of/me")
    public Result queryMyBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .eq("user_id", user.getId()).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }


    @GetMapping("/hot")
    public Result queryHotBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        return blogService.queryHotBlog(current);
    }

    @GetMapping("/{id}")
    public Result queryById(@PathVariable("id") Long id){
        return blogService.queryBlogById(id);
    }

    //点赞展示列表实现
    @GetMapping("/likes/{id}")
    public Result queryBlogLikes(@PathVariable("id") Long id){
        return blogService.queryBlogLikes(id);
    }

    // 根据id查询该id的发的blog
    @GetMapping("/of/user")
    public Result queryBlogByUserId(
            //这里current为当前页码 默认值为1
            @RequestParam(value = "current", defaultValue = "1") Integer current,
            @RequestParam("id") Long id) {
        // 根据用户查询    page (new Page<>(current, SystemConstants.MAX_PAGE_SIZE))表示分页查询 当前页码 current 和每页最大条数
        Page<Blog> page = blogService.query()
                .eq("user_id", id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页的博客列表
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    /**
     * 滚动查询关注的人发出的新blog
     * @param max 上一次查询的最小时间戳
     * @param offset 偏移量
     * @return
     */
    @GetMapping("/of/follow")
    public Result queryBlogOfFollow(@RequestParam("lastId")Long max,@RequestParam(value = "offset", defaultValue = "0") Integer offset){
        return blogService.queryBlogOfFollw(max,offset);
    }
}
