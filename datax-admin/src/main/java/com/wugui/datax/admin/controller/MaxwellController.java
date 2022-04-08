package com.wugui.datax.admin.controller;

import cn.hutool.core.util.StrUtil;
import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datax.admin.core.util.I18nUtil;
import com.wugui.datax.admin.entity.JobUser;
import com.wugui.datax.admin.entity.MaxwellJob;
import com.wugui.datax.admin.mapper.JobUserMapper;
import com.wugui.datax.admin.mapper.MaxwellJobMapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wugui.datatx.core.biz.model.ReturnT.FAIL_CODE;

@RestController
@RequestMapping("/api/maxwell")
@Api(tags = "maxwell接口")
public class MaxwellController {

    @Resource
    private MaxwellJobMapper maxwellJobMapper;

    @Resource
    private BCryptPasswordEncoder bCryptPasswordEncoder;


    @GetMapping("/pageList")
    @ApiOperation("maxwell任务列表")
    public ReturnT<Map<String, Object>> pageList(@RequestParam(required = false, defaultValue = "1") int current,
                                                 @RequestParam(required = false, defaultValue = "10") int size) {

        // page list
        List<MaxwellJob> list = maxwellJobMapper.pageList((current - 1) * size, size);
        int recordsTotal = maxwellJobMapper.pageListCount((current - 1) * size, size);

        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("recordsTotal", recordsTotal);        // 总记录数
        maps.put("recordsFiltered", recordsTotal);    // 过滤后的总记录数
        maps.put("data", list);                    // 分页列表
        return new ReturnT<>(maps);
    }

    @GetMapping("/list")
    @ApiOperation("maxwell列表")
    public ReturnT<List<MaxwellJob>> list() {

        // page list
        List<MaxwellJob> list = maxwellJobMapper.findAll();
        return new ReturnT<>(list);
    }

//    @GetMapping("/getUserById")
//    @ApiOperation(value = "根据id获取用户")
//    public ReturnT<JobUser> selectById(@RequestParam("userId") Integer userId) {
//        return new ReturnT<>(jobUserMapper.getUserById(userId));
//    }
//
    @PostMapping("/add")
    @ApiOperation("添加job")
    public ReturnT<String> add(@RequestBody MaxwellJob maxwellJob) {

//        // valid username
//        if (!StringUtils.hasText(maxwellJob.getUsername())) {
//            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_please_input") + I18nUtil.getString("user_username"));
//        }
//        jobUser.setUsername(jobUser.getUsername().trim());
//        if (!(maxwellJob.getUsername().length() >= 4 && jobUser.getUsername().length() <= 20)) {
//            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_length_limit") + "[4-20]");
//        }
        // valid password
//        if (!StringUtils.hasText(jobUser.getPassword())) {
//            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_please_input") + I18nUtil.getString("user_password"));
//        }
//        jobUser.setPassword(jobUser.getPassword().trim());
//        if (!(jobUser.getPassword().length() >= 4 && jobUser.getPassword().length() <= 20)) {
//            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_length_limit") + "[4-20]");
//        }
//        jobUser.setPassword(bCryptPasswordEncoder.encode(jobUser.getPassword()));
//
//
//        // check repeat
//        JobUser existUser = jobUserMapper.loadByUserName(jobUser.getUsername());
//        if (existUser != null) {
//            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("user_username_repeat"));
//        }

        // write
        maxwellJobMapper.save(maxwellJob);
        return ReturnT.SUCCESS;
    }
//
    @PostMapping(value = "/update")
    @ApiOperation("更新job信息")
    public ReturnT<String> update(@RequestBody MaxwellJob maxwellJob) {

        if (StrUtil.isBlank(maxwellJob.getMysqlHost())) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_no_blank") + "mysql");
        }
        if (StrUtil.isBlank(maxwellJob.getMysqlUser())) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_no_blank") + "mysql");
        }
        if (StrUtil.isBlank(maxwellJob.getMysqlPassword())) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_no_blank") + "mysql");
        }
        if (StrUtil.isBlank(maxwellJob.getKafkaServer())) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_no_blank") + "kafka");
        }
        if (StrUtil.isBlank(maxwellJob.getKafkaTopic())) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_no_blank") + "kafka");
        }


        // write
        maxwellJobMapper.update(maxwellJob);
        return ReturnT.SUCCESS;
    }

    @RequestMapping(value = "/remove", method = RequestMethod.POST)
    @ApiOperation("删除job")
    public ReturnT<String> remove(int id) {
        int result = maxwellJobMapper.delete(id);
        return result != 1 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }
//
//    @PostMapping(value = "/updatePwd")
//    @ApiOperation("修改密码")
//    public ReturnT<String> updatePwd(@RequestBody JobUser jobUser) {
//        String password = jobUser.getPassword();
//        if (password == null || password.trim().length() == 0) {
//            return new ReturnT<>(ReturnT.FAIL.getCode(), "密码不可为空");
//        }
//        password = password.trim();
//        if (!(password.length() >= 4 && password.length() <= 20)) {
//            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_length_limit") + "[4-20]");
//        }
//        // do write
//        JobUser existUser = jobUserMapper.loadByUserName(jobUser.getUsername());
//        existUser.setPassword(bCryptPasswordEncoder.encode(password));
//        jobUserMapper.update(existUser);
//        return ReturnT.SUCCESS;
//    }

}
