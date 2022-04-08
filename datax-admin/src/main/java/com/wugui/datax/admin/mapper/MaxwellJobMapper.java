package com.wugui.datax.admin.mapper;

import com.wugui.datax.admin.entity.JobUser;
import com.wugui.datax.admin.entity.MaxwellJob;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author xuxueli 2019-05-04 16:44:59
 */
@Mapper
@Repository
public interface MaxwellJobMapper {

    List<MaxwellJob> pageList(@Param("offset") int offset,
                              @Param("pagesize") int pagesize);

    List<MaxwellJob> findAll();

    int pageListCount(@Param("offset") int offset,
                      @Param("pagesize") int pagesize);

//    JobUser loadByUserName(@Param("username") String username);
//
//    JobUser getUserById(@Param("id") int id);
//
//    List<JobUser> getUsersByIds(@Param("ids") String[] ids);

    int save(MaxwellJob maxwellJob);

    int update(MaxwellJob maxwellJob);

    int delete(@Param("id") int id);

}

