package com.nanfeng.gmall.realtime.bean

/**
  * @author: nanfeng
  * @date: 2021/7/29 22:31
  * @description:
  */
case class UserInfo(
                     id:String,
                     user_level:String,
                     birthday:String,
                     gender:String,
                     var age_group:String,//年龄段
                     var gender_name:String //性别
                   )
