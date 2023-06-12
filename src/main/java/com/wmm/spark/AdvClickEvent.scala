package com.wmm.spark

import java.time.LocalDateTime

/**
 * 广告点击事件
 */
case class AdvClickEvent(clickTime: LocalDateTime, province: String, city: String, user: String, url: String)