/*
Navicat MySQL Data Transfer

Source Server         : zlj
Source Server Version : 50638
Source Host           : localhost:3306
Source Database       : geologicmessage

Target Server Type    : MYSQL
Target Server Version : 50638
File Encoding         : 65001

Date: 2018-03-20 10:28:18
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for device_history_data
-- ----------------------------
DROP TABLE IF EXISTS `device_history_data`;
CREATE TABLE `device_history_data` (
  `id` varchar(128) NOT NULL COMMENT '系统唯一标识',
  `monitoring_area` varchar(64) NOT NULL COMMENT '监测区域',
  `convergence` double DEFAULT NULL COMMENT '收敛参数1',
  `crack` double DEFAULT NULL COMMENT '裂缝参数1',
  `displacement` double DEFAULT NULL COMMENT '水平位移参数1',
  `hydraulic_pressure` double DEFAULT NULL COMMENT '水压参数1水压',
  `moisture` double DEFAULT NULL COMMENT '土体含水参数1含水率',
  `mud_depth` double DEFAULT NULL COMMENT '泥位参数1泥位',
  `osmotic_pressure` double DEFAULT NULL COMMENT '渗压参数1渗压',
  `rainfall` double DEFAULT NULL COMMENT '降雨量参数1降雨量',
  `settlement` double DEFAULT NULL COMMENT '沉降参数1沉降',
  `soil_pressure` double DEFAULT NULL COMMENT '土压参数1土压',
  `temperature` double DEFAULT NULL COMMENT '温度参数1温度',
  `voltage` double DEFAULT NULL COMMENT '电压',
  `water_level` double DEFAULT NULL COMMENT '水位',
  `gps` double DEFAULT NULL,
  `tilt` double DEFAULT NULL COMMENT '深度位移a方向倾斜',
  `clinometer` double DEFAULT NULL COMMENT '测斜绳参数1俯仰角',
  `result` char(1) NOT NULL COMMENT '预警等级',
  `coltime` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
