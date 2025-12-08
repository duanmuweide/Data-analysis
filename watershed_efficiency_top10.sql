/*
 Navicat Premium Data Transfer

 Source Server         : localhost_3306
 Source Server Type    : MySQL
 Source Server Version : 80021
 Source Host           : localhost:3306
 Source Schema         : american_data_analysis

 Target Server Type    : MySQL
 Target Server Version : 80021
 File Encoding         : 65001

 Date: 08/12/2025 19:53:07
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for watershed_efficiency_top10
-- ----------------------------
DROP TABLE IF EXISTS `watershed_efficiency_top10`;
CREATE TABLE `watershed_efficiency_top10`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `year` int NOT NULL,
  `fips` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `n_efficiency` decimal(10, 6) NULL DEFAULT NULL,
  `p_efficiency` decimal(10, 6) NULL DEFAULT NULL,
  `avg_efficiency` decimal(10, 6) NULL DEFAULT NULL,
  `ranks` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
