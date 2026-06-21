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

 Date: 12/12/2025 21:06:29
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for watershed_surplus_trend
-- ----------------------------
DROP TABLE IF EXISTS `watershed_surplus_trend`;
CREATE TABLE `watershed_surplus_trend`  (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `fips` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `year` int NOT NULL,
  `n_surplus` decimal(20, 6) NOT NULL,
  `p_surplus` decimal(20, 6) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
