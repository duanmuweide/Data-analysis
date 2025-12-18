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

 Date: 18/12/2025 20:20:15
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for nutrient_surplus_atm_dep_corr
-- ----------------------------
DROP TABLE IF EXISTS `nutrient_surplus_atm_dep_corr`;
CREATE TABLE `nutrient_surplus_atm_dep_corr`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `year` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `avg_n_ag_surplus` decimal(20, 6) NOT NULL,
  `avg_n_atm_dep` decimal(20, 6) NOT NULL,
  `corr_coefficient` decimal(20, 6) NOT NULL,
  `hid` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 68 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
