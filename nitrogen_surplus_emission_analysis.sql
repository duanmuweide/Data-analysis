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

 Date: 08/12/2025 19:50:01
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for nitrogen_surplus_emission_analysis
-- ----------------------------
DROP TABLE IF EXISTS `nitrogen_surplus_emission_analysis`;
CREATE TABLE `nitrogen_surplus_emission_analysis`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `fips` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `cumulative_surplus` decimal(20, 6) NOT NULL,
  `avg_annual_surplus` decimal(20, 6) NOT NULL,
  `avg_annual_emission` decimal(20, 6) NOT NULL,
  `emission_per_surplus_ratio` decimal(20, 6) NOT NULL,
  `emission_level` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
