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

 Date: 18/12/2025 19:32:15
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for phosphorus_pollution_by_basin_area
-- ----------------------------
DROP TABLE IF EXISTS `phosphorus_pollution_by_basin_area`;
CREATE TABLE `phosphorus_pollution_by_basin_area`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `area_grade` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `basin_count` int NOT NULL,
  `farm_fert_total` decimal(20, 6) NOT NULL,
  `manure_total` decimal(20, 6) NOT NULL,
  `point_source_total` decimal(20, 6) NOT NULL,
  `farm_fert_ratio` decimal(20, 6) NOT NULL,
  `manure_ratio` decimal(20, 6) NOT NULL,
  `point_source_ratio` decimal(20, 6) NOT NULL,
  `hid` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
