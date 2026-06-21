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

 Date: 18/12/2025 19:55:15
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for human_impact_ranking_analysis
-- ----------------------------
DROP TABLE IF EXISTS `human_impact_ranking_analysis`;
CREATE TABLE `human_impact_ranking_analysis`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `year` int NOT NULL,
  `fips` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `human_impact_score` decimal(20, 6) NOT NULL,
  `impact_rank` int NOT NULL,
  `hid` int NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 60 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
