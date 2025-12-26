-- MySQL表结构：用于存储每日高风险犯罪数据
CREATE TABLE IF NOT EXISTS crime_analysis.daily_high_risk_crimes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50),
    zip_code VARCHAR(20),
    high_risk_count INT NOT NULL,
    avg_victims DECIMAL(5,2),
    latest_incident DATETIME,
    center_lat DECIMAL(10,6),
    center_lon DECIMAL(10,6),
    total_incidents INT NOT NULL,
    high_risk_percentage DECIMAL(5,2),
    export_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引优化查询
CREATE INDEX idx_city ON daily_high_risk_crimes(city);
CREATE INDEX idx_state ON daily_high_risk_crimes(state);
CREATE INDEX idx_export_date ON daily_high_risk_crimes(export_date);

-- MySQL表结构：用于存储犯罪趋势分析数据
CREATE TABLE IF NOT EXISTS crime_analysis.crime_trend_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT NOT NULL,
    total_crimes INT NOT NULL,
    total_victims INT NOT NULL,
    avg_victims_per_crime DECIMAL(5,2),
    affected_cities INT NOT NULL,
    affected_states INT NOT NULL,
    crime_growth_rate VARCHAR(20),
    victim_growth_rate VARCHAR(20),
    peak_month_crimes INT,
    lowest_month_crimes INT,
    export_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_year ON crime_trend_analysis(year);
CREATE INDEX idx_export_date ON crime_trend_analysis(export_date);

-- MySQL表结构：用于存储月度犯罪趋势分析数据
CREATE TABLE IF NOT EXISTS crime_analysis.monthly_crime_trends (
    id INT AUTO_INCREMENT PRIMARY KEY,
    report_month DATE NOT NULL,
    total_crimes INT NOT NULL,
    total_victims INT NOT NULL,
    avg_victims_per_crime DECIMAL(10,2),
    affected_cities INT,
    affected_states INT,
    crime_growth_rate VARCHAR(20),
    victim_growth_rate VARCHAR(20),
    peak_day_crimes INT,
    lowest_day_crimes INT,
    export_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_report_month ON monthly_crime_trends(report_month);
CREATE INDEX idx_export_date ON monthly_crime_trends(export_date);

-- MySQL表结构：用于存储受害者人口统计数据
CREATE TABLE IF NOT EXISTS crime_analysis.victim_demographics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    victim_category VARCHAR(50) NOT NULL,
    incident_count INT NOT NULL,
    total_victims INT NOT NULL,
    avg_victims DECIMAL(5,2),
    min_victims INT,
    max_victims INT,
    category_percentage DECIMAL(5,2),
    victim_percentage DECIMAL(5,2),
    highest_victim_state VARCHAR(50),
    highest_state_avg_victims DECIMAL(5,2),
    overall_avg_victims DECIMAL(5,2),
    overall_min_victims INT,
    overall_max_victims INT,
    overall_median_victims DECIMAL(5,2),
    overall_90th_percentile DECIMAL(5,2),
    export_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_category ON victim_demographics(victim_category);
CREATE INDEX idx_export_date ON victim_demographics(export_date);

-- MySQL表结构：用于存储地理分布分析数据
CREATE TABLE IF NOT EXISTS crime_analysis.geographic_distribution (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(50) NOT NULL,
    state_crimes INT NOT NULL,
    state_victims INT NOT NULL,
    cities_affected INT NOT NULL,
    zip_codes_affected INT NOT NULL,
    avg_victims_per_crime DECIMAL(5,2),
    victim_stddev DECIMAL(10,2),
    state_crime_rank INT,
    state_victim_rank INT,
    state_city_count_rank INT,
    top_city_in_state VARCHAR(100),
    top_city_crimes INT,
    overall_top_city VARCHAR(100),
    overall_top_city_crimes INT,
    highest_crime_state VARCHAR(50),
    highest_state_crimes INT,
    crime_level VARCHAR(20),
    export_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_state ON geographic_distribution(state);
CREATE INDEX idx_export_date ON geographic_distribution(export_date);

-- MySQL表结构：用于存储时间模式分析数据
CREATE TABLE IF NOT EXISTS crime_analysis.time_pattern_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hour_of_day INT NOT NULL,
    hourly_crimes INT NOT NULL,
    hourly_victims INT NOT NULL,
    avg_victims_per_hour DECIMAL(5,2),
    states_affected INT NOT NULL,
    hour_crime_rank INT,
    hour_victim_rank INT,
    peak_day_name VARCHAR(20),
    peak_day_crimes INT,
    peak_quarter VARCHAR(20),
    peak_quarter_crimes INT,
    time_period VARCHAR(20),
    crime_intensity VARCHAR(20),
    export_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_hour ON time_pattern_analysis(hour_of_day);
CREATE INDEX idx_export_date ON time_pattern_analysis(export_date);
