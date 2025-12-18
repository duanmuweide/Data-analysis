import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HiveDAO {
    
    /**
     * 获取按城市和犯罪类型统计的数据
     */
    public List<CityCrimeType> getCrimeByCityType() throws SQLException {
        List<CityCrimeType> results = new ArrayList<>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConfig.getHiveConnection();
            String sql = "SELECT city, crime_name1, crime_count, avg_victims " +
                        "FROM crime_by_city_type " +
                        "WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_by_city_type') " +
                        "ORDER BY city, crime_count DESC";
            
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            
            while (rs.next()) {
                CityCrimeType data = new CityCrimeType();
                data.setCity(rs.getString("city"));
                data.setCrimeName(rs.getString("crime_name1"));
                data.setCrimeCount(rs.getInt("crime_count"));
                data.setAvgVictims(rs.getDouble("avg_victims"));
                results.add(data);
            }
        } finally {
            if (rs != null) rs.close();
            if (pstmt != null) pstmt.close();
            DatabaseConfig.closeConnection(conn);
        }
        
        return results;
    }
    
    /**
     * 获取每月犯罪率最高的前5个城市
     */
    public List<TopCrimeCity> getTopCrimeCitiesMonthly() throws SQLException {
        List<TopCrimeCity> results = new ArrayList<>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConfig.getHiveConnection();
            String sql = "SELECT year, month, city, crime_count, rank " +
                        "FROM top_crime_cities_monthly " +
                        "WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'top_crime_cities_monthly') " +
                        "ORDER BY year DESC, month DESC, rank";
            
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            
            while (rs.next()) {
                TopCrimeCity data = new TopCrimeCity();
                data.setYear(rs.getInt("year"));
                data.setMonth(rs.getInt("month"));
                data.setCity(rs.getString("city"));
                data.setCrimeCount(rs.getInt("crime_count"));
                data.setRank(rs.getInt("rank"));
                results.add(data);
            }
        } finally {
            if (rs != null) rs.close();
            if (pstmt != null) pstmt.close();
            DatabaseConfig.closeConnection(conn);
        }
        
        return results;
    }
    
    /**
     * 获取犯罪时间模式数据
     */
    public List<CrimeTimePattern> getCrimeTimePatterns() throws SQLException {
        List<CrimeTimePattern> results = new ArrayList<>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConfig.getHiveConnection();
            String sql = "SELECT hour_of_day, crime_count, affected_cities, avg_duration_minutes " +
                        "FROM crime_time_patterns " +
                        "WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_time_patterns') " +
                        "ORDER BY hour_of_day";
            
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            
            while (rs.next()) {
                CrimeTimePattern data = new CrimeTimePattern();
                data.setHourOfDay(rs.getInt("hour_of_day"));
                data.setCrimeCount(rs.getInt("crime_count"));
                data.setAffectedCities(rs.getInt("affected_cities"));
                data.setAvgDurationMinutes(rs.getDouble("avg_duration_minutes"));
                results.add(data);
            }
        } finally {
            if (rs != null) rs.close();
            if (pstmt != null) pstmt.close();
            DatabaseConfig.closeConnection(conn);
        }
        
        return results;
    }
    
    /**
     * 获取按地点类型统计的犯罪数据
     */
    public List<CrimeByPlaceType> getCrimeByPlaceType() throws SQLException {
        List<CrimeByPlaceType> results = new ArrayList<>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConfig.getHiveConnection();
            String sql = "SELECT place, crime_count, avg_victims, percentage " +
                        "FROM crime_by_place_type " +
                        "WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_by_place_type') " +
                        "ORDER BY crime_count DESC";
            
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            
            while (rs.next()) {
                CrimeByPlaceType data = new CrimeByPlaceType();
                data.setPlace(rs.getString("place"));
                data.setCrimeCount(rs.getInt("crime_count"));
                data.setAvgVictims(rs.getDouble("avg_victims"));
                data.setPercentage(rs.getDouble("percentage"));
                results.add(data);
            }
        } finally {
            if (rs != null) rs.close();
            if (pstmt != null) pstmt.close();
            DatabaseConfig.closeConnection(conn);
        }
        
        return results;
    }
    
    /**
     * 获取犯罪趋势分析数据
     */
    public List<CrimeTrend> getCrimeTrendAnalysis() throws SQLException {
        List<CrimeTrend> results = new ArrayList<>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            conn = DatabaseConfig.getHiveConnection();
            String sql = "SELECT year, month, city, monthly_crime, previous_month, change_percentage " +
                        "FROM crime_trend_analysis " +
                        "WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_trend_analysis') " +
                        "ORDER BY year DESC, month DESC, city";
            
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            
            while (rs.next()) {
                CrimeTrend data = new CrimeTrend();
                data.setYear(rs.getInt("year"));
                data.setMonth(rs.getInt("month"));
                data.setCity(rs.getString("city"));
                data.setMonthlyCrime(rs.getInt("monthly_crime"));
                data.setPreviousMonth(rs.getInt("previous_month"));
                data.setChangePercentage(rs.getDouble("change_percentage"));
                results.add(data);
            }
        } finally {
            if (rs != null) rs.close();
            if (pstmt != null) pstmt.close();
            DatabaseConfig.closeConnection(conn);
        }
        
        return results;
    }
    
    // 数据模型类
    public static class CityCrimeType {
        private String city;
        private String crimeName;
        private int crimeCount;
        private double avgVictims;
        
        // getter和setter方法
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
        public String getCrimeName() { return crimeName; }
        public void setCrimeName(String crimeName) { this.crimeName = crimeName; }
        public int getCrimeCount() { return crimeCount; }
        public void setCrimeCount(int crimeCount) { this.crimeCount = crimeCount; }
        public double getAvgVictims() { return avgVictims; }
        public void setAvgVictims(double avgVictims) { this.avgVictims = avgVictims; }
    }
    
    public static class TopCrimeCity {
        private int year;
        private int month;
        private String city;
        private int crimeCount;
        private int rank;
        
        // getter和setter方法
        public int getYear() { return year; }
        public void setYear(int year) { this.year = year; }
        public int getMonth() { return month; }
        public void setMonth(int month) { this.month = month; }
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
        public int getCrimeCount() { return crimeCount; }
        public void setCrimeCount(int crimeCount) { this.crimeCount = crimeCount; }
        public int getRank() { return rank; }
        public void setRank(int rank) { this.rank = rank; }
    }
    
    public static class CrimeTimePattern {
        private int hourOfDay;
        private int crimeCount;
        private int affectedCities;
        private double avgDurationMinutes;
        
        // getter和setter方法
        public int getHourOfDay() { return hourOfDay; }
        public void setHourOfDay(int hourOfDay) { this.hourOfDay = hourOfDay; }
        public int getCrimeCount() { return crimeCount; }
        public void setCrimeCount(int crimeCount) { this.crimeCount = crimeCount; }
        public int getAffectedCities() { return affectedCities; }
        public void setAffectedCities(int affectedCities) { this.affectedCities = affectedCities; }
        public double getAvgDurationMinutes() { return avgDurationMinutes; }
        public void setAvgDurationMinutes(double avgDurationMinutes) { this.avgDurationMinutes = avgDurationMinutes; }
    }
    
    public static class CrimeByPlaceType {
        private String place;
        private int crimeCount;
        private double avgVictims;
        private double percentage;
        
        // getter和setter方法
        public String getPlace() { return place; }
        public void setPlace(String place) { this.place = place; }
        public int getCrimeCount() { return crimeCount; }
        public void setCrimeCount(int crimeCount) { this.crimeCount = crimeCount; }
        public double getAvgVictims() { return avgVictims; }
        public void setAvgVictims(double avgVictims) { this.avgVictims = avgVictims; }
        public double getPercentage() { return percentage; }
        public void setPercentage(double percentage) { this.percentage = percentage; }
    }
    
    public static class CrimeTrend {
        private int year;
        private int month;
        private String city;
        private int monthlyCrime;
        private int previousMonth;
        private double changePercentage;
        
        // getter和setter方法
        public int getYear() { return year; }
        public void setYear(int year) { this.year = year; }
        public int getMonth() { return month; }
        public void setMonth(int month) { this.month = month; }
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
        public int getMonthlyCrime() { return monthlyCrime; }
        public void setMonthlyCrime(int monthlyCrime) { this.monthlyCrime = monthlyCrime; }
        public int getPreviousMonth() { return previousMonth; }
        public void setPreviousMonth(int previousMonth) { this.previousMonth = previousMonth; }
        public double getChangePercentage() { return changePercentage; }
        public void setChangePercentage(double changePercentage) { this.changePercentage = changePercentage; }
    }
}