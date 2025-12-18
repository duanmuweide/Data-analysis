import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

public class CrimeAnalysisServer {
    
    private static final int PORT = 8080;
    
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final HiveDAO hiveDAO = new HiveDAO();
    
    public static void main(String[] args) throws IOException {
        // 创建HTTP服务器
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        
        // 配置线程池
        server.setExecutor(Executors.newFixedThreadPool(10));
        
        // 注册API端点
        server.createContext("/api/crime-by-city-type", new CrimeByCityTypeHandler());
        server.createContext("/api/top-crime-cities-monthly", new TopCrimeCitiesMonthlyHandler());
        server.createContext("/api/crime-time-patterns", new CrimeTimePatternsHandler());
        server.createContext("/api/crime-by-place-type", new CrimeByPlaceTypeHandler());
        server.createContext("/api/crime-trend-analysis", new CrimeTrendAnalysisHandler());
        server.createContext("/", new FileHandler());
        
        // 启动服务器
        server.start();
        System.out.println("Crime Analysis Server started on port " + PORT);
        System.out.println("Visit http://localhost:" + PORT + "/index.html to view the dashboard");
    }
    
    // 响应处理类
    private static void sendResponse(HttpExchange exchange, int statusCode, Object data) throws IOException {
        String response = mapper.writeValueAsString(data);

        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, response.getBytes().length);
        
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }
    
    // API处理类
    static class CrimeByCityTypeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    sendResponse(exchange, 200, hiveDAO.getCrimeByCityType());
                } catch (Exception e) {
                    sendResponse(exchange, 500, "Error: " + e.getMessage());
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }
    
    static class TopCrimeCitiesMonthlyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    sendResponse(exchange, 200, hiveDAO.getTopCrimeCitiesMonthly());
                } catch (Exception e) {
                    sendResponse(exchange, 500, "Error: " + e.getMessage());
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }
    
    static class CrimeTimePatternsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    sendResponse(exchange, 200, hiveDAO.getCrimeTimePatterns());
                } catch (Exception e) {
                    sendResponse(exchange, 500, "Error: " + e.getMessage());
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }
    
    static class CrimeByPlaceTypeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    sendResponse(exchange, 200, hiveDAO.getCrimeByPlaceType());
                } catch (Exception e) {
                    sendResponse(exchange, 500, "Error: " + e.getMessage());
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }
    
    static class CrimeTrendAnalysisHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                try {
                    sendResponse(exchange, 200, hiveDAO.getCrimeTrendAnalysis());
                } catch (Exception e) {
                    sendResponse(exchange, 500, "Error: " + e.getMessage());
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }
    
    static class FileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/")) {
                path = "/index.html";
            }
            
            // 从文件系统读取静态资源
            String filePath = System.getProperty("user.dir") + path;
            
            // 设置响应头
            String contentType = getContentType(filePath);
            exchange.getResponseHeaders().set("Content-Type", contentType + "; charset=UTF-8");
            
            try {
                // 读取文件内容
                byte[] fileContent = Files.readAllBytes(Paths.get(filePath));
                exchange.sendResponseHeaders(200, fileContent.length);
                
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(fileContent);
                }
            } catch (IOException e) {
                // 文件不存在或无法读取
                String errorMessage = "File not found: " + path;
                exchange.sendResponseHeaders(404, errorMessage.getBytes().length);
                
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorMessage.getBytes());
                }
            }
        }
        
        // 获取文件内容类型
        private String getContentType(String filePath) {
            if (filePath.endsWith(".html")) {
                return "text/html";
            } else if (filePath.endsWith(".css")) {
                return "text/css";
            } else if (filePath.endsWith(".js")) {
                return "application/javascript";
            } else if (filePath.endsWith(".json")) {
                return "application/json";
            } else if (filePath.endsWith(".png")) {
                return "image/png";
            } else if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg")) {
                return "image/jpeg";
            } else {
                return "application/octet-stream";
            }
        }
    }
}