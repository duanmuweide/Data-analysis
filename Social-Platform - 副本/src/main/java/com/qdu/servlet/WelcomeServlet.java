package com.qdu.servlet;


import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import java.io.IOException;
import java.sql.*;

@WebServlet("/welcome")
public class WelcomeServlet extends HttpServlet {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/data?characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai";
    private static final String DB_USER = "data";
    private static final String DB_PASSWORD = "data";

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        HttpSession session = request.getSession(false);
        if (session == null || session.getAttribute("userId") == null) {
            response.sendRedirect("index.jsp");
            return;
        }

        Integer userId = (Integer) session.getAttribute("userId");
        String username = (String) session.getAttribute("username");
        Integer vipsignal = (Integer) session.getAttribute("vipsignal");

        StringBuilder htmlBuilder = new StringBuilder();

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            pstmt = conn.prepareStatement("SELECT data_name, jump_url, vipsignal FROM datalist ORDER BY id");
            rs = pstmt.executeQuery();

            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                String name = rs.getString("data_name");
                String url = rs.getString("jump_url");
                int itemVip = rs.getInt("vipsignal");

                if (itemVip == 2 && (vipsignal == null || vipsignal != 2)) {
                    // 普通用户访问 VIP 报表 → 灰色不可点击
                    htmlBuilder.append("<li class=\"data-item\">")
                            .append("<span style=\"color:#94a3b8; cursor:not-allowed;\">")
                            .append(escapeHtml(name))
                            .append(" <span style=\"background:#fbbf24;color:#78350f;padding:2px 6px;border-radius:4px;font-size:12px;\">VIP</span>")
                            .append("</span></li>");
                } else {
                    // 可点击链接
                    htmlBuilder.append("<li class=\"data-item\">")
                            .append("<a href=\"").append(escapeHtml(url)).append("\" target=\"_blank\" class=\"data-link\">")
                            .append(escapeHtml(name));
                    if (itemVip == 2) {
                        htmlBuilder.append(" <span style=\"background:#fbbf24;color:#78350f;padding:2px 6px;border-radius:4px;font-size:12px;\">VIP</span>");
                    }
                    htmlBuilder.append("</a></li>");
                }
            }

            if (!hasData) {
                htmlBuilder.append("<li class=\"data-item\">暂无数据报表</li>");
            }

        } catch (Exception e) {
            e.printStackTrace();
            htmlBuilder.setLength(0); // 清空
            htmlBuilder.append("<li class=\"data-item\">⚠️ 加载数据失败，请联系管理员。</li>");
        } finally {
            try {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
                if (conn != null) conn.close();
            } catch (SQLException ignored) {}
        }

        // 将数据传给 JSP
        request.setAttribute("username", username);
        request.setAttribute("dataListHtml", htmlBuilder.toString());

        request.getRequestDispatcher("welcome.jsp").forward(request, response);
    }

    // 简单的 HTML 转义，防止 XSS（基础防护）
    private String escapeHtml(String input) {
        if (input == null) return "";
        return input.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#x27;");
    }
}