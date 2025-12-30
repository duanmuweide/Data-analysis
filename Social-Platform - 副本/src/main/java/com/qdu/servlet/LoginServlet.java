package com.qdu.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import java.io.IOException;
import java.sql.*;

@WebServlet("/login")
public class LoginServlet extends HttpServlet {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/data?characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai";
    private static final String DB_USER = "data";
    private static final String DB_PASSWORD = "data";

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String idStr = request.getParameter("id");
        String password = request.getParameter("password");

        if (idStr == null || idStr.trim().isEmpty() || password == null) {
            request.setAttribute("error", "请输入用户ID和密码");
            request.getRequestDispatcher("index.jsp").forward(request, response);
            return;
        }

        int userId;
        try {
            userId = Integer.parseInt(idStr);
        } catch (NumberFormatException e) {
            request.setAttribute("error", "用户ID必须是数字");
            request.getRequestDispatcher("index.jsp").forward(request, response);
            return;
        }

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            String sql = "SELECT id, username, password, vipsignal FROM users WHERE id = ?";
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, userId);
            rs = pstmt.executeQuery();

            if (rs.next()) {
                String storedPassword = rs.getString("password");
                if (password.equals(storedPassword)) {
                    HttpSession session = request.getSession();
                    session.setAttribute("userId", userId);
                    session.setAttribute("username", rs.getString("username"));
                    session.setAttribute("vipsignal", rs.getInt("vipsignal"));

                    // 登录成功后跳转到 WelcomeServlet
                    response.sendRedirect("welcome");
                    return;
                }
            }
            request.setAttribute("error", "用户ID或密码错误");
        } catch (Exception e) {
            e.printStackTrace();
            request.setAttribute("error", "系统错误，请稍后再试");
        } finally {
            try {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
                if (conn != null) conn.close();
            } catch (SQLException ignored) {}
        }

        request.getRequestDispatcher("index.jsp").forward(request, response);
    }
}