<%--
  Created by IntelliJ IDEA.
  User: 24437
  Date: 2025/12/26
  Time: 下午3:55
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    Integer userId = (Integer) session.getAttribute("userId");
    String username = (String) session.getAttribute("username");

    if (userId == null) {
        response.sendRedirect("index.jsp");
        return;
    }
%>
<html>
<head>
    <title>欢迎</title>
    <style>
        body { margin: 0; padding: 0; font-family: Arial, sans-serif; }
        .user-info {
            position: absolute;
            top: 10px;
            left: 10px;
            background: #e9ecef;
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 14px;
        }
        .main { text-align: center; margin-top: 80px; }
    </style>
</head>
<body>
<div class="user-info">
    ID: <%= userId %> | 用户名: <%= username %>
</div>
<div class="main">
    <h2>登录成功！</h2>
    <p>欢迎，<%= username %>！</p>
    <a href="logout">退出登录</a>
</div>
</body>
</html>