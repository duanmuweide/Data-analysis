<%--
  Created by IntelliJ IDEA.
  User: 24437
  Date: 2025/12/26
  Time: 下午5:47
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>数据平台 · 登录</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;500;600&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', Arial, sans-serif;
            background: linear-gradient(135deg, #1a2a6c, #2a4d9e);
            height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            color: #333;
        }
        .login-card {
            background: white;
            width: 380px;
            padding: 32px;
            border-radius: 16px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
            text-align: center;
        }
        .login-card h2 {
            font-weight: 600;
            font-size: 24px;
            margin-bottom: 24px;
            color: #1a2a6c;
        }
        .input-group {
            margin-bottom: 16px;
            text-align: left;
        }
        .input-group label {
            display: block;
            margin-bottom: 6px;
            font-size: 14px;
            color: #555;
            font-weight: 500;
        }
        .input-group input {
            width: 100%;
            padding: 12px;
            border: 1px solid #ddd;
            border-radius: 8px;
            font-size: 16px;
            transition: border-color 0.3s;
        }
        .input-group input:focus {
            outline: none;
            border-color: #2a4d9e;
            box-shadow: 0 0 0 2px rgba(42, 77, 158, 0.2);
        }
        .btn-login {
            width: 100%;
            padding: 12px;
            background: #1a2a6c;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.3s;
        }
        .btn-login:hover {
            background: #2a4d9e;
        }
        .error {
            color: #e74c3c;
            font-size: 14px;
            margin-top: 16px;
            padding: 10px;
            background: #fdf2f2;
            border-radius: 6px;
            display: inline-block;
            width: 100%;
        }
        .platform-tag {
            margin-top: 20px;
            font-size: 13px;
            color: #777;
        }
    </style>
</head>
<body>
<div class="login-card">
    <h2>📊 数据查询平台</h2>
    <form action="login" method="post">
        <div class="input-group">
            <label for="id">用户 ID</label>
            <input type="number" id="id" name="id" min="1" required placeholder="请输入您的用户ID">
        </div>
        <div class="input-group">
            <label for="password">密码</label>
            <input type="password" id="password" name="password" required placeholder="请输入密码">
        </div>
        <button type="submit" class="btn-login">立即登录</button>
    </form>

    <%
        String error = (String) request.getAttribute("error");
        if (error != null) {
    %>
    <div class="error"><%= error %></div>
    <% } %>

    <div class="platform-tag">安全访问 · 实时数据 · 智能查询</div>
</div>
</body>
</html>