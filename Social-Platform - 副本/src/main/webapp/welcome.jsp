<%--
  Created by IntelliJ IDEA.
  User: 24437
  Date: 2025/12/26
  Time: 下午5:46
  To change this template use File | Settings | File Templates.
--%>
<%--
  Created by IntelliJ IDEA.
  User: 24437
  Date: 2025/12/26
  Time: 下午5:46
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>数据平台 · 欢迎</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        /* （样式完全保留，此处省略） */
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', Arial, sans-serif;
            background-color: #f8fafc;
            color: #1e293b;
            min-height: 100vh;
            padding: 20px;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 24px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 32px;
        }
        .user-info { font-size: 15px; color: #475569; }
        .vip-tag {
            background: #fbbf24;
            color: #78350f;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 12px;
            margin-left: 6px;
        }
        .logout-btn {
            background: #ef4444; color: white; border: none; padding: 8px 16px;
            border-radius: 6px; font-weight: 500; text-decoration: none; font-size: 14px;
        }
        .logout-btn:hover { background: #dc2626; }
        .main-content { max-width: 700px; margin: 0 auto; }
        .main-content h1 { font-size: 28px; font-weight: 600; color: #1a2a6c; margin-bottom: 8px; }
        .data-actions {
            background: white; padding: 24px; border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.05); margin-top: 20px;
        }
        .data-actions h2 { margin-bottom: 16px; color: #1e293b; }
        .data-list { list-style: none; }
        .data-item { padding: 12px 0; border-bottom: 1px solid #eee; }
        .data-item:last-child { border-bottom: none; }
        .data-link {
            text-decoration: none; color: #1a2a6c; font-weight: 500; font-size: 16px;
            display: flex; justify-content: space-between; align-items: center;
        }
        .data-link:hover { color: #2a4d9e; }
        .footer-note {
            margin-top: 30px;
            text-align: center;
            font-size: 13px;
            color: #94a3b8;
        }
    </style>
</head>
<body>
<div class="header">
    <div class="user-info">
        👤 欢迎，<strong>${username}</strong>
        <% if (session.getAttribute("vipsignal") != null &&
                ((Integer) session.getAttribute("vipsignal")) == 2) { %>
        <span class="vip-tag">VIP</span>
        <% } %>
    </div>
    <a href="logout" class="logout-btn">退出登录</a>
</div>

<div class="main-content">
    <h1>✅ 登录成功！</h1>
    <p>您可访问以下数据报表：</p>

    <div class="data-actions">
        <h2>📊 可用数据报表</h2>
        <ul class="data-list">
            <%= request.getAttribute("dataListHtml") != null ? request.getAttribute("dataListHtml") : "<li>暂无数据</li>" %>
        </ul>
    </div>

    <div class="footer-note">
        数据平台 · 安全 · 实时 · 可靠
    </div>
</div>
</body>
</html>