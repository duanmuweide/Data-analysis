<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
  <title>ID 登录</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      background-color: #f4f4f4;
      display: flex;
      justify-content: center;
      align-items: center;
      height: 100vh;
      margin: 0;
    }
    .login-box {
      background: white;
      padding: 20px 30px;
      border-radius: 8px;
      box-shadow: 0 0 10px rgba(0,0,0,0.1);
      width: 300px;
    }
    .login-box h2 {
      text-align: center;
      margin-bottom: 20px;
    }
    .login-box input[type="number"],
    .login-box input[type="password"] {
      width: 100%;
      padding: 10px;
      margin: 8px 0;
      border: 1px solid #ccc;
      border-radius: 4px;
      box-sizing: border-box;
    }
    .login-box input[type="submit"] {
      width: 100%;
      padding: 10px;
      background-color: #4CAF50;
      color: white;
      border: none;
      border-radius: 4px;
      cursor: pointer;
    }
    .login-box input[type="submit"]:hover {
      background-color: #45a049;
    }
    .error {
      color: red;
      text-align: center;
      margin-top: 10px;
    }
  </style>
</head>
<body>
<div class="login-box">
  <h2>ID 登录</h2>
  <form action="login" method="post">
    <input type="number" name="id" placeholder="用户ID" min="1" required>
    <input type="password" name="password" placeholder="密码" required>
    <input type="submit" value="登录">
  </form>
  <%
    String error = (String) request.getAttribute("error");
    if (error != null) {
  %>
  <div class="error"><%= error %></div>
  <% } %>
</div>
</body>
</html>s