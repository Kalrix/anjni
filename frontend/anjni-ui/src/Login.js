import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import "./Login.css";

const Login = ({ setIsLoggedIn }) => {
  const [anjniId, setAnjniId] = useState("");
  const [password, setPassword] = useState("");
  const [activationCode, setActivationCode] = useState("");
  const [message, setMessage] = useState("");
  const [isFirstTimeUser, setIsFirstTimeUser] = useState(false);
  const navigate = useNavigate();

  const handleLogin = async () => {
    const requestBody = { anjni_id: anjniId, password: password };
    if (isFirstTimeUser) requestBody.activation_code = activationCode;

    try {
      const response = await fetch(`${process.env.REACT_APP_API_URL}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody),
      });

      const data = await response.json();
      if (response.ok) {
        localStorage.setItem("isLoggedIn", "true");
        window.location.href = "/dashboard";
      } else {
        setMessage(data.detail);
      }
    } catch (error) {
      setMessage("Error: Could not connect to backend");
      console.error("Login API error:", error);
    }
  };

  return (
    <div className="login-wrapper">
      <div className="animated-bg"></div>

      {/* ANJNI Logo & Tagline */}
      <div className="logo-container">
        <h1 className="anjni-logo-home">A.N.J.N.I</h1>
        <p className="anjni-tagline">AI-Driven Neural JavaScript Node Interface</p>
      </div>

      {/* 🚀 Login Box */}
      <div className="login-container">
        {/* Form */}
        <div className="form-container">
          <input
            type="text"
            className="futuristic-input"
            placeholder="ANJNI ID"
            value={anjniId}
            onChange={(e) => setAnjniId(e.target.value)}
          />

          <input
            type="password"
            className="futuristic-input"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />

          {isFirstTimeUser && (
            <input
              type="text"
              className="futuristic-input"
              placeholder="Activation Code"
              value={activationCode}
              onChange={(e) => setActivationCode(e.target.value)}
            />
          )}

          <button className="futuristic-button" onClick={handleLogin}>Login</button>
        </div>

        {/* First-time user toggle */}
        {!isFirstTimeUser ? (
          <p className="toggle-text">
            <a href="#" onClick={() => setIsFirstTimeUser(true)}>First-time user?</a>
          </p>
        ) : (
          <p className="toggle-text">
            <a href="#" onClick={() => setIsFirstTimeUser(false)}>Already Registered? Go to Login</a>
          </p>
        )}

        {/* Error message */}
        {message && <p className="error-message">{message}</p>}
      </div>

      {/* Footer */}
      <footer className="footer">
        © 2025 ANJNI. All Rights Reserved. | Version 1.0.0
      </footer>
    </div>
  );
};

export default Login;
