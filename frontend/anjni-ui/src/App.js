import React, { useState, useEffect } from "react";
import { BrowserRouter as Router, Route, Routes, Navigate } from "react-router-dom";
import Login from "./Login";
import Dashboard from "./Dashboard";
import ScripDetails from "./ScripDetails"; // ✅ Import ScripDetails component
import "./Login.css";

function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(false);

  // ✅ Ensure login state is synced with localStorage
  useEffect(() => {
    const loggedIn = localStorage.getItem("isLoggedIn") === "true";
    setIsLoggedIn(loggedIn);
  }, []);

  return (
    <Router>
      <Routes>
        {/* ✅ Default Route: Login Page */}
        <Route
          path="/"
          element={isLoggedIn ? <Navigate to="/dashboard" replace /> : <Login setIsLoggedIn={setIsLoggedIn} />}
        />

        {/* ✅ Dashboard Page (Protected) */}
        <Route
          path="/dashboard"
          element={isLoggedIn ? <Dashboard /> : <Navigate to="/" replace />}
        />

        {/* ✅ FIXED: Scrip Details Page (Dynamic Route for Any Scrip) */}
        <Route
          path="/scrip/:symbol"
          element={isLoggedIn ? <ScripDetails /> : <Navigate to="/" replace />}
        />

        {/* ✅ Catch-all for invalid routes (Redirects to Dashboard) */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Router>
  );
}

export default App;
