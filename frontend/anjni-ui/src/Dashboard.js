import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom"; // ✅ Ensure correct navigation handling
import "./Dashboard.css";

const Dashboard = () => {
  const navigate = useNavigate();
  const [isLoggedIn, setIsLoggedIn] = useState(true);
  const [data, setData] = useState([]); // Full data
  const [filteredData, setFilteredData] = useState([]); // Search results
  const [watchlist, setWatchlist] = useState(new Set(JSON.parse(localStorage.getItem("watchlist")) || [])); // Persisted watchlist
  const [searchTerm, setSearchTerm] = useState("");
  const [activeTab, setActiveTab] = useState("All"); // "All" | "Watchlist"
  const [page, setPage] = useState(1); // Infinite Scroll Pagination

  // ✅ Ensure user stays logged in
  useEffect(() => {
    const loggedIn = localStorage.getItem("isLoggedIn");
    if (!loggedIn || loggedIn !== "true") {
      navigate("/");
    } else {
      setIsLoggedIn(true);
    }
  }, [navigate]);

  // ✅ Fetch Scrip Data from API (Paginated)
  useEffect(() => {
    fetchData();
  }, [page]); // ✅ Fetch on page scroll

  const fetchData = async () => {
    try {
      const response = await fetch(`http://127.0.0.1:8000/api/get-data/?page=${page}`);
      const result = await response.json();
      
      if (Array.isArray(result.data)) {  // ✅ Ensure response is an array
        setData((prev) => [...new Set([...prev, ...result.data])]);
        setFilteredData((prev) => [...new Set([...prev, ...result.data])]);
      } else {
        console.error("Invalid API response:", result);
      }
    } catch (error) {
      console.error("❌ Error fetching data:", error);
    }
  };

  // ✅ Search API Integration (With Debounce for Optimization)
  useEffect(() => {
    if (!searchTerm) {
      setFilteredData(data);
      return;
    }
    const timeout = setTimeout(() => handleSearch(searchTerm), 300);
    return () => clearTimeout(timeout);
  }, [searchTerm, data]);

  const handleSearch = async (query) => {
    try {
      const response = await fetch(`http://127.0.0.1:8000/api/search/?query=${query}`);
      const result = await response.json();

      if (Array.isArray(result)) {  // ✅ Ensure result is an array
        setFilteredData(result);
      } else {
        console.error("Invalid search response:", result);
        setFilteredData([]);
      }
    } catch (error) {
      console.error("❌ Error in search:", error);
    }
  };

  // ✅ FIXED: Navigate to ScripDetails instead of opening a new tab
  const handleScripClick = (scrip) => {
    console.log(`Navigating to: /scrip/${scrip}`);
    navigate(`/scrip/${scrip}`);
  };

  return (
    isLoggedIn && (
      <div className="dashboard-container">
        <nav className="navbar">
          <h1 className="anjni-logo">ANJNI</h1>
          <button className="logout-button" onClick={() => navigate("/")}>Logout</button>
        </nav>

        <div className="top-controls">
          <input
            type="text"
            placeholder="Search Scrip..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="search-bar"
          />
        </div>

        {/* ✅ Table Section */}
        <div className="table-container">
          <table className="dashboard-table">
            <thead>
              <tr>
                <th>Scrip Name</th>
                <th>Exch</th>
                <th>Analysis</th>
              </tr>
            </thead>
            <tbody>
              {(filteredData || []).map((row, index) => (
                <tr key={index}>
                  <td 
                    onClick={() => handleScripClick(row.trading_symbol)} 
                    className="clickable"
                  >
                    {row.trading_symbol}
                  </td>
                  <td>{row.exchange}</td>
                  <td>📈</td> {/* Placeholder for Analysis */}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )
  );
};

export default Dashboard;
