import React, { useState, useEffect, useCallback } from "react";
import { useParams } from "react-router-dom";
import "./ScripDetails.css"; // ✅ Importing CSS

const ScripDetails = () => {
  const { symbol } = useParams();
  const [scripData, setScripData] = useState(null);
  const [expiryList, setExpiryList] = useState([]);
  const [selectedExpiry, setSelectedExpiry] = useState("");
  const [optionChain, setOptionChain] = useState(null);
  const [loading, setLoading] = useState(true);
  const [ws, setWs] = useState(null);
  const [isLive, setIsLive] = useState(true);

  // ✅ Fetch Scrip Details & Expiry List
  const fetchData = useCallback(async () => {
    try {
      console.log(`🔄 Fetching data for: ${symbol}`);

      // ✅ Step 1: Search API to Get Correct `security_id`
      const searchResponse = await fetch(`http://127.0.0.1:8000/api/search/?query=${symbol}`);
      const searchResults = await searchResponse.json();

      if (!searchResults.length) {
        console.error(`❌ No matching scrip found for: ${symbol}`);
        setLoading(false);
        return;
      }

      const scrip = searchResults.find(s => s.exchange === "NSE") || searchResults[0];
      console.log(`✅ Found Scrip: ${scrip.alias} (ID: ${scrip.security_id}, Exchange: ${scrip.exchange})`);

      setScripData(scrip);

      // ✅ Step 2: Fetch Expiry List
      const expiryResponse = await fetch(
        `http://127.0.0.1:8000/api/get_expiry_list/?security_id=${scrip.security_id}&exchange_segment=${scrip.attribute}`
      );
      const expiryData = await expiryResponse.json();

      if (!expiryData || expiryData.length === 0) {
        console.warn("⚠️ No expiry dates found.");
        setExpiryList([]);
        setLoading(false);
        return;
      }

      console.log(`✅ Expiry List for ${scrip.alias}: ${expiryData}`);
      setExpiryList(expiryData);
      setSelectedExpiry(expiryData[0]);

      // ✅ Step 3: Fetch Option Chain for the Nearest Expiry
      await fetchOptionChain(scrip.security_id, scrip.attribute, expiryData[0]);

      // ✅ Step 4: Open WebSocket Connection
      connectWebSocket(scrip.security_id, scrip.attribute, expiryData[0]);

    } catch (error) {
      console.error("❌ Error fetching data:", error);
    } finally {
      setLoading(false);
    }
  }, [symbol]);

  // ✅ Fetch Option Chain Data
  const fetchOptionChain = async (security_id, exchange_segment, expiry) => {
    try {
      const optionChainResponse = await fetch(
        `http://127.0.0.1:8000/api/get_option_chain/?security_id=${security_id}&exchange_segment=${exchange_segment}`
      );
      const optionChainData = await optionChainResponse.json();

      if (optionChainData.option_chain) {
        console.log(`✅ Option Chain Data Received for Expiry ${expiry}:`, optionChainData);
        setOptionChain(optionChainData.option_chain);
        setIsLive(true);
      } else {
        console.warn("⚠️ No live data available. Showing last saved data.");
        setIsLive(false);
      }
    } catch (error) {
      console.error("❌ Error fetching option chain:", error);
    }
  };

  // ✅ WebSocket Connection Function
  const connectWebSocket = (security_id, exchange_segment, expiry) => {
    if (!security_id || !exchange_segment || !expiry) {
      console.error("❌ Missing WebSocket parameters!");
      return;
    }

    console.log(`🔗 Connecting WebSocket for ${security_id} - ${exchange_segment} - ${expiry}`);

    const wsUrl = `ws://127.0.0.1:8765/ws/option_chain/${security_id}/${exchange_segment}`;
    const websocket = new WebSocket(wsUrl);

    websocket.onopen = () => {
      console.log("✅ WebSocket Connected!");
    };

    websocket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      console.log("📩 WebSocket Data Received:", data);
      setOptionChain(data);
      setIsLive(true);
    };

    websocket.onclose = () => {
      console.warn("⚠️ WebSocket Disconnected!");
      setIsLive(false);
    };

    setWs(websocket);
  };

  // ✅ Handle Expiry Selection Change
  const handleExpiryChange = async (event) => {
    const selectedExpiry = event.target.value;
    setSelectedExpiry(selectedExpiry);

    console.log(`🔄 Fetching Option Chain for Expiry: ${selectedExpiry}`);
    await fetchOptionChain(scripData.security_id, scripData.attribute, selectedExpiry);
  };

  // ✅ Ensure useEffect runs correctly
  useEffect(() => {
    fetchData();
    return () => {
      if (ws) ws.close();
    };
  }, [fetchData]);

  // ✅ Handle Loading State
  if (loading) return <p className="loading">Loading...</p>;

  return (
    <div className="scrip-details">
      <h2>{scripData?.alias} - Option Chain</h2>

      {/* 🔹 Expiry Selection Dropdown */}
      {expiryList.length > 0 ? (
        <div className="expiry-container">
          <label>Select Expiry: </label>
          <select value={selectedExpiry} onChange={handleExpiryChange}>
            {expiryList.map((expiry) => (
              <option key={expiry} value={expiry}>{expiry}</option>
            ))}
          </select>
        </div>
      ) : (
        <p>No Expiry Data Available</p>
      )}

      {/* 🔹 Live Status */}
      <p className={`status ${isLive ? "live" : "offline"}`}>
        {isLive ? "Live Data Streaming" : "Showing Last Available Data"}
      </p>

      {/* 🔹 Option Chain Table */}
      {optionChain ? (
        <div className="option-chain-container">
          <table>
            <thead>
              <tr>
                <th>Strike</th>
                <th>Call OI</th>
                <th>Call Price</th>
                <th>Put OI</th>
                <th>Put Price</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(optionChain).map(([strike, data]) => (
                <tr key={strike}>
                  <td>{strike}</td>
                  <td>{data.ce?.oi ?? "N/A"}</td> {/* ✅ Handle Undefined */}
                  <td>{data.ce?.last_price ?? "N/A"}</td>
                  <td>{data.pe?.oi ?? "N/A"}</td> {/* ✅ Handle Undefined */}
                  <td>{data.pe?.last_price ?? "N/A"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p>No Option Chain Available</p>
      )}
    </div>
  );
};

export default ScripDetails;
