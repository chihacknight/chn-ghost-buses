import React from "react";
import bus from "../assets/path6.png";
import busStop from "../assets/busStop.png";
import ghostBusLogo from "../assets/ghostBusLogo.png";
import chiHackLogo from "../assets/chiHackLogo.png";

export default function Header() {
  return (
    <header>
      <div className="h1-container">
        <h1>Ghost Bus</h1>
        <img src={ghostBusLogo} alt="ghost bus logo" />
        <div className="subtitle-container">
          <p className="subtitle">powered by</p>
          <img src={chiHackLogo} alt="ChiHackNight logo" />
        </div>
      </div>

      <div className="svg-container"></div>
      <div className="bus-stop-container">
        <p>?</p>
        <img src={busStop} alt="" />
      </div>
      <div className="bus-container">
        <img src={bus} alt="" />
      </div>
    </header>
  );
}
