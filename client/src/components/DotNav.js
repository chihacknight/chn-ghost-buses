import React from "react";
import busNavIcon from '../assets/bus-nav-icon.png'

export default function DotNav({setDetailNavSelect, detailNavSelect}) {
  return (
    <div className="dot-nav">
      <img className={`dot-nav-indicator ${detailNavSelect}`} src={busNavIcon} alt=''/>
      <div className="dot-label" onClick={() => setDetailNavSelect('matters')}>
        <div className="dot"></div>
        <p>Why It Matters</p>
      </div>
      <div className="dot-line"></div>
      <div className="dot-label" onClick={() => setDetailNavSelect('methods')}>
        <div className="dot"></div>
        <p>Our Methods</p>
      </div>
      <div className="dot-line"></div>
      <div className="dot-label" onClick={() => setDetailNavSelect('findings')}>
        <div className="dot"></div>
        <p>Our Findings</p>
      </div>
      <div className="dot-line"></div>
      <div className="dot-label" onClick={() => setDetailNavSelect('cta')}>
        <div className="dot"></div>
        <p>Call to Action</p>
      </div>
    </div>
  );
}
