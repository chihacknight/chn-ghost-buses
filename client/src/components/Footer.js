import React from "react";

export default function Footer() {
  return (
    <footer>
      <div className="padding-container footer-container">
        <div className="footer-links">
          <h5> Links</h5>
          <ul>
            <li>
              <a href="https://chihacknight.org/">ChiHackNight</a>
            </li>
            <li>
              <a href="https://github.com/lauriemerrell/chn-ghost-buses">
                Github Repo
              </a>
            </li>
          </ul>
        </div>
        <div className="footer-attributions">
          <h5>Attributions</h5>
          <ul>
            <li>
              <a href="https://www.freepik.com/vectors/business-bag">
                Business bag vector created by pch.vector
              </a>
            </li>
            <li></li>
          </ul>
        </div>
      </div>
    </footer>
  );
}
