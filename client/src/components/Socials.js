import React from "react";
import CHNsocial from "../assets/CHNsocial.png";

export default function Socials() {
  return (
    <div className="social-sidebar">
      <a href="https://twitter.com/chihacknight">
        <div className="social-icon twitter">
          <i class="fa-brands fa-twitter"></i>
        </div>
      </a>
      <a  href="https://github.com/lauriemerrell/chn-ghost-buses">
        <div className="social-icon github">
          <i class="fa-brands fa-github"></i>
        </div>
      </a>
      <a href="https://chihacknight.org/">
        <div className="social-icon chn">
          <img src={CHNsocial} alt="" />
        </div>
      </a>
    </div>
  );
}
