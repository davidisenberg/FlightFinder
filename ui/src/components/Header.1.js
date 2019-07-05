import React, { Component } from "react";
import "../css/animate.css";
import "../css/bootstrap.css";
import "../css/cs-select.css";
import "../css/cs-skin-border.css";
import "../css/icomoon.css";
import "../css/style.css";
import "../css/superfish.css";
import "../css/magnific-popup.css";
import "../fonts/icomoon/icomoon.eot";
import "../fonts/icomoon/icomoon.svg";
import "../fonts/icomoon/icomoon.ttf";
import "../fonts/icomoon/icomoon.woff";
import "../fonts/bootstrap/glyphicons-halflings-regular.eot";
import "../fonts/bootstrap/glyphicons-halflings-regular.svg";
import "../fonts/bootstrap/glyphicons-halflings-regular.ttf";
import "../fonts/bootstrap/glyphicons-halflings-regular.woff";
import "../fonts/bootstrap/glyphicons-halflings-regular.woff2";

class Header extends React.Component {
  render() {
    return (
      <header id="fh5co-header-section" class="sticky-banner">
        <div class="container">
          <div class="nav-header">
            <a href="#" class="js-fh5co-nav-toggle fh5co-nav-toggle dark">
              <i />
            </a>
            <h1 id="fh5co-logo">
              <a href="index.html">
                <i class="icon-airplane" />
                Trip Builder
              </a>
            </h1>
          </div>
        </div>
      </header>
    );
  }
}

export default Header;
