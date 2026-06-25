/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/* Adds a button that hides the right-hand "On this page" sidebar so the
 * article can use the full page width. The choice is remembered across
 * pages via localStorage. */
(function () {
  "use strict";
  var KEY = "pst-secondary-hidden";

  function apply(hidden, btn) {
    document.body.classList.toggle("pst-secondary-hidden", hidden);
    if (btn) {
      btn.setAttribute("aria-pressed", String(hidden));
      btn.title = hidden ? "Show page contents" : "Hide page contents";
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    // Only offer the toggle on pages that actually have the sidebar.
    if (!document.querySelector(".bd-sidebar-secondary")) {
      return;
    }

    var btn = document.createElement("button");
    btn.id = "pst-secondary-toggle";
    btn.type = "button";
    btn.setAttribute("aria-label", "Toggle page contents sidebar");
    btn.innerHTML = '<i class="fa-solid fa-list" aria-hidden="true"></i>';
    document.body.appendChild(btn);

    btn.addEventListener("click", function () {
      var hidden = !document.body.classList.contains("pst-secondary-hidden");
      try {
        localStorage.setItem(KEY, hidden ? "1" : "0");
      } catch (e) {
        /* localStorage may be unavailable; toggle still works for this page. */
      }
      apply(hidden, btn);
    });

    var stored = null;
    try {
      stored = localStorage.getItem(KEY);
    } catch (e) {
      /* ignore */
    }
    apply(stored === "1", btn);
  });
})();
