(function () {
  if (window.__sessionGuardActive) {
    return;
  }
  window.__sessionGuardActive = true;

  var scriptTag = document.currentScript;
  if (!scriptTag) {
    var candidates = document.querySelectorAll('script[src*="session_guard.js"]');
    scriptTag = candidates.length ? candidates[candidates.length - 1] : null;
  }

  var idleTimeoutSeconds = Number((scriptTag && scriptTag.dataset.idleTimeoutSeconds) || 1800);
  var warningSeconds = Number((scriptTag && scriptTag.dataset.warningSeconds) || 120);

  var idleTimeoutMs = Math.max(60 * 1000, idleTimeoutSeconds * 1000);
  var warningWindowMs = Math.max(10 * 1000, warningSeconds * 1000);
  var warningAfterMs = Math.max(0, idleTimeoutMs - warningWindowMs);

  var heartbeatUrl = "/session/heartbeat";
  var logoutUrl = "/logout";

  var heartbeatPollMs = 15000;
  var heartbeatMinGapMs = 60000;

  var lastActivityAt = Date.now();
  var lastHeartbeatAt = 0;
  var activitySinceHeartbeat = false;
  var warningVisible = false;
  var heartbeatInFlight = false;
  var warningTimerId = null;
  var logoutTimerId = null;
  var heartbeatTimerId = null;
  var lastNoisyEventAt = 0;

  var overlayEl = null;

  function ensureWarningUi() {
    if (overlayEl) {
      return overlayEl;
    }

    overlayEl = document.createElement("div");
    overlayEl.id = "session-warning-overlay";
    overlayEl.setAttribute("aria-hidden", "true");
    overlayEl.innerHTML =
      '<div class="session-warning-modal" role="dialog" aria-live="polite" aria-modal="true" aria-label="Session timeout warning">' +
      '<h3>Your session is about to expire</h3>' +
      "<p>Click this popup or anywhere on the page to stay signed in.</p>" +
      '<button type="button" id="session-warning-keep-open">Keep Session Open</button>' +
      "</div>";

    var styleEl = document.createElement("style");
    styleEl.textContent =
      "#session-warning-overlay{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(7,18,36,.46);z-index:9999;padding:16px;}" +
      "#session-warning-overlay.show{display:flex;}" +
      "#session-warning-overlay .session-warning-modal{max-width:480px;width:100%;background:#fff;border-radius:14px;padding:18px 20px;box-shadow:0 20px 40px rgba(0,0,0,.25);font-family:Segoe UI,Tahoma,sans-serif;}" +
      "#session-warning-overlay h3{margin:0 0 8px;font-size:20px;color:#0f172a;}" +
      "#session-warning-overlay p{margin:0 0 14px;color:#334155;font-size:14px;line-height:1.45;}" +
      "#session-warning-overlay button{background:#0f62fe;color:#fff;border:none;border-radius:10px;padding:10px 14px;font-weight:600;cursor:pointer;}" +
      "#session-warning-overlay button:hover{filter:brightness(0.95);}";

    document.head.appendChild(styleEl);
    document.body.appendChild(overlayEl);

    var keepOpenBtn = document.getElementById("session-warning-keep-open");
    if (keepOpenBtn) {
      keepOpenBtn.addEventListener("click", function (event) {
        event.preventDefault();
        event.stopPropagation();
        acknowledgeWarning();
      });
    }

    return overlayEl;
  }

  function showWarning() {
    ensureWarningUi();
    if (!overlayEl) {
      return;
    }
    warningVisible = true;
    overlayEl.classList.add("show");
    overlayEl.setAttribute("aria-hidden", "false");
  }

  function hideWarning() {
    if (!overlayEl) {
      warningVisible = false;
      return;
    }
    warningVisible = false;
    overlayEl.classList.remove("show");
    overlayEl.setAttribute("aria-hidden", "true");
  }

  function goToLogin() {
    window.location.assign(logoutUrl);
  }

  function resetIdleTimers() {
    if (warningTimerId) {
      window.clearTimeout(warningTimerId);
    }
    if (logoutTimerId) {
      window.clearTimeout(logoutTimerId);
    }

    warningTimerId = window.setTimeout(function () {
      showWarning();
    }, warningAfterMs);

    logoutTimerId = window.setTimeout(function () {
      goToLogin();
    }, idleTimeoutMs);
  }

  function sendHeartbeat(force) {
    var now = Date.now();
    if (heartbeatInFlight) {
      return;
    }
    if (!force) {
      if (!activitySinceHeartbeat) {
        return;
      }
      if (now - lastHeartbeatAt < heartbeatMinGapMs) {
        return;
      }
    }

    heartbeatInFlight = true;
    fetch(heartbeatUrl, {
      method: "POST",
      credentials: "same-origin",
      cache: "no-store",
      keepalive: true,
      headers: { "Content-Type": "application/json" },
      body: "{}"
    })
      .then(function (resp) {
        if (resp.status === 401) {
          goToLogin();
          return null;
        }
        if (!resp.ok) {
          return null;
        }
        return resp.json();
      })
      .then(function (payload) {
        if (!payload || !payload.ok) {
          return;
        }
        lastHeartbeatAt = Date.now();
        lastActivityAt = lastHeartbeatAt;
        activitySinceHeartbeat = false;
        hideWarning();
        resetIdleTimers();
      })
      .catch(function () {
        // Ignore network glitches; timeout flow still enforces logout.
      })
      .finally(function () {
        heartbeatInFlight = false;
      });
  }

  function acknowledgeWarning() {
    activitySinceHeartbeat = true;
    sendHeartbeat(true);
    hideWarning();
    lastActivityAt = Date.now();
    resetIdleTimers();
  }

  function onActivity(event) {
    if (!event) {
      return;
    }

    var eventType = event.type || "";
    if ((eventType === "mousemove" || eventType === "scroll") && !warningVisible) {
      var nowNoisy = Date.now();
      if (nowNoisy - lastNoisyEventAt < 1000) {
        return;
      }
      lastNoisyEventAt = nowNoisy;
    }

    activitySinceHeartbeat = true;
    lastActivityAt = Date.now();

    if (warningVisible) {
      acknowledgeWarning();
      return;
    }

    resetIdleTimers();
  }

  function startHeartbeatLoop() {
    heartbeatTimerId = window.setInterval(function () {
      if (document.hidden) {
        return;
      }
      var inactiveMs = Date.now() - lastActivityAt;
      if (inactiveMs >= warningAfterMs) {
        return;
      }
      sendHeartbeat(false);
    }, heartbeatPollMs);
  }

  [
    "click",
    "keydown",
    "mousedown",
    "mousemove",
    "pointerdown",
    "scroll",
    "touchstart"
  ].forEach(function (eventName) {
    document.addEventListener(eventName, onActivity, { passive: true, capture: true });
  });

  document.addEventListener("visibilitychange", function () {
    if (!document.hidden) {
      activitySinceHeartbeat = true;
      lastActivityAt = Date.now();
      resetIdleTimers();
    }
  });

  resetIdleTimers();
  startHeartbeatLoop();
})();
