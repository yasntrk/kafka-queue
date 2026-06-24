/* =====================================================================
 * Fish Auction — shared cross-service UI glue (flow nav + notifications)
 * One self-contained file, dropped into each service's static dir and
 * loaded with: <script>window.FLOW_STEP = N</script><script src="/flow-widget.js"></script>
 * It does NOT touch any existing markup/styles — everything is fixed-position
 * overlay with the fa-flow- prefix.
 * ===================================================================== */
(function () {
  "use strict";

  // --- Where each service lives ---
  // Host is DERIVED from the page's own address, so the same file works on
  // localhost, an EC2 public IP, or a domain with zero edits. Only the ports
  // are fixed. (Override by setting window.FLOW_HOSTS before this script.)
  var proto = window.location.protocol === "https:" ? "https:" : "http:";
  var host = window.location.hostname || "localhost";
  function svc(port, path) { return proto + "//" + host + ":" + port + (path || ""); }
  var FLOW = (window.FLOW = window.FLOW_HOSTS || {
    steps: [
      { n: 1, label: "Üyelik",        url: svc(3001) },
      { n: 2, label: "Katalog",       url: svc(3002) },
      { n: 3, label: "Açık Artırma",  url: svc(3003) },
      { n: 4, label: "Satış Sonrası", url: svc(3004, "/fulfillment") }
    ],
    hub: svc(25067, "/notificationHub")
  });

  var current = Number(window.FLOW_STEP || 0); // page declares its own step

  /* ---------- current user role (for role-based notification filtering) ----------
   * Carried across services only as ?fishrole=<role> (set by the login redirect and
   * by the nav links below). No token / no buyerId is propagated. Stored per-origin. */
  var ROLE = (function () {
    try {
      var qs = new URLSearchParams(window.location.search);
      var r = qs.get("fishrole");
      if (r) {
        localStorage.setItem("fishrole", r);
        qs.delete("fishrole");
        var clean = window.location.pathname + (qs.toString() ? "?" + qs.toString() : "") + window.location.hash;
        window.history.replaceState(null, "", clean);
        return r;
      }
      return localStorage.getItem("fishrole") || "";
    } catch (e) { return ""; }
  })();
  // Append the role to a service URL so it survives navigation.
  function withRole(url) {
    if (!ROLE) return url;
    return url + (url.indexOf("?") !== -1 ? "&" : "?") + "fishrole=" + encodeURIComponent(ROLE);
  }
  // Notifications a BUYER is allowed to see: the bidding lifecycle (bid başladı/bitti).
  // Members (and unknown/anonymous) see everything.
  var BUYER_NOTIF = ["auction session started", "basket opened", "basket sold", "re-bid round opened", "auction closed"];
  function notifAllowed(category) {
    if (ROLE !== "buyer") return true; // member / unknown → all
    var c = String(category || "").toLowerCase();
    return BUYER_NOTIF.some(function (k) { return c.indexOf(k) !== -1; });
  }

  /* ---------- styles ---------- */
  var css = [
    ".fa-flow-bar{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);z-index:2147483000;",
    "display:flex;align-items:center;gap:6px;padding:8px 10px;border-radius:999px;",
    "background:rgba(15,28,32,.92);box-shadow:0 8px 30px rgba(0,0,0,.35);backdrop-filter:blur(6px);",
    "font:600 13px/1 Inter,system-ui,sans-serif;color:#cfe;}",
    ".fa-flow-step{display:flex;align-items:center;gap:6px;padding:7px 12px;border-radius:999px;",
    "color:#9fc;text-decoration:none;white-space:nowrap;transition:background .15s,color .15s;}",
    ".fa-flow-step:hover{background:rgba(255,255,255,.08);color:#fff;}",
    ".fa-flow-step .fa-flow-n{display:inline-flex;width:18px;height:18px;border-radius:50%;",
    "align-items:center;justify-content:center;font-size:11px;background:rgba(255,255,255,.12);}",
    ".fa-flow-step.is-current{background:#16a34a;color:#fff;}",
    ".fa-flow-step.is-current .fa-flow-n{background:rgba(255,255,255,.25);}",
    ".fa-flow-sep{opacity:.35;}",
    "@media(max-width:640px){.fa-flow-step .fa-flow-lbl{display:none;}}",
    /* toasts */
    ".fa-toast-wrap{position:fixed;top:16px;right:16px;z-index:2147483600;display:flex;",
    "flex-direction:column;gap:10px;max-width:360px;}",
    ".fa-toast{background:#0f1c20;color:#eafff4;border-left:4px solid #16a34a;border-radius:10px;",
    "padding:12px 14px;box-shadow:0 10px 30px rgba(0,0,0,.4);font:400 13px/1.45 Inter,system-ui,sans-serif;",
    "opacity:0;transform:translateX(20px);transition:opacity .25s,transform .25s;}",
    ".fa-toast.show{opacity:1;transform:none;}",
    ".fa-toast .fa-toast-top{font-weight:700;font-size:11px;letter-spacing:.04em;text-transform:uppercase;",
    "color:#7ee0b0;margin-bottom:4px;display:flex;justify-content:space-between;gap:10px;}",
    ".fa-toast .fa-dot{width:7px;height:7px;border-radius:50%;background:#16a34a;display:inline-block;margin-right:6px;}"
  ].join("");
  var style = document.createElement("style");
  style.textContent = css;
  document.head.appendChild(style);

  /* ---------- flow nav bar ---------- */
  function buildBar() {
    var bar = document.createElement("div");
    bar.className = "fa-flow-bar";
    FLOW.steps.forEach(function (s, i) {
      var a = document.createElement("a");
      a.className = "fa-flow-step" + (s.n === current ? " is-current" : "");
      a.href = withRole(s.url);
      a.innerHTML =
        '<span class="fa-flow-n">' + s.n + "</span>" +
        '<span class="fa-flow-lbl">' + s.label + "</span>";
      bar.appendChild(a);
      if (i < FLOW.steps.length - 1) {
        var sep = document.createElement("span");
        sep.className = "fa-flow-sep";
        sep.textContent = "→";
        bar.appendChild(sep);
      }
    });
    document.body.appendChild(bar);
  }

  /* ---------- toasts ---------- */
  var wrap;
  function toast(topic, message) {
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.className = "fa-toast-wrap";
      document.body.appendChild(wrap);
    }
    var el = document.createElement("div");
    el.className = "fa-toast";
    el.innerHTML =
      '<div class="fa-toast-top"><span><span class="fa-dot"></span>Bildirim</span><span>' +
      escapeHtml(topic || "") + "</span></div><div>" + escapeHtml(message || "") + "</div>";
    wrap.appendChild(el);
    requestAnimationFrame(function () { el.classList.add("show"); });
    setTimeout(function () {
      el.classList.remove("show");
      setTimeout(function () { el.remove(); }, 300);
    }, 15000); // toast on-screen duration (ms)
    // keep at most 5 visible
    while (wrap.children.length > 5) wrap.removeChild(wrap.firstChild);
  }
  function escapeHtml(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  /* ---------- SignalR (loaded from CDN, falls back silently) ---------- */
  function connectSignalR() {
    if (!window.signalR) return;
    var conn = new window.signalR.HubConnectionBuilder()
      .withUrl(FLOW.hub)
      .withAutomaticReconnect()
      .build();
    // The notification service broadcasts every event on TWO channels to Clients.All:
    //   ReceiveNotification     -> general board (what we show as a popup)
    //   ReceiveUserNotification -> personal board (shown only on the Notification UI)
    // We subscribe to the general channel only, so each event pops exactly once.
    conn.on("ReceiveNotification", function (topic, message) {
      if (!notifAllowed(topic)) return; // buyer sees only bid started/ended
      toast(topic, message);
    });
    conn.start().catch(function () { setTimeout(connectSignalR, 5000); });
  }
  function loadSignalR() {
    if (window.signalR) return connectSignalR();
    var s = document.createElement("script");
    s.src = "https://cdnjs.cloudflare.com/ajax/libs/microsoft-signalr/8.0.7/signalr.min.js";
    s.onload = connectSignalR;
    s.onerror = function () { /* notifications unavailable, nav bar still works */ };
    document.head.appendChild(s);
  }

  function init() { buildBar(); loadSignalR(); }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
