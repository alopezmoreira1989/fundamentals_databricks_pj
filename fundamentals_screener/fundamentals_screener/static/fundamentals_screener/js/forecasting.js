// Forecasting tab — 10-year history / 10-year explicit ML forecast / 10-year front-loaded
// terminal-convergence fan chart, driven by the `chart_data` JSON embedded via
// {{ chart_data|json_script:"fc-data" }} in forecasting.html (no extra request;
// forecasting_data() is a separate JSON sibling for API consumers only).
//
// Chart geometry ports docs/mockups/forecasting_tab.html's v2 layout: uniform px-per-year
// across the whole 30-year span (10 hist + 10 explicit + 10 terminal), real y-axis gridlines,
// x-axis year ticks, and a separate <path> per scenario per segment (explicit/terminal) so the
// terminal segment can carry its own lighter/thinner .fc-line--terminal treatment. A single
// shared min/max scale is still computed across the historical line AND every scenario line
// for a metric, so every line originates from the same FY0 point (issue #336's original
// required test, unchanged by this milestone).
(function () {
  "use strict";

  var dataEl = document.getElementById("fc-data");
  if (!dataEl) return;
  var DATA = JSON.parse(dataEl.textContent);

  var METRICS = {};
  DATA.metrics.forEach(function (m) { METRICS[m.metric] = m; });

  var SCENARIO_KEYS = { "0.1": "bear", "0.25": "lowbear", "0.5": "crab", "0.75": "lowbull", "0.9": "bull" };

  var svg = document.getElementById("fc-svg");
  var titleEl = document.getElementById("fc-chart-title");
  var SVG_NS = "http://www.w3.org/2000/svg";

  // Layout constants — uniform px-per-year across the full 30-year span. X0 marks the leftmost
  // possible historical point (FY-10); a ticker with less than 10 years of history simply
  // starts its historical line further right (see histX below) rather than the axis itself
  // changing shape — the axis frame is always the full nominal FY-10..FY+20 range.
  var YEARS_HIST = 10, YEARS_EXPLICIT = 10, YEARS_TERMINAL = 10;
  var PX_PER_YEAR = 34;
  var X0 = 70;                                          // FY-10
  var X_FY0 = X0 + YEARS_HIST * PX_PER_YEAR;             // FY0
  var X_FY10 = X_FY0 + YEARS_EXPLICIT * PX_PER_YEAR;     // FY+10
  var X_FY20 = X_FY10 + YEARS_TERMINAL * PX_PER_YEAR;    // FY+20
  var Y_TOP = 44, Y_BASE = 320, PLOT_H = Y_BASE - Y_TOP;

  // Historical years step back from FY0 by "years ago" (not by array index against a fixed
  // 10-point assumption), so a ticker with fewer than 10 years of history renders a shorter
  // line starting later, never overflowing past X0.
  function histX(index, len) {
    return X_FY0 - (len - 1 - index) * PX_PER_YEAR;
  }
  function forecastX(horizon) {
    return X_FY0 + horizon * PX_PER_YEAR;
  }
  function toY(value, scale) {
    return Y_BASE - ((value - scale.lo) / (scale.hi - scale.lo)) * PLOT_H;
  }

  function sharedScale(m) {
    var all = m.historical.map(function (h) { return h.value; });
    m.scenarios.forEach(function (s) { all = all.concat(s.values); });
    all = all.filter(function (v) { return v !== null && v !== undefined; });
    if (!all.length) return { lo: 0, hi: 1 };
    var min = Math.min.apply(null, all);
    var max = Math.max.apply(null, all);
    var pad = (max - min) * 0.06 || 1;
    return { lo: min - pad, hi: max + pad };
  }

  function histPath(m, scale) {
    var d = "";
    m.historical.forEach(function (h, idx) {
      if (h.value === null || h.value === undefined) return;
      var x = histX(idx, m.historical.length);
      d += (d === "" ? "M" : " L") + x.toFixed(1) + "," + toY(h.value, scale).toFixed(1);
    });
    return d;
  }

  // Explicit segment: horizons 0 (the FY0 anchor, if a historical value exists to anchor from)
  // through 10. Terminal segment: horizons 10 through 20. Both include horizon 10 so the two
  // <path> elements visually connect with no gap.
  function explicitPath(series, scale) {
    var d = "";
    series.horizons.forEach(function (h, i) {
      if (h > YEARS_EXPLICIT) return;
      var v = series.values[i];
      if (v === null || v === undefined) return;
      var x = forecastX(h);
      d += (d === "" ? "M" : " L") + x.toFixed(1) + "," + toY(v, scale).toFixed(1);
    });
    return d;
  }
  function terminalPath(series, scale) {
    var d = "";
    series.horizons.forEach(function (h, i) {
      if (h < YEARS_EXPLICIT) return;
      var v = series.values[i];
      if (v === null || v === undefined) return;
      var x = forecastX(h);
      d += (d === "" ? "M" : " L") + x.toFixed(1) + "," + toY(v, scale).toFixed(1);
    });
    return d;
  }

  function el(tag, attrs, text) {
    var e = document.createElementNS(SVG_NS, tag);
    Object.keys(attrs || {}).forEach(function (k) { e.setAttribute(k, attrs[k]); });
    if (text !== undefined) e.textContent = text;
    return e;
  }

  // Auto-scaled $ formatter (B/M/K) -- real figures span from small-cap millions to AAPL-scale
  // hundreds of billions, unlike the mockup's fixed "$ billions" assumption.
  function fmtY(v) {
    var abs = Math.abs(v);
    if (abs >= 1e9) return "$" + (v / 1e9).toFixed(abs / 1e9 >= 100 ? 0 : 1) + "B";
    if (abs >= 1e6) return "$" + (v / 1e6).toFixed(abs / 1e6 >= 100 ? 0 : 1) + "M";
    if (abs >= 1e3) return "$" + (v / 1e3).toFixed(abs / 1e3 >= 100 ? 0 : 1) + "K";
    return "$" + v.toFixed(1);
  }

  function renderStaticAxes() {
    var zoneTerminal = document.getElementById("zone-terminal");
    zoneTerminal.setAttribute("x", X_FY10);
    zoneTerminal.setAttribute("y", Y_TOP - 14);
    zoneTerminal.setAttribute("width", X_FY20 - X_FY10);
    zoneTerminal.setAttribute("height", Y_BASE - Y_TOP + 14);

    document.getElementById("zone-label-hist").setAttribute("x", (X0 + X_FY0) / 2);
    document.getElementById("zone-label-explicit").setAttribute("x", (X_FY0 + X_FY10) / 2);
    document.getElementById("zone-label-terminal").setAttribute("x", (X_FY10 + X_FY20) / 2);

    var baseline = document.getElementById("fc-baseline");
    baseline.setAttribute("x1", X0); baseline.setAttribute("y1", Y_BASE);
    baseline.setAttribute("x2", X_FY20); baseline.setAttribute("y2", Y_BASE);

    var fy0 = document.getElementById("fc-line-fy0");
    fy0.setAttribute("x1", X_FY0); fy0.setAttribute("y1", Y_TOP - 14);
    fy0.setAttribute("x2", X_FY0); fy0.setAttribute("y2", Y_BASE);

    var fy10 = document.getElementById("fc-line-fy10");
    fy10.setAttribute("x1", X_FY10); fy10.setAttribute("y1", Y_TOP - 14);
    fy10.setAttribute("x2", X_FY10); fy10.setAttribute("y2", Y_BASE);

    // x-axis ticks: minor every year, major (labeled) every 5 years, spanning the full nominal
    // FY-10..FY+20 range regardless of how much real historical data any one ticker has.
    var ticksG = document.getElementById("fc-ticks-x");
    ticksG.innerHTML = "";
    for (var yr = -YEARS_HIST; yr <= YEARS_EXPLICIT + YEARS_TERMINAL; yr++) {
      var x = X_FY0 + yr * PX_PER_YEAR;
      var isMajor = yr % 5 === 0;
      ticksG.appendChild(el("line", {
        class: isMajor ? "fc-tick-major" : "fc-tick-minor",
        x1: x, y1: Y_BASE, x2: x, y2: Y_BASE + (isMajor ? 8 : 4),
      }));
      if (isMajor) {
        var label = yr === 0 ? "FY0" : (yr > 0 ? "FY+" + yr : "FY" + yr);
        ticksG.appendChild(el("text", { class: "fc-axis-label", x: x, y: Y_BASE + 22, "text-anchor": "middle" }, label));
      }
    }
  }

  function renderYGrid(scale) {
    var g = document.getElementById("fc-gridlines-y");
    g.innerHTML = "";
    var levels = 4; // quartile gridlines
    for (var i = 0; i <= levels; i++) {
      var v = scale.lo + (scale.hi - scale.lo) * (i / levels);
      var y = toY(v, scale);
      g.appendChild(el("line", { class: "fc-grid-y", x1: X0, y1: y.toFixed(1), x2: X_FY20, y2: y.toFixed(1) }));
      g.appendChild(el("text", { class: "fc-axis-label fc-axis-label--y", x: X0 - 10, y: (y + 3).toFixed(1) }, fmtY(v)));
    }
  }

  function renderChart(metricKey) {
    var m = METRICS[metricKey];
    if (!m) return;
    titleEl.textContent = m.label + (m.unit ? " · " + m.unit : "");
    var scale = sharedScale(m);
    renderYGrid(scale);
    document.getElementById("ln-hist").setAttribute("d", histPath(m, scale));
    m.scenarios.forEach(function (s) {
      var key = SCENARIO_KEYS[String(s.quantile_level)];
      if (!key) return;
      var explicitEl = document.getElementById("ln-" + key + "-explicit");
      var terminalEl = document.getElementById("ln-" + key + "-terminal");
      if (explicitEl) explicitEl.setAttribute("d", explicitPath(s, scale));
      if (terminalEl) terminalEl.setAttribute("d", terminalPath(s, scale));
    });
  }

  document.querySelectorAll("#fc-metric-tabs .nav-link").forEach(function (btn) {
    btn.addEventListener("click", function () {
      document.querySelectorAll("#fc-metric-tabs .nav-link").forEach(function (b) {
        b.classList.remove("active");
      });
      btn.classList.add("active");
      renderChart(btn.dataset.metric);
    });
  });

  document.querySelectorAll(".fc-chip").forEach(function (chip) {
    chip.addEventListener("click", function () {
      var active = chip.dataset.active === "true";
      chip.dataset.active = (!active).toString();
      ["explicit", "terminal"].forEach(function (seg) {
        var line = document.getElementById("ln-" + chip.dataset.scenario + "-" + seg);
        if (line) line.setAttribute("data-hidden", active.toString());
      });
    });
  });

  renderStaticAxes();
  var firstTab = document.querySelector("#fc-metric-tabs .nav-link");
  if (firstTab) renderChart(firstTab.dataset.metric);

  // Forward-multiples table — plain cell fill-in, no chart geometry involved.
  var byRow = {};
  (DATA.forward_multiples || []).forEach(function (row) {
    byRow[row.metric] = byRow[row.metric] || {};
    byRow[row.metric][row.horizon] = row.value;
  });
  document.querySelectorAll("[data-multiple-row]").forEach(function (tr) {
    var metric = tr.dataset.multipleRow;
    var values = byRow[metric] || {};
    tr.querySelectorAll("[data-horizon]").forEach(function (td) {
      var v = values[td.dataset.horizon];
      if (v === null || v === undefined) return;
      td.textContent = metric === "forward_pe" ? v.toFixed(1) + "×" : (v * 100).toFixed(1) + "%";
    });
  });
})();
