// Forecasting tab — 10-year history / 10-year explicit ML forecast / 10-year front-loaded
// terminal-convergence fan chart, rendered via Chart.js (self-hosted,
// static/fundamentals_screener/js/vendor/chart.umd.js). Driven by the `forecast_chart_data`
// JSON embedded via {{ forecast_chart_data|json_script:"fc-data" }} in company_detail.html.
// Replaces the previous hand-rolled inline-<svg> fan chart with a real chart-library
// component — native hover tooltips, real axis ticks — same migration already done for the
// Price/Income Statement/Balance Sheet/Cash Flow/Quarterly tabs.
//
// x-axis is a linear NUMERIC scale (years relative to FY0: -historyLen..+20), not Chart.js
// category labels — a ticker with fewer than 10 years of history just has a shorter
// historical dataset (fewer points starting later), the same "shared px-per-year, variable
// history length" idea the old SVG version used, expressed as a numeric scale instead of
// hand-computed pixel positions. Every scenario's own values/horizons already carries a
// prepended FY0 anchor point (services.get_forecast_chart, server-side) so every line -
// historical + all 5 scenarios - shares one point of origin and one y-scale domain.
//
// Colors are read at runtime from app.css's :root custom properties via getComputedStyle() —
// never hardcoded here, so the chart always matches whatever the current theme tokens are.
(function () {
  "use strict";

  if (typeof Chart === "undefined") return;
  var dataEl = document.getElementById("fc-data");
  var canvas = document.getElementById("fc-canvas");
  if (!dataEl || !canvas) return;
  var DATA = JSON.parse(dataEl.textContent);
  if (!DATA.metrics || !DATA.metrics.length) return;

  var METRICS = {};
  DATA.metrics.forEach(function (m) { METRICS[m.metric] = m; });

  var root = getComputedStyle(document.documentElement);
  var tok = function (name) { return root.getPropertyValue(name).trim(); };
  var COLOR_INK = tok("--ink");
  var COLOR_NEGATIVE = tok("--negative");
  var COLOR_ACCENT = tok("--accent");
  var COLOR_POSITIVE = tok("--positive");
  var COLOR_INK3 = tok("--ink-3");
  var COLOR_RULE_SOFT = tok("--rule-soft");
  var COLOR_BG_SUBTLE = tok("--bg-subtle");
  var FONT_MONO = tok("--mono");

  // quantile_level -> display config, matching the legend chips already in the template
  // (data-scenario values / .fc-chip--* colors in app.css).
  var SCENARIOS = [
    { level: "0.1", key: "bear", label: "Bear", color: COLOR_NEGATIVE, faded: false },
    { level: "0.25", key: "lowbear", label: "Low Bear", color: COLOR_NEGATIVE, faded: true },
    { level: "0.5", key: "crab", label: "Crab", color: COLOR_ACCENT, faded: false },
    { level: "0.75", key: "lowbull", label: "Low Bull", color: COLOR_POSITIVE, faded: true },
    { level: "0.9", key: "bull", label: "Bull", color: COLOR_POSITIVE, faded: false },
  ];
  var TERMINAL_START = 10; // horizon where the terminal (dashed/lighter) segment begins
  var TERMINAL_END = 20;

  function withAlpha(hex, alpha) {
    if (hex.charAt(0) !== "#" || hex.length !== 7) return hex; // not a plain #rrggbb -- leave as-is
    return hex + Math.round(alpha * 255).toString(16).padStart(2, "0");
  }

  // Auto-scaled $ formatter (B/M/K) -- real figures span from small-cap millions to AAPL-scale
  // hundreds of billions.
  function fmtY(v) {
    var abs = Math.abs(v);
    if (abs >= 1e9) return "$" + (v / 1e9).toFixed(abs / 1e9 >= 100 ? 0 : 1) + "B";
    if (abs >= 1e6) return "$" + (v / 1e6).toFixed(abs / 1e6 >= 100 ? 0 : 1) + "M";
    if (abs >= 1e3) return "$" + (v / 1e3).toFixed(abs / 1e3 >= 100 ? 0 : 1) + "K";
    return "$" + v.toFixed(1);
  }
  function fmtX(v) {
    return v === 0 ? "FY0" : (v > 0 ? "FY+" + v : "FY" + v);
  }

  // Shaded terminal-zone band + FY0 "today" reference line -- Chart.js has no built-in region-
  // shading primitive, so this draws straight on the canvas via the chart's own x/y scale
  // pixel mapping, correct across resize.
  var zonePlugin = {
    id: "fcTerminalZone",
    beforeDatasetsDraw: function (chart) {
      var xScale = chart.scales.x, yScale = chart.scales.y;
      if (!xScale || !yScale) return;
      var ctx = chart.ctx;
      var xTerminal = xScale.getPixelForValue(TERMINAL_START);
      var xEnd = xScale.getPixelForValue(xScale.max);
      var xToday = xScale.getPixelForValue(0);
      ctx.save();
      ctx.fillStyle = COLOR_BG_SUBTLE || "rgba(0,0,0,.03)";
      ctx.fillRect(xTerminal, yScale.top, xEnd - xTerminal, yScale.bottom - yScale.top);
      ctx.strokeStyle = COLOR_INK3;
      ctx.setLineDash([2, 2]);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(xToday, yScale.top);
      ctx.lineTo(xToday, yScale.bottom);
      ctx.stroke();
      ctx.restore();
    },
  };

  var titleEl = document.getElementById("fc-chart-title");
  var chart = null;

  function buildDatasets(m) {
    var datasets = [];
    if (m.historical.length) {
      var fy0 = m.historical[m.historical.length - 1].fiscal_year;
      var histPoints = m.historical
        .filter(function (h) { return h.value !== null && h.value !== undefined; })
        .map(function (h) { return { x: h.fiscal_year - fy0, y: h.value }; });
      datasets.push({
        fcKey: "hist", label: "Historical", data: histPoints,
        borderColor: COLOR_INK, backgroundColor: COLOR_INK,
        borderWidth: 2, pointRadius: 0, tension: 0, order: 10,
      });
    }

    m.scenarios.forEach(function (s) {
      var cfg = null;
      for (var i = 0; i < SCENARIOS.length; i++) {
        if (SCENARIOS[i].level === String(s.quantile_level)) { cfg = SCENARIOS[i]; break; }
      }
      if (!cfg) return;
      var points = [];
      s.horizons.forEach(function (h, i) {
        var v = s.values[i];
        if (v !== null && v !== undefined) points.push({ x: h, y: v });
      });
      if (!points.length) return;
      var explicitColor = cfg.faded ? withAlpha(cfg.color, 0.55) : cfg.color;
      var terminalColor = withAlpha(cfg.color, 0.4);
      datasets.push({
        fcKey: cfg.key, label: cfg.label, data: points,
        borderColor: explicitColor, backgroundColor: cfg.color,
        borderWidth: 2, pointRadius: 0, tension: 0, order: 1,
        segment: {
          borderDash: function (ctx) {
            if (ctx.p0.parsed.x >= TERMINAL_START) return [4, 3];
            return cfg.faded ? [3, 2] : undefined;
          },
          borderColor: function (ctx) {
            return ctx.p0.parsed.x >= TERMINAL_START ? terminalColor : explicitColor;
          },
          borderWidth: function (ctx) {
            return ctx.p0.parsed.x >= TERMINAL_START ? 1.5 : 2;
          },
        },
      });
    });
    return datasets;
  }

  // Re-apply every legend chip's current on/off state via the public setDatasetVisibility API
  // (not a `hidden` flag baked into the dataset objects) -- used both after a metric-tab
  // switch (e.g. a user hid "Bear" on Revenue, then switches to Net Income: that choice
  // should carry over) and after the initial render, so both paths go through the same,
  // version-stable API rather than depending on how Chart.js's internal per-dataset meta
  // survives a wholesale chart.data.datasets replacement.
  function applyChipVisibility() {
    document.querySelectorAll(".fc-chip").forEach(function (chip) {
      var idx = chart.data.datasets.findIndex(function (d) { return d.fcKey === chip.dataset.scenario; });
      if (idx === -1) return;
      chart.setDatasetVisibility(idx, chip.dataset.active === "true");
    });
    chart.update();
  }

  function renderChart(metricKey) {
    var m = METRICS[metricKey];
    if (!m) return;
    if (titleEl) titleEl.textContent = m.label + (m.unit ? " · " + m.unit : "");
    var datasets = buildDatasets(m);
    var histMinX = m.historical.length
      ? m.historical[0].fiscal_year - m.historical[m.historical.length - 1].fiscal_year
      : 0;

    if (chart) {
      chart.data.datasets = datasets;
      chart.options.scales.x.min = histMinX;
      applyChipVisibility();
      return;
    }

    chart = new Chart(canvas.getContext("2d"), {
      type: "line",
      data: { datasets: datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        parsing: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: {
            type: "linear", min: histMinX, max: TERMINAL_END,
            grid: { color: COLOR_RULE_SOFT },
            ticks: {
              color: COLOR_INK3, font: { family: FONT_MONO, size: 10 },
              stepSize: 5, callback: fmtX,
            },
          },
          y: {
            grid: { color: COLOR_RULE_SOFT }, border: { display: false },
            ticks: { color: COLOR_INK3, font: { family: FONT_MONO, size: 11 }, callback: fmtY },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: function (items) { return items.length ? fmtX(items[0].parsed.x) : ""; },
              label: function (c) { return c.dataset.label + ": " + fmtY(c.parsed.y); },
            },
          },
        },
      },
      plugins: [zonePlugin],
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
      if (!chart) return;
      var willBeVisible = chip.dataset.active !== "true";
      chip.dataset.active = willBeVisible.toString();
      var idx = chart.data.datasets.findIndex(function (d) { return d.fcKey === chip.dataset.scenario; });
      if (idx === -1) return;
      chart.setDatasetVisibility(idx, willBeVisible);
      chart.update();
    });
  });

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
