// Forecasting tab (issue #336) — fan chart + forward-multiples table, driven by the
// `chart_data` JSON embedded via {{ chart_data|json_script:"fc-data" }} in forecasting.html
// (no extra request; forecasting_data() is a separate JSON sibling for API consumers only).
//
// Chart geometry mirrors docs/mockups/forecasting_tab.html's own sharedScale()/toPath()
// almost verbatim — a single min/max computed across the historical line AND every scenario
// line for a metric, so every line shares one y-scale/domain and visually originates from the
// same FY0 point (issue #336's explicit required test).
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

  // x=200 is FY0 (today) on the fixed 0..760 viewBox; historical years step back by 32px/year,
  // forecast horizons step forward by 53px/year (FY+10 lands at x=730) — same spacing the
  // mockup used for its own fixed 6-historical/11-forecast point arrays, generalized here to
  // whatever length of history a given ticker actually has.
  function histX(index, len) {
    return 200 - (len - 1 - index) * 32;
  }
  function forecastX(horizon) {
    return 200 + horizon * 53;
  }
  function toY(value, scale) {
    return 250 - ((value - scale.lo) / (scale.hi - scale.lo)) * 230;
  }

  function sharedScale(m) {
    var all = m.historical.map(function (h) { return h.value; });
    m.scenarios.forEach(function (s) { all = all.concat(s.values); });
    all = all.filter(function (v) { return v !== null && v !== undefined; });
    if (!all.length) return { lo: 0, hi: 1 };
    var min = Math.min.apply(null, all);
    var max = Math.max.apply(null, all);
    var pad = (max - min) * 0.08 || 1;
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

  function scenarioPath(series, scale) {
    var d = "";
    series.horizons.forEach(function (h, i) {
      var v = series.values[i];
      if (v === null || v === undefined) return;
      d += (d === "" ? "M" : " L") + forecastX(h).toFixed(1) + "," + toY(v, scale).toFixed(1);
    });
    return d;
  }

  function renderChart(metricKey) {
    var m = METRICS[metricKey];
    if (!m) return;
    titleEl.textContent = m.label + (m.unit ? " · " + m.unit : "");
    var scale = sharedScale(m);
    document.getElementById("ln-hist").setAttribute("d", histPath(m, scale));
    m.scenarios.forEach(function (s) {
      var key = SCENARIO_KEYS[String(s.quantile_level)];
      var el = key && document.getElementById("ln-" + key);
      if (el) el.setAttribute("d", scenarioPath(s, scale));
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
      var el = document.getElementById("ln-" + chip.dataset.scenario);
      if (el) el.setAttribute("data-hidden", active.toString());
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
