// Balance Sheet composition — Chart.js horizontal 100%-stacked bar (Assets | Liabilities &
// Equity), self-hosted (static/fundamentals_screener/js/vendor/chart.umd.js). Driven by the
// `bs_compositions_data` JSON embedded via {{ bs_compositions_data|json_script:"bs-chart-data" }}
// in company_detail.html — every fiscal year is embedded in one payload, exactly like the
// previous CSS-bar version embedded every year's markup server-side; the year <select> just
// swaps which year's datasets are shown, client-side, same mechanism as the old bsShowYear().
//
// This is the one chart of the four that doesn't map onto a stock Chart.js type: each stack's
// segments have their OWN names/colors (Assets' "Cash & Equivalents" vs Liabilities & Equity's
// "Accounts Payable"), not a shared set of named series across both bars — so each dataset here
// is one "slot" (a position within its own stack), with the OTHER bar's value left `null` so
// Chart.js draws nothing there. The real segment name/value for the tooltip is read from a
// parallel segmentNames/segmentValues array on the dataset, not dataset.label. The current/
// non-current divider rule (the black line in the original SVG/CSS version) has no Chart.js
// built-in — a small custom plugin (segDivider, below) draws it.
//
// Colors for the divider line come from app.css's :root custom properties via
// getComputedStyle(); every segment's own fill color is server-computed (see charts._ramp) and
// arrives ready-to-use in the payload — a dark→light shade ramp isn't something a single CSS
// custom property could express, so it's the one color genuinely NOT read from a token here.
(function () {
  "use strict";

  var dataEl = document.getElementById("bs-chart-data");
  var canvas = document.getElementById("bs-chart");
  var select = document.getElementById("bs-year-select");
  if (!dataEl || !canvas || !select || typeof Chart === "undefined") return;
  var YEARS = JSON.parse(dataEl.textContent);
  if (!YEARS.length) return;

  // Phase 5.7a: the ticker's real reporting currency (company_detail.html's shared
  // #chart-currency-data payload) — replaces a hardcoded "$" that mislabeled non-USD figures.
  var ccyEl = document.getElementById("chart-currency-data");
  var CCY = ccyEl ? JSON.parse(ccyEl.textContent) : "USD";

  var root = getComputedStyle(document.documentElement);
  var tok = function (name) { return root.getPropertyValue(name).trim(); };
  var COLOR_INK = tok("--ink");
  var COLOR_INK3 = tok("--ink-3");
  var FONT_MONO = tok("--mono");

  function compact(v) {
    var a = Math.abs(v);
    var body;
    if (a >= 1e9) body = (v / 1e9).toFixed(1) + "B";
    else if (a >= 1e6) body = (v / 1e6).toFixed(1) + "M";
    else body = v.toFixed(0);
    return CCY === "USD" ? "$" + body : body + " " + CCY;
  }

  var dividerPlugin = {
    id: "segDivider",
    afterDatasetsDraw: function (chart) {
      var meta0 = chart.getDatasetMeta(0);
      if (!meta0 || !meta0.data.length) return;
      var boundaries = chart.data.boundaries || [];
      var ctx = chart.ctx;
      var xScale = chart.scales.x;
      var yScale = chart.scales.y;
      boundaries.forEach(function (b, catIndex) {
        if (b == null) return;
        var bar = meta0.data[catIndex];
        if (!bar) return;
        var xPix = xScale.getPixelForValue(b);
        var top = yScale.getPixelForValue(catIndex) - bar.height / 2;
        var bottom = yScale.getPixelForValue(catIndex) + bar.height / 2;
        ctx.save();
        ctx.strokeStyle = COLOR_INK;
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(xPix, top);
        ctx.lineTo(xPix, bottom);
        ctx.stroke();
        ctx.restore();
      });
    },
  };

  function buildDatasets(stackA, stackB) {
    var maxSlots = Math.max(stackA.segments.length, stackB.segments.length);
    var datasets = [];
    for (var i = 0; i < maxSlots; i++) {
      var segA = stackA.segments[i] || null;
      var segB = stackB.segments[i] || null;
      datasets.push({
        label: "slot" + i,
        data: [segA ? segA.pct : null, segB ? segB.pct : null],
        backgroundColor: [segA ? segA.color : "transparent", segB ? segB.color : "transparent"],
        segmentNames: [segA ? segA.name : null, segB ? segB.name : null],
        segmentValues: [segA ? segA.value : null, segB ? segB.value : null],
        stack: "composition",
        barPercentage: 0.6,
      });
    }
    return datasets;
  }

  function boundaryOffset(stack) {
    // First segment with boundary=true marks where the current/non-current divider goes —
    // the cumulative pct of every segment before it (see charts._ramped).
    var idx = stack.segments.findIndex(function (s) { return s.boundary; });
    if (idx <= 0) return null;
    return stack.segments.slice(0, idx).reduce(function (sum, s) { return sum + s.pct; }, 0);
  }

  var chart = new Chart(canvas.getContext("2d"), {
    type: "bar",
    data: { labels: ["Assets", "Liabilities & Equity"], datasets: [], boundaries: [] },
    options: {
      indexAxis: "y",
      responsive: true, maintainAspectRatio: false,
      scales: {
        x: { stacked: true, min: 0, max: 100, display: false },
        y: { stacked: true, grid: { display: false }, ticks: { color: COLOR_INK3, font: { family: FONT_MONO, size: 12 } } },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function (c) {
              var name = c.dataset.segmentNames[c.dataIndex];
              if (!name) return null;
              var val = c.dataset.segmentValues[c.dataIndex];
              return name + ": " + compact(val) + " (" + c.raw.toFixed(1) + "%)";
            },
          },
        },
      },
    },
    plugins: [dividerPlugin],
  });

  function renderLegend(elId, stack) {
    var el = document.getElementById(elId);
    if (!el) return;
    el.innerHTML = stack.segments.map(function (s) {
      return '<div class="bs-leg-item"><span><span class="bs-leg-dot" style="background:' + s.color + '"></span>'
        + s.name + "</span><span class=\"mono\">" + compact(s.value) + "</span></div>";
    }).join("");
  }

  function showYear(year) {
    var y = YEARS.filter(function (r) { return String(r.year) === String(year); })[0];
    if (!y) return;
    chart.data.datasets = buildDatasets(y.assets, y.liabilities_equity);
    chart.data.boundaries = [boundaryOffset(y.assets), boundaryOffset(y.liabilities_equity)];
    chart.update();
    var assetsTotal = document.getElementById("bs-assets-total");
    var leTotal = document.getElementById("bs-le-total");
    if (assetsTotal) assetsTotal.textContent = compact(y.assets.total);
    if (leTotal) leTotal.textContent = compact(y.liabilities_equity.total);
    renderLegend("bs-assets-legend", y.assets);
    renderLegend("bs-le-legend", y.liabilities_equity);
  }

  select.addEventListener("change", function (e) { showYear(e.target.value); });
  showYear(YEARS[0].year);
})();
