// Income Statement / Cash Flow / Quarterly tab charts — Chart.js bar/combo, self-hosted
// (static/fundamentals_screener/js/vendor/chart.umd.js). Driven by two JSON payloads:
//   #statement-chart-data  — {"Income Statement": {labels, series}, "Cash Flow": {...}}
//                             (charts.income_statement_chart / charts.cash_flow_chart via
//                             views._tab_chart_json, keyed by Statement.name)
//   #quarterly-chart-data   — {labels, series} (charts.quarterly_chart)
// Each canvas that should render one of these carries a data-* attribute naming which payload
// key it wants: <canvas data-statement-chart="Income Statement"> / <canvas data-quarterly-chart>.
// Replaces the previous hand-rolled inline-<svg> bar/combo charts (charts._bar_chart_svg,
// charts._ocf_fcf_chart_svg) with real hover tooltips and Chart.js's own axis-tick density.
//
// Colors are read at runtime from app.css's :root custom properties via getComputedStyle() —
// never hardcoded here. Series are styled by NAME (a small fixed mapping below), not by
// position, since "Revenue"/"Net Income"/"Operating CF"/"Free CF (= OCF − CapEx)" are the only
// series these three charts ever emit (see charts.py's income_statement_chart/cash_flow_chart/
// quarterly_chart) — a name this mapping doesn't recognize falls back to a plain accent bar.
(function () {
  "use strict";

  if (typeof Chart === "undefined") return;

  var root = getComputedStyle(document.documentElement);
  var tok = function (name) { return root.getPropertyValue(name).trim(); };
  var COLOR_ACCENT = tok("--accent");
  var COLOR_ACCENT_SOFT = tok("--accent-soft");
  var COLOR_POSITIVE = tok("--positive");
  var COLOR_INK3 = tok("--ink-3");
  var COLOR_RULE = tok("--rule");
  var FONT_MONO = tok("--mono");

  // Phase 5.7a: the ticker's real reporting currency (company_detail.html's shared
  // #chart-currency-data payload) — replaces a hardcoded "$" that mislabeled non-USD figures
  // in the tooltip (the axis ticks below stay bare/unprefixed, unchanged).
  var ccyEl = document.getElementById("chart-currency-data");
  var CCY = ccyEl ? JSON.parse(ccyEl.textContent) : "USD";

  function compact(v) {
    var a = Math.abs(v);
    if (a >= 1e12) return (v / 1e12).toFixed(1) + "T";
    if (a >= 1e9) return (v / 1e9).toFixed(1) + "B";
    if (a >= 1e6) return (v / 1e6).toFixed(1) + "M";
    if (a >= 1e3) return (v / 1e3).toFixed(1) + "K";
    return v.toFixed(0);
  }

  // Name -> { Chart.js dataset overrides, legendColor }. Order here also fixes z-order (a wide
  // "Operating CF" bar behind a narrower "Free CF" bar in front, like the original SVG).
  function datasetFor(name, kind) {
    if (kind === "line") {
      return {
        type: "line", borderColor: COLOR_POSITIVE, backgroundColor: COLOR_POSITIVE,
        borderWidth: 2.5, pointRadius: 3, tension: 0, order: 1, legendColor: COLOR_POSITIVE,
      };
    }
    if (name.indexOf("Operating CF") === 0) {
      return {
        type: "bar", backgroundColor: COLOR_ACCENT_SOFT, borderColor: COLOR_ACCENT, borderWidth: 1,
        borderRadius: 2, barPercentage: 0.9, categoryPercentage: 0.7, order: 2, legendColor: COLOR_ACCENT,
      };
    }
    if (name.indexOf("Free CF") === 0) {
      return {
        type: "bar", backgroundColor: COLOR_POSITIVE, borderRadius: 2,
        barPercentage: 0.42, categoryPercentage: 0.7, order: 1, legendColor: COLOR_POSITIVE,
      };
    }
    return { type: "bar", backgroundColor: COLOR_ACCENT, borderRadius: 2, order: 2, legendColor: COLOR_ACCENT };
  }

  function renderChart(canvas, payload) {
    if (!payload || !payload.series || !payload.series.length) return;
    var datasets = payload.series.map(function (s) {
      var style = datasetFor(s.name, s.kind);
      return Object.assign({ label: s.name, data: s.values }, style);
    });
    new Chart(canvas.getContext("2d"), {
      type: "bar",
      data: { labels: payload.labels, datasets: datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        scales: {
          x: { grid: { display: false }, ticks: { color: COLOR_INK3, font: { family: FONT_MONO, size: 11 } } },
          y: {
            grid: { color: COLOR_RULE }, border: { display: false },
            ticks: { color: COLOR_INK3, font: { family: FONT_MONO, size: 11 }, callback: compact },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: function (c) {
            var body = compact(c.parsed.y);
            return c.dataset.label + ": " + (CCY === "USD" ? "$" + body : body + " " + CCY);
          } } },
        },
      },
    });

    var legendEl = document.querySelector('[data-legend-for="' + (canvas.dataset.statementChart || "quarterly") + '"]');
    if (legendEl) {
      legendEl.innerHTML = datasets.map(function (d) {
        return '<span><span class="dot" style="background:' + d.legendColor + '"></span>' + d.label + "</span>";
      }).join("");
    }
  }

  var statementDataEl = document.getElementById("statement-chart-data");
  if (statementDataEl) {
    var STATEMENT_DATA = JSON.parse(statementDataEl.textContent);
    document.querySelectorAll("[data-statement-chart]").forEach(function (canvas) {
      renderChart(canvas, STATEMENT_DATA[canvas.dataset.statementChart]);
    });
  }

  var quarterlyDataEl = document.getElementById("quarterly-chart-data");
  var quarterlyCanvas = document.querySelector("[data-quarterly-chart]");
  if (quarterlyDataEl && quarterlyCanvas) {
    renderChart(quarterlyCanvas, JSON.parse(quarterlyDataEl.textContent));
  }
})();
