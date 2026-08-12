// Price tab — adjusted close + SMA 20/50/200, rendered via TradingView Lightweight Charts
// (self-hosted, static/fundamentals_screener/js/vendor/lightweight-charts.standalone.production.js).
// Driven by the `price_tab_data` JSON embedded via {{ price_tab_data|json_script:"price-chart-data" }}
// in company_detail.html — replaces the previous hand-rolled inline-<svg> line chart
// (charts.price_line_chart) with a real interactive chart: native crosshair, hover tooltip,
// drag/scroll zoom-pan. The ?window= range buttons are unchanged (still a full-page
// server round trip — see charts.price_chart_data's docstring) since each window is a
// differently-downsampled series from the repository, not a client-side crop of one series.
//
// Colors are read at runtime from app.css's :root custom properties via getComputedStyle() —
// never hardcoded here, so the chart always matches whatever the current theme tokens are.
(function () {
  "use strict";

  var dataEl = document.getElementById("price-chart-data");
  var container = document.getElementById("price-chart-container");
  if (!dataEl || !container || typeof LightweightCharts === "undefined") return;
  var DATA = JSON.parse(dataEl.textContent);
  if (!DATA.length) return;

  var root = getComputedStyle(document.documentElement);
  var tok = function (name) { return root.getPropertyValue(name).trim(); };
  var COLOR_PRICE = tok("--ink");
  var COLOR_SMA20 = tok("--orange");
  var COLOR_SMA50 = tok("--positive");
  var COLOR_SMA200 = tok("--negative");
  var COLOR_RULE = tok("--rule");
  var COLOR_INK3 = tok("--ink-3");
  var FONT_MONO = tok("--mono");

  var chart = LightweightCharts.createChart(container, {
    layout: { background: { color: "transparent" }, textColor: COLOR_INK3, fontFamily: FONT_MONO, fontSize: 11 },
    grid: { vertLines: { color: COLOR_RULE }, horzLines: { color: COLOR_RULE } },
    rightPriceScale: { borderColor: COLOR_RULE },
    timeScale: { borderColor: COLOR_RULE },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    autoSize: true,
  });

  var SERIES = [
    { key: "adj_close", name: "Price", color: COLOR_PRICE, lineWidth: 2 },
    { key: "sma20", name: "SMA 20", color: COLOR_SMA20, lineWidth: 1 },
    { key: "sma50", name: "SMA 50", color: COLOR_SMA50, lineWidth: 1 },
    { key: "sma200", name: "SMA 200", color: COLOR_SMA200, lineWidth: 1 },
  ];

  var legendHtml = "";
  SERIES.forEach(function (s) {
    var points = DATA.filter(function (p) { return p[s.key] !== null && p[s.key] !== undefined; })
      .map(function (p) { return { time: p.date, value: p[s.key] }; });
    if (points.length < 2) return; // matches price_chart_data's own "need >=2 points" guard, per series
    var lineSeries = chart.addSeries(LightweightCharts.LineSeries, { color: s.color, lineWidth: s.lineWidth });
    lineSeries.setData(points);
    legendHtml += '<span><span class="dot" style="background:' + s.color + '"></span>' + s.name + "</span>";
  });

  var legendEl = document.getElementById("price-chart-legend");
  if (legendEl) legendEl.innerHTML = legendHtml;

  chart.timeScale().fitContent();

  // The Price tab-pane is `display:none` (Bootstrap tab) at page load unless it's the
  // active tab, so the container's width was ~0 when fitContent() ran above — that
  // computed logical range then stays stuck once the canvas widens later, leaving only a
  // few most-recent bars visible, right-anchored, with the rest of the chart empty. Re-fit
  // once the pane actually becomes visible (also covers the ?window= range buttons, which
  // do a full-page reload back onto the Price tab via a #pane-price hash restore — see
  // base template's DOMContentLoaded hash handling).
  //
  // Deferred via setTimeout, not called inline: with autoSize:true, chart.resize() is a
  // documented no-op (the library's own resize() short-circuits whenever autoSize is
  // active), and `shown.bs.tab` fires synchronously the moment Bootstrap's fade-in
  // transition ends — before the container's ResizeObserver callback (queued for a later
  // turn of the event loop) has actually widened the canvas. Calling fitContent() inline
  // here would just recompute the same wrong range against the still-stale canvas width.
  // Pushing it to a macrotask lets that pending resize land first.
  var tabTrigger = document.getElementById("tab-price");
  if (tabTrigger) {
    tabTrigger.addEventListener("shown.bs.tab", function () {
      setTimeout(function () { chart.timeScale().fitContent(); }, 50);
    });
  }
})();
