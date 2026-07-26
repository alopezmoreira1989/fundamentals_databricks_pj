/*
 * Shared screener JS — General Screener / Net-Net Finder / Investor Presets.
 *
 * Loaded once, unconditionally, from base_screener.html (not a per-template {% block
 * extra_scripts %}) so it's present regardless of which mode was the initial full-page load.
 * This is what makes AJAX mode-switching possible: before this consolidation, each mode's own
 * within-mode auto-apply logic (filters/sort/pagination/level-pills) lived in that mode's own
 * template-local <script>, so swapping #main's content to a DIFFERENT mode via fetch would have
 * landed a mode with no event listeners bound to it at all.
 *
 * Two layers:
 *   1. Mode-switching — a delegated click listener on #main (persists across swaps) for the
 *      mode-nav's .nav-tabs links. Fetches with the X-Mode-Switch header, swaps #main's whole
 *      content, updates document.title from the response's X-Page-Title header (nothing outside
 *      #main, including <title>, is touched by an innerHTML swap otherwise), then re-runs
 *      initModeContent() to wire up whichever mode just landed.
 *   2. Within-mode interactions (filters, sort, pagination, level/preset pills) — unchanged
 *      behavior from before this consolidation, just relocated here and made re-invokable via
 *      initModeContent() after every swap. Each mode's own init function is a no-op (returns
 *      immediately) when that mode's container isn't present in the current DOM.
 *
 * Progressive enhancement throughout: every underlying <form method="get"> and <a href> still
 * works with JS disabled, exactly as before — this only intercepts and short-circuits them.
 */
(function () {
  'use strict';

  /* Fetch `url` and swap `target`'s innerHTML with the response, showing a dimmed loading state
     meanwhile. One shared abort-controller per target (stored ON the element) so a fast second
     request correctly supersedes an in-flight first one, whichever target it's for. `onDone`,
     if given, is called with the response's Headers once the swap completes (used only by the
     mode-switch case, to read X-Page-Title). */
  function fetchAndSwap(url, target, headers, push, onDone) {
    if (target._fsController) target._fsController.abort();
    var controller = new AbortController();
    target._fsController = controller;
    target.classList.add('fs-swap-loading');
    fetch(url, { headers: headers, signal: controller.signal })
      .then(function (r) {
        if (!r.ok) throw new Error('bad response: ' + r.status);
        return r.text().then(function (html) { return { html: html, headers: r.headers }; });
      })
      .then(function (result) {
        target.innerHTML = result.html;
        target.classList.remove('fs-swap-loading');
        if (push) history.pushState(null, '', url);
        if (onDone) onDone(result.headers);
      })
      .catch(function (err) {
        if (err.name === 'AbortError') return;  // superseded by a newer request — expected
        target.classList.remove('fs-swap-loading');
        // Leave the previous content showing rather than a blank page; stale beats empty. The
        // failure still needs to be visible somewhere for debugging.
        console.error('Screener update failed:', err);
      });
  }

  // ---- General Screener ----------------------------------------------------------------
  function initGeneralScreener() {
    var results = document.getElementById('scr-results');
    var form = document.getElementById('scr-filter-form');
    if (!results || !form) return;

    var search = document.querySelector('.scr-col-search');
    var rowsContainer = document.querySelector('.scr-filter-rows');

    if (search) {
      var grid = search.nextElementSibling;
      var labels = Array.prototype.slice.call(grid.querySelectorAll('label'));
      var noMatch = grid.nextElementSibling;

      search.addEventListener('input', function () {
        var q = search.value.trim().toLowerCase();
        var visible = 0;
        labels.forEach(function (label) {
          var match = !q || label.textContent.toLowerCase().indexOf(q) !== -1;
          label.classList.toggle('scr-hidden', !match);
          if (match) visible++;
        });
        noMatch.hidden = visible !== 0;
      });

      // Land focus in the search box whenever the Columns disclosure opens, so typing works
      // immediately. `toggle` only fires on an actual open/close interaction, never just from
      // the page loading with `open` already set server-side, so this never steals focus
      // unexpectedly on first render.
      var details = search.closest('details');
      details.addEventListener('toggle', function () {
        if (details.open) search.focus();
      });
    }

    // Metric filters: add/remove rows client-side. The server already accepts any number of
    // fmetric/fmin/fmax triplets (no hardcoded cap) — this only adds the UI to grow past the
    // 3 rows rendered by default.
    var addBtn = document.querySelector('.scr-add-filter');
    if (addBtn && rowsContainer) {
      var rowTemplate = document.querySelector('.scr-filter-row-template');
      addBtn.addEventListener('click', function () {
        rowsContainer.appendChild(rowTemplate.content.cloneNode(true));
      });
    }

    var debounceTimer = null;

    function applyForm() {
      var params = new URLSearchParams(new FormData(form));
      fetchAndSwap(window.location.pathname + '?' + params.toString(), results,
        { 'X-Requested-With': 'XMLHttpRequest' }, true);
    }

    form.addEventListener('submit', function (e) {
      e.preventDefault();
      clearTimeout(debounceTimer);
      applyForm();
    });

    form.addEventListener('change', function (e) {
      if (e.target.matches('input[type="text"]')) return;  // debounced on 'input' instead
      applyForm();
    });

    form.addEventListener('input', function (e) {
      // .scr-col-search is a purely client-side filter over the already-rendered checkbox
      // list (see above) — it must never trigger a server round-trip.
      if (e.target.type !== 'text' || e.target.classList.contains('scr-col-search')) return;
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(applyForm, 400);
    });

    // Removing a metric-filter row changes the active filter set immediately — re-apply like
    // any other change. Adding a blank row doesn't: nothing to apply until a metric is picked.
    if (rowsContainer) {
      rowsContainer.addEventListener('click', function (e) {
        if (e.target.closest('.scr-rm-filter')) applyForm();
      });
    }

    // Sort-header links, pagination links, and the table-columns toggle all live inside
    // #scr-results and get replaced on every swap — delegate from the container itself
    // (which persists across swaps) rather than binding elements that won't exist after the
    // next one.
    results.addEventListener('click', function (e) {
      var a = e.target.closest('.pagination a, thead a');
      if (!a) return;
      e.preventDefault();
      fetchAndSwap(a.href, results, { 'X-Requested-With': 'XMLHttpRequest' }, true);
    });

    results.addEventListener('change', function (e) {
      var box = e.target.closest('.scr-table-cols input[type="checkbox"]');
      if (!box) return;
      var params = new URLSearchParams(new FormData(box.closest('form')));
      fetchAndSwap(window.location.pathname + '?' + params.toString(), results,
        { 'X-Requested-With': 'XMLHttpRequest' }, true);
    });
  }

  // ---- Net-Net Finder --------------------------------------------------------------------
  function initNetNet() {
    var content = document.getElementById('nn-content');
    if (!content) return;

    // Sector/Index/Industry/Country/Market selects, plus the hide-value-traps toggle (which
    // lives in the hero card, associated to #nn-filter-form via its form="" attribute rather
    // than DOM nesting — FormData still picks it up regardless of where it physically sits).
    content.addEventListener('change', function (e) {
      if (!e.target.matches('#nn-sector, #nn-index, #nn-industry, #nn-country, #nn-market, #nn-hide-traps')) return;
      var form = document.getElementById('nn-filter-form');
      if (!form) return;
      var params = new URLSearchParams(new FormData(form));
      fetchAndSwap(window.location.pathname + '?' + params.toString(), content,
        { 'X-Requested-With': 'XMLHttpRequest' }, true);
    });

    // Level pills, sort-header links, and pagination links all live inside #nn-content and get
    // replaced on every swap — delegate from the container itself.
    content.addEventListener('click', function (e) {
      var a = e.target.closest('.levels a, thead a, .pagination a');
      if (!a) return;
      e.preventDefault();
      fetchAndSwap(a.href, content, { 'X-Requested-With': 'XMLHttpRequest' }, true);
    });
  }

  // ---- Investor Presets -------------------------------------------------------------------
  function initPresets() {
    var content = document.getElementById('presets-content');
    if (!content) return;

    // Sector/Index/Industry/Country/Market selects.
    content.addEventListener('change', function (e) {
      if (!e.target.matches('#presets-sector, #presets-index, #presets-industry, #presets-country, #presets-market')) return;
      var form = document.getElementById('presets-filter-form');
      if (!form) return;
      var params = new URLSearchParams(new FormData(form));
      fetchAndSwap(window.location.pathname + '?' + params.toString(), content,
        { 'X-Requested-With': 'XMLHttpRequest' }, true);
    });

    // Preset pills, sort-header links, and pagination links all live inside #presets-content
    // and get replaced on every swap — delegate from the container itself.
    content.addEventListener('click', function (e) {
      var a = e.target.closest('.levels a, thead a, .pagination a');
      if (!a) return;
      e.preventDefault();
      fetchAndSwap(a.href, content, { 'X-Requested-With': 'XMLHttpRequest' }, true);
    });
  }

  // Feature-detect which mode's container is currently in the DOM and wire that mode's own
  // behavior. Safe to call repeatedly (after every mode-switch swap): the previous mode's
  // elements — and any listeners bound to them — were discarded wholesale by the innerHTML
  // swap, so there's no double-binding risk on the new ones.
  function initModeContent() {
    if (document.getElementById('scr-results')) { initGeneralScreener(); return; }
    if (document.getElementById('nn-content')) { initNetNet(); return; }
    if (document.getElementById('presets-content')) { initPresets(); }
  }

  // Mode-switching: a delegated click listener on #main, which persists across swaps (only its
  // children are ever replaced), so this only needs binding once. .nav-tabs is unique to
  // _mode_nav.html, so this never intercepts unrelated links (sort headers, pagination, level/
  // preset pills all live in different, more specific containers matched by the mode-specific
  // init functions above).
  function initModeNav() {
    var main = document.getElementById('main');
    if (!main) return;
    main.addEventListener('click', function (e) {
      var a = e.target.closest('.nav-tabs a');
      if (!a) return;
      e.preventDefault();
      fetchAndSwap(a.href, main, { 'X-Mode-Switch': '1' }, true, function (headers) {
        // Percent-encoded server-side (see views._render_mode) — a raw em-dash in a header
        // value gets RFC 2047 MIME-encoded by Django, which Headers.get() does not undo.
        var title = headers.get('X-Page-Title');
        if (title) document.title = decodeURIComponent(title);
        initModeContent();
      });
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    initModeNav();
    initModeContent();

    // Back/forward: a swapped-in mode's field values (search text, selected dropdowns) live
    // outside any single pushed URL's own reach, so a full reload is the simplest way to
    // guarantee they end up back in sync with whichever URL the user navigated to. Registered
    // once here, not inside initModeContent() (which re-runs after every swap) — otherwise
    // repeated mode switches would stack up duplicate reload-on-popstate listeners.
    window.addEventListener('popstate', function () {
      window.location.reload();
    });
  });
})();
