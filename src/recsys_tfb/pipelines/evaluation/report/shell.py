"""HTML report generation for evaluation results: the page around the
sections, shared by the main report and the comparison report.

The shell draws the title, the metadata block, the table of contents (static
HTML), and each section — its heading, description and content blocks
(``report.types``; drawn by ``report.pages.render_block``); nothing collapses
but a figure's data table, under the figure. Every section needs a ``key`` (its
HTML id is ``s-{key}``). It carries the page's CSS, the table tool (sort,
filter, copy for every ``div.tblk``) and the one term list (``TERMS``: a key
term's first use in each section gets a dotted underline with its English on
hover). No network, no package beyond plotly, whose JS is embedded once.

**Every string is escaped**: the ``<title>``, the ``<h1>``, the metadata keys
and values, the table of contents, section titles and descriptions, table
titles, headers, row labels, cells, the lines under a table and the
definitions. A section cannot put markup in any of them.
"""

import json
from pathlib import Path

import plotly.offline

from recsys_tfb.report.pages import (
    escape,
    escape_attr,
    render_block,
)
from recsys_tfb.report.types import ReportSection  # noqa: F401

#: The page's CSS: ADR-0031 decision 16's look (the demo the owner approved),
#: light and dark. Colours are tokens on ``:root``, redefined for dark mode.
_CSS = """
:root {
  --bg: #ffffff; --fg: #2a2f36; --muted: #5d6570; --head: #333a42;
  --line: #dadfe5; --th: #f3f5f7; --stripe: #fafbfc; --meta: #edf2f7;
  --accent: #0b63c5; --formula-bg: #f4f6f9; --formula-line: #94a3b8;
  --warn: #b0500c;
  --font: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang TC", "Noto Sans TC", sans-serif;
  --mono: "SF Mono", Menlo, Consolas, monospace;
  color-scheme: light;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #15181c; --fg: #e2e6ea; --muted: #a3acb6; --head: #eef1f4;
  --line: #343b44; --th: #1f242a; --stripe: #191d22; --meta: #1b2430;
  --accent: #74b2ff; --formula-bg: #1c2128; --formula-line: #5b6b80;
  --warn: #f2a45c;
  color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #15181c; --fg: #e2e6ea; --muted: #a3acb6; --head: #eef1f4;
  --line: #343b44; --th: #1f242a; --stripe: #191d22; --meta: #1b2430;
  --accent: #74b2ff; --formula-bg: #1c2128; --formula-line: #5b6b80;
  --warn: #f2a45c;
  color-scheme: dark; }
[hidden] { display: none !important; }
body { background: var(--bg); color: var(--fg); font-family: var(--font);
  font-size: 15px; line-height: 1.55; margin: 0;
  padding-left: max(16px, env(safe-area-inset-left, 0px));
  padding-right: max(16px, env(safe-area-inset-right, 0px)); padding-block: 0 48px; }
.wrap { max-width: 1120px; margin: 0 auto; }
.report h1 { color: var(--head); font-size: 28px; margin: 24px 0 16px; }
.report h2 { color: var(--head); border-bottom: 1px solid var(--line); padding-bottom: 8px;
  font-size: 22px; margin: 0 0 10px; text-wrap: balance; }
.report h1 .en, .report h2 .en { font-size: .55em; font-weight: 500; color: var(--muted);
  margin-left: 10px; letter-spacing: .02em; }
.report h3 { font-size: 17px; color: var(--head); margin: 26px 0 4px; padding-left: 8px;
  border-left: 3px solid var(--accent); }
.report table { border-collapse: collapse; margin: 4px 0; font-variant-numeric: tabular-nums; }
.report th, .report td { border: 1px solid var(--line); padding: 6px 10px; text-align: left; }
.report th { background: var(--th); }
.report tr:nth-child(even) td { background: var(--stripe); }
.report td.num { text-align: right; }
.report td.dl { color: var(--muted); }
.report th.grp { text-align: center; }
.tw { overflow-x: auto; max-width: 100%; }
.metadata { background: var(--meta); padding: 12px 16px; border-radius: 8px; margin-bottom: 20px; }
.metadata table { margin: 0; }
.section { margin-bottom: 44px; }
.description { color: var(--muted); margin: 0 0 12px; max-width: 95ch; }
.note { font-size: 13.5px; color: var(--muted); margin: 2px 0 10px; }
.formula { font-family: var(--mono); background: var(--formula-bg);
  border-left: 3px solid var(--formula-line); padding: 10px 16px; margin: 12px 0 16px;
  border-radius: 2px; font-size: .92em; white-space: pre-wrap; overflow-x: auto; }
.section-bullets { margin: 10px 0 14px; padding-left: 22px; }
.section-bullets li { margin: 4px 0; color: var(--fg); }
nav.toc { background: var(--th); padding: 12px 20px; border-radius: 4px; margin-bottom: 28px; }
nav.toc strong { display: block; margin-bottom: 6px; color: var(--head); }
nav.toc ol { margin: 4px 0 0; padding-left: 22px; display: grid; gap: 4px; }
nav.toc a { color: var(--accent); text-decoration: none; }
nav.toc a:hover { text-decoration: underline; }
nav.toc .q { color: var(--muted); }
.defs { margin: 6px 0 14px; padding: 8px 12px; border: 1px dashed var(--line);
  border-radius: 6px; display: grid; gap: 3px; font-size: 13.5px; }
.defs-src { font-size: 12px; color: var(--muted); }
.def { display: flex; flex-wrap: wrap; gap: 2px 10px; }
.def dt { font-weight: 600; }
.def dd { margin: 0; color: var(--muted); }
sup.warn, span.warn, .warn-note { color: var(--warn); }
.warn-note { font-size: 13.5px; margin: 2px 0 10px; }
.tblk { margin: 16px 0 6px; width: fit-content; max-width: 100%; }
details.fig-data { margin: 0 0 14px; }
details.fig-data > summary { width: fit-content; cursor: pointer; font-size: 13.5px; color: var(--accent); }
details.fig-data > .tblk { margin-top: 6px; }
.thead-row { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: end; gap: 6px 16px; margin-bottom: 2px; }
h4.tt { margin: 0; font-size: 15px; color: var(--head); }
.cpy { font: inherit; font-size: 12.5px; padding: 2px 10px; border: 1px solid var(--line); border-radius: 4px;
  background: var(--th); color: var(--fg); cursor: pointer; }
.cpy:hover { border-color: var(--accent); }
.cpy:focus-visible, th.srt:focus-visible, .fmenu :focus-visible, .fstat button:focus-visible {
  outline: 2px solid var(--accent); outline-offset: 1px; }
.fstat { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; font-size: 13px; color: var(--muted); margin: 4px 0 2px; }
.fstat .chip { font: inherit; border: 1px solid var(--accent); color: var(--accent); background: transparent;
  border-radius: 999px; padding: 1px 10px; cursor: pointer; }
.fstat .chip-clear { font: inherit; border: none; background: none; color: var(--muted); text-decoration: underline; cursor: pointer; }
.tblk th, .tblk td { white-space: nowrap; }
th.srt { cursor: pointer; user-select: none; }
th.srt:hover { background: var(--meta); }
.fbtn { margin-left: 6px; font-size: 10px; padding: 1px 3px; border-radius: 3px; color: var(--muted); }
.fbtn::after { content: "\\25BC"; opacity: .45; }
th.srt[aria-sort="ascending"] .fbtn::after { content: "\\25B2"; opacity: 1; color: var(--accent); }
th.srt[aria-sort="descending"] .fbtn::after { content: "\\25BC"; opacity: 1; color: var(--accent); }
th.srt.filtered .fbtn { background: var(--accent); }
th.srt.filtered .fbtn::after { color: var(--bg); opacity: 1; }
tr.pin th, tr.pin td { font-weight: 600; }
.tw.tall { max-height: 26rem; overflow: auto; }
.tw.tall thead th { position: sticky; top: 0; z-index: 1; }
.fmenu { position: absolute; z-index: 50; width: 15rem; box-sizing: border-box; display: grid; gap: 8px;
  background: var(--bg); color: var(--fg); border: 1px solid var(--line); border-radius: 8px;
  box-shadow: 0 8px 28px rgba(0, 0, 0, .22); padding: 10px 12px; font-size: 13.5px; }
.fm-title { font-weight: 600; color: var(--head); }
.fm-sort, .fm-act { display: flex; flex-wrap: wrap; gap: 4px; }
.fm-act { justify-content: flex-end; }
.fmenu button { font: inherit; font-size: 13px; border: 1px solid var(--line); background: var(--th); color: var(--fg);
  border-radius: 4px; padding: 2px 8px; cursor: pointer; }
.fmenu button.on { border-color: var(--accent); color: var(--accent); }
.fmenu .fm-ok { background: var(--accent); color: var(--bg); border-color: var(--accent); }
.fm-list { max-height: 12rem; overflow: auto; display: grid; gap: 2px; border: 1px solid var(--line);
  border-radius: 4px; padding: 4px 6px; }
.fm-range { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
.fm-range label { display: grid; gap: 2px; color: var(--muted); }
.fm-range input, .fm-q { font: inherit; width: 100%; box-sizing: border-box; padding: 3px 6px;
  border: 1px solid var(--line); border-radius: 4px; background: var(--bg); color: var(--fg); }
.fm-hint { color: var(--muted); font-size: 12px; }
.term { border-bottom: 1px dotted var(--muted); cursor: help; }
#to-top { position: fixed; bottom: 24px; right: 24px; background: var(--accent); color: var(--bg);
  border: none; border-radius: 50%; width: 44px; height: 44px; font-size: 20px; cursor: pointer;
  box-shadow: 0 2px 8px rgba(0, 0, 0, .2); opacity: 0; pointer-events: none;
  transition: opacity .2s; z-index: 1000; }
#to-top.visible { opacity: .9; pointer-events: auto; }
@media (max-width: 600px) { .report h1 { font-size: 22px; } .report h2 { font-size: 19px; } }
@media (prefers-reduced-motion: reduce) { * { scroll-behavior: auto !important; } }
"""

#: The back-to-top button.
_NAV_JS = (
    "(function(){"
    "var btn = document.getElementById('to-top');"
    "window.addEventListener('scroll', function(){"
    "btn.classList.toggle('visible', window.scrollY > 300);"
    "});"
    "btn.addEventListener('click', function(){"
    "window.scrollTo({top: 0, behavior: 'smooth'});"
    "});"
    "})();"
)

#: The key terms, Chinese to English: the one list of the page (ADR-0031
#: decision 16); sections keep no list of their own.
TERMS = {
    "分群": "segment", "大類": "category", "正例": "positive（label=1）",
    "熱門度": "popularity", "名次": "rank", "歸因": "attribution",
    "候選列": "candidate row", "全正": "all-positive", "等權": "equal weight",
    "分箱": "binning", "細箱": "fine bin", "門檻": "threshold",
    "正例率": "positive rate", "母體": "population", "截斷": "truncation",
    "對照": "reference", "涵蓋": "coverage", "粒度": "grain",
}

#: The table tool for every ``div.tblk`` (sort and filter from a column
#: header, copy the visible rows as tab-separated text: headers from
#: ``data-full``, numbers from ``data-v``, never the ⚠ marks; ``tr.pin`` rows
#: stay on top and are never filtered out), then the key-term underlines.
#: ``%TERMS%`` is replaced by :data:`TERMS` as JSON.
_TABLE_JS = r"""
(function(){
  var TERMS = %TERMS%;
  function shown(el){ return getComputedStyle(el).display !== 'none'; }
  // the text a reader sees, without hidden variants or the ⚠ marks
  function vis(el){
    var out = '';
    Array.prototype.forEach.call(el.childNodes, function(c){
      if (c.nodeType === 3) out += c.nodeValue;
      else if (c.nodeType === 1 && !c.matches('sup.warn') && shown(c)) out += vis(c);
    });
    return out.replace(/\s+/g, ' ').trim();
  }
  function num(c){ var v = c.getAttribute('data-v'); return v !== null && v !== '' ? parseFloat(v) : null; }
  function esc(t){ return String(t).replace(/[&<>"]/g, function(ch){
    return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[ch]; }); }

  // one shared menu, opened from any column header
  var menu = document.createElement('div');
  menu.className = 'fmenu'; menu.hidden = true; menu.setAttribute('role', 'dialog');
  document.body.appendChild(menu);
  var openFor = null;
  function closeMenu(focus){ var th = openFor && openFor.th; menu.hidden = true; openFor = null; if (focus && th) th.focus(); }
  document.addEventListener('mousedown', function(e){
    if (!menu.hidden && !menu.contains(e.target) && !openFor.th.contains(e.target)) closeMenu(false); });
  document.addEventListener('keydown', function(e){ if (e.key === 'Escape' && !menu.hidden) closeMenu(true); });

  function Ctl(blk){
    var self = this;
    this.tbl = blk.querySelector('table'); this.tb = this.tbl.tBodies[0];
    this.stat = blk.querySelector('.fstat');
    this.rows = Array.prototype.slice.call(this.tb.rows);
    this.rows.forEach(function(r, i){ r.setAttribute('data-i', i); });
    this.leafs = Array.prototype.slice.call(this.tbl.tHead.querySelectorAll('th.srt'));
    this.filters = {}; this.sortCol = null; this.sortDir = 'none';
    this.leafs.forEach(function(th, col){
      th.tabIndex = 0; th.setAttribute('aria-haspopup', 'dialog'); th.title = '排序與篩選';
      var mark = document.createElement('span'); mark.className = 'fbtn'; mark.setAttribute('aria-hidden', 'true');
      th.appendChild(mark);
      th.addEventListener('click', function(){
        if (openFor && openFor.th === th) closeMenu(false); else self.openMenu(col, th); });
      th.addEventListener('keydown', function(e){
        if (e.key === 'Enter' || e.key === ' ' || e.key === 'ArrowDown') { e.preventDefault(); self.openMenu(col, th); } });
    });
    var btn = blk.querySelector('.cpy');
    btn.addEventListener('click', function(){ self.copy(btn); });
  }
  Ctl.prototype.free = function(){ return this.rows.filter(function(r){ return !r.classList.contains('pin'); }); };
  Ctl.prototype.isNum = function(col){ return this.free().some(function(r){ return num(r.cells[col]) !== null; }); };
  Ctl.prototype.label = function(col){ return this.leafs[col].getAttribute('data-full') || '列名'; };

  Ctl.prototype.openMenu = function(col, th){
    var self = this, numeric = this.isNum(col), f = this.filters[col];
    var h = '<div class="fm-title">' + esc(this.label(col)) + '</div><div class="fm-sort">'
      + (numeric ? '<button type="button" data-s="ascending">由小到大</button><button type="button" data-s="descending">由大到小</button>'
                 : '<button type="button" data-s="ascending">A → Z</button><button type="button" data-s="descending">Z → A</button>')
      + '<button type="button" data-s="none">原本順序</button></div>';
    if (numeric) {
      h += '<div class="fm-range"><label>最小<input type="number" step="any" class="fm-min"></label>'
         + '<label>最大<input type="number" step="any" class="fm-max"></label></div>'
         + '<div class="fm-hint">只填一邊也可以。</div>';
    } else {
      var vals = [];
      this.free().forEach(function(r){ var v = vis(r.cells[col]); if (vals.indexOf(v) < 0) vals.push(v); });
      vals.sort(function(a, b){ return a.localeCompare(b, 'zh-Hant', {numeric: true}); });
      h += (vals.length > 8 ? '<input type="search" class="fm-q" placeholder="找值" aria-label="找值">' : '')
         + '<label><input type="checkbox" class="fm-all"> 全選</label><div class="fm-list">'
         + vals.map(function(v){ var on = !f || f.values.indexOf(v) >= 0;
             return '<label><input type="checkbox" value="' + esc(v) + '"' + (on ? ' checked' : '') + '> ' + esc(v || '（空白）') + '</label>'; }).join('')
         + '</div>';
    }
    h += '<div class="fm-act"><button type="button" class="fm-clear">清除這欄</button><button type="button" class="fm-ok">套用</button></div>';
    menu.innerHTML = h; menu.hidden = false; openFor = {th: th};
    if (numeric && f) {
      if (f.min !== null) menu.querySelector('.fm-min').value = f.min;
      if (f.max !== null) menu.querySelector('.fm-max').value = f.max;
    }
    menu.querySelectorAll('[data-s]').forEach(function(b){
      if (self.sortCol === col && self.sortDir === b.getAttribute('data-s')) b.classList.add('on');
      b.addEventListener('click', function(){ self.sortBy(col, b.getAttribute('data-s')); closeMenu(true); });
    });
    var all = menu.querySelector('.fm-all');
    if (all) {
      var boxes = function(){ return Array.prototype.filter.call(menu.querySelectorAll('.fm-list input'),
        function(x){ return !x.parentElement.hidden; }); };
      var sync = function(){ var bs = boxes(); var n = bs.filter(function(x){ return x.checked; }).length;
        all.checked = n === bs.length; all.indeterminate = n > 0 && n < bs.length; };
      all.addEventListener('change', function(){ var on = all.checked; boxes().forEach(function(x){ x.checked = on; }); sync(); });
      menu.querySelector('.fm-list').addEventListener('change', sync); sync();
      var q = menu.querySelector('.fm-q');
      if (q) q.addEventListener('input', function(){ var t = q.value.trim().toLowerCase();
        menu.querySelectorAll('.fm-list label').forEach(function(l){ l.hidden = !!t && l.textContent.toLowerCase().indexOf(t) < 0; });
        sync(); });
    }
    menu.querySelector('.fm-clear').addEventListener('click', function(){ delete self.filters[col]; self.apply(); closeMenu(true); });
    menu.querySelector('.fm-ok').addEventListener('click', function(){
      if (numeric) {
        var mn = menu.querySelector('.fm-min').value, mx = menu.querySelector('.fm-max').value;
        if (mn === '' && mx === '') delete self.filters[col];
        else self.filters[col] = {min: mn === '' ? null : parseFloat(mn), max: mx === '' ? null : parseFloat(mx)};
      } else {
        var list = Array.prototype.slice.call(menu.querySelectorAll('.fm-list input'));
        var picked = list.filter(function(x){ return x.checked; }).map(function(x){ return x.value; });
        if (picked.length === list.length) delete self.filters[col]; else self.filters[col] = {values: picked};
      }
      self.apply(); closeMenu(true);
    });
    var r = th.getBoundingClientRect(), w = menu.offsetWidth;
    var left = Math.max(8, Math.min(r.left, document.documentElement.clientWidth - w - 8));
    menu.style.left = (left + window.scrollX) + 'px';
    menu.style.top = (r.bottom + window.scrollY + 4) + 'px';
    var first = menu.querySelector('.fm-sort button'); if (first) first.focus();
  };

  Ctl.prototype.sortBy = function(col, dir){
    this.sortCol = dir === 'none' ? null : col; this.sortDir = dir;
    this.leafs.forEach(function(o){ o.removeAttribute('aria-sort'); });
    if (dir !== 'none') this.leafs[col].setAttribute('aria-sort', dir);
    var s = dir === 'ascending' ? 1 : -1;
    var rest = this.free().slice();
    rest.sort(function(a, b){
      if (dir === 'none') return a.getAttribute('data-i') - b.getAttribute('data-i');
      var ca = a.cells[col], cb = b.cells[col], va = num(ca), vb = num(cb);
      if (va !== null && vb !== null) return s * (va - vb);
      if (va === null && vb !== null) return 1;
      if (vb === null && va !== null) return -1;
      return s * vis(ca).localeCompare(vis(cb), 'zh-Hant', {numeric: true});
    });
    var tb = this.tb;
    this.rows.filter(function(r){ return r.classList.contains('pin'); }).concat(rest)
      .forEach(function(r){ tb.appendChild(r); });
  };

  Ctl.prototype.apply = function(){
    var self = this, fs = this.filters, cols = Object.keys(fs), k = 0;
    this.rows.forEach(function(r){
      var ok = r.classList.contains('pin') || cols.every(function(c){
        var f = fs[c], cell = r.cells[c];
        if (f.values) return f.values.indexOf(vis(cell)) >= 0;
        var v = num(cell);
        return v !== null && (f.min === null || v >= f.min) && (f.max === null || v <= f.max);
      });
      r.hidden = !ok; if (ok) k++;
    });
    this.leafs.forEach(function(th, c){ th.classList.toggle('filtered', fs.hasOwnProperty(c)); });
    if (!cols.length) { this.stat.hidden = true; this.stat.innerHTML = ''; return; }
    var h = '<span>顯示 ' + k + '／' + this.rows.length + ' 列</span>';
    cols.forEach(function(c){
      var f = fs[c], what;
      if (f.values) what = f.values.length === 0 ? '都沒選' : f.values.length <= 2 ? f.values.join('、') : f.values.length + ' 個值';
      else what = f.min !== null && f.max !== null ? f.min + '～' + f.max : f.min !== null ? '≥ ' + f.min : '≤ ' + f.max;
      h += '<button type="button" class="chip" data-c="' + c + '" title="拿掉這個篩選">'
         + esc(self.label(c)) + '：' + esc(what) + ' ✕</button>';
    });
    h += '<button type="button" class="chip-clear">清除全部</button>';
    this.stat.innerHTML = h; this.stat.hidden = false;
    this.stat.querySelectorAll('.chip').forEach(function(b){
      b.addEventListener('click', function(){ delete self.filters[b.getAttribute('data-c')]; self.apply(); }); });
    this.stat.querySelector('.chip-clear').addEventListener('click', function(){ self.filters = {}; self.apply(); });
  };

  Ctl.prototype.copy = function(btn){
    var lines = [Array.prototype.map.call(this.tbl.tHead.querySelectorAll('th[data-full]'),
      function(th){ return th.getAttribute('data-full'); }).join('\t')];
    Array.prototype.forEach.call(this.tb.rows, function(r){
      if (r.hidden) return;
      lines.push(Array.prototype.map.call(r.cells, function(c){
        var v = c.getAttribute('data-v'); return v !== null && v !== '' ? v : vis(c); }).join('\t'));
    });
    var text = lines.join('\n');
    var done = function(){ btn.textContent = '已複製'; setTimeout(function(){ btn.textContent = '複製'; }, 1500); };
    var fallback = function(){
      var ta = document.createElement('textarea'); ta.value = text;
      ta.style.position = 'fixed'; ta.style.opacity = '0'; document.body.appendChild(ta); ta.select();
      var ok = false; try { ok = document.execCommand('copy'); } catch (e) {}
      document.body.removeChild(ta);
      if (ok) done(); else btn.textContent = '瀏覽器擋住了複製';
    };
    if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done, fallback);
    else fallback();
  };

  document.querySelectorAll('.tblk').forEach(function(blk){ new Ctl(blk); });

  // first use of a key term in each section: dotted underline, English on hover
  var SKIP = 'table, h1, h2, h3, h4, summary, .thead-row, .fstat, .fmenu, .term, .defs, .formula, code, svg, '
           + '.warn-note, .plotly-graph-div';
  document.querySelectorAll('.report .section').forEach(function(sec){
    var seen = {};
    var walker = document.createTreeWalker(sec, NodeFilter.SHOW_TEXT, { acceptNode: function(node){
      var p = node.parentElement;
      return !p || p.closest(SKIP) ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT; } });
    var nodes = []; while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach(function(node){
      while (node) {
        var text = node.nodeValue, best = -1, bt = null;
        for (var t in TERMS) { if (seen[t]) continue; var i = text.indexOf(t);
          if (i >= 0 && (best < 0 || i < best)) { best = i; bt = t; } }
        if (bt === null) return;
        var hit = node.splitText(best), rest = hit.splitText(bt.length);
        var span = document.createElement('span'); span.className = 'term'; span.title = TERMS[bt]; span.textContent = bt;
        hit.parentNode.replaceChild(span, hit); seen[bt] = true; node = rest;
      }
    });
  });
})();
"""


def _ascii_js(source: str) -> str:
    """``source`` with every non-ASCII character written as a ``\\uXXXX``
    escape. Every one of them sits in a string literal or a comment, where the
    escape means the same character; written out, the script's words would
    sit in the page source beside the report's own text, and a search of the
    page — a reader's, or a test's "this section is absent" — would find them
    there."""
    return "".join(c if ord(c) < 128 else f"\\u{ord(c):04x}" for c in source)


def section_id(section: ReportSection) -> str:
    """The HTML id of ``section``: ``s-{key}``."""
    if not section.key:
        raise ValueError(f"section {section.title!r} has no key")
    return f"s-{section.key}"


def _toc(sections: list[ReportSection]) -> list[str]:
    """The table of contents, static HTML."""
    parts = ['<nav class="toc">', "<strong>目錄</strong>", "<ol>"]
    for section in sections:
        q = (f'<span class="q"> — {escape(section.question)}</span>'
             if section.question else "")
        parts.append(f'<li><a href="#{escape_attr(section_id(section))}">'
                     f"{escape(section.title)}</a>{q}</li>")
    parts += ["</ol>", "</nav>"]
    return parts


def _section(section: ReportSection) -> list[str]:
    """One section: its heading and description, then its content blocks."""
    sid = escape_attr(section_id(section))
    en = (f'<span class="en">{escape(section.title_en)}</span>'
          if section.title_en else "")
    parts = [f'<div class="section" id="{sid}">',
             f"<h2>{escape(section.title)}{en}</h2>"]
    # A section that needs more than text says it with content blocks.
    if section.description:
        parts.append(f'<p class="description">{escape(section.description)}</p>')
    parts.extend(render_block(block) for block in section.blocks)
    parts.append("</div>")
    return parts


def generate_html_report(
    sections: list[ReportSection],
    title: str = "Model Evaluation Report",
    metadata: dict | None = None,
    title_en: str = "",
) -> str:
    """Generate a self-contained HTML report with embedded Plotly charts.

    Args:
        sections: The sections, in page order, each with a ``key``. Two with the same ``key``
            raise ``ValueError`` (their ids would collide).
        title: The report title, in Chinese (the ``<h1>`` and ``<title>``).
        metadata: Optional metadata dict shown at the top, one row a key.
        title_en: The English beside the ``<h1>``; none when empty.

    Returns:
        Complete HTML string.
    """
    for s in sections:
        section_id(s)  # no key -> ValueError
    keys = [s.key for s in sections]
    dupes = sorted({k for k in keys if keys.count(k) > 1})
    if dupes:
        raise ValueError(f"sections share a key: {dupes}")
    plotly_js = plotly.offline.get_plotlyjs()
    en = f'<span class="en">{escape(title_en)}</span>' if title_en else ""

    html_parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width,initial-scale=1">',
        f"<title>{escape(title)}</title>",
        f"<style>{_CSS}</style>",
        f"<script>{plotly_js}</script>",
        "</head><body>",
        '<div class="wrap"><main class="report">',
        f"<h1>{escape(title)}{en}</h1>",
    ]

    if metadata:
        html_parts.append('<div class="metadata">')
        html_parts.append("<table>")
        for key, value in metadata.items():
            html_parts.append(
                f"<tr><th>{escape(key)}</th><td>{escape(value)}</td></tr>")
        html_parts.append("</table></div>")

    html_parts.extend(_toc(sections))
    for section in sections:
        html_parts.extend(_section(section))

    html_parts.append("</main></div>")
    html_parts.append(
        '<button id="to-top" title="Back to top" aria-label="Back to top">↑</button>'
    )
    html_parts.append(f"<script>{_NAV_JS}</script>")
    html_parts.append(f"<script>{_ascii_js(_TABLE_JS.replace('%TERMS%', json.dumps(TERMS)))}</script>")
    html_parts.append("</body></html>")

    return "\n".join(html_parts)


def save_report(html: str, output_dir: str | Path) -> Path:
    """Write report.html to output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "report.html"
    path.write_text(html, encoding="utf-8")
    return path


def save_metrics_json(metrics: dict, output_dir: str | Path) -> Path:
    """Write metrics.json to output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "metrics.json"
    path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
