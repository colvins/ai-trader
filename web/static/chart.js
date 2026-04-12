const state = {
  symbols: [],
  currentSymbol: null,
  signals: [],
  chart: null,
  candleSeries: null,
};

const levelColor = {
  A: '#17824d',
  B: '#d2a11d',
  C: '#8d959c',
};

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Request failed: ${url}`);
  }
  return res.json();
}

function toBusinessDay(value) {
  if (!value) {
    return null;
  }
  const text = String(value).trim();
  const parts = text.split('-');
  if (parts.length !== 3) {
    return null;
  }
  const [year, month, day] = parts.map((item) => Number(item));
  if (!year || !month || !day) {
    return null;
  }
  return { year, month, day };
}

function resizeChart() {
  if (!state.chart) {
    return;
  }
  const chartBox = document.getElementById('chart');
  const width = Math.max(chartBox.clientWidth || 0, 300);
  const height = Math.max(chartBox.clientHeight || 0, 320);
  state.chart.resize(width, height);
}

function initChart() {
  const container = document.getElementById('chart');
  state.chart = LightweightCharts.createChart(container, {
    width: Math.max(container.clientWidth || 0, 300),
    height: Math.max(container.clientHeight || 0, 320),
    layout: {
      background: { color: '#fffdf8' },
      textColor: '#1d241f',
    },
    grid: {
      vertLines: { color: '#eee7da' },
      horzLines: { color: '#eee7da' },
    },
    rightPriceScale: {
      borderColor: '#d8cfbf',
    },
    timeScale: {
      borderColor: '#d8cfbf',
      timeVisible: true,
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
  });

  state.candleSeries = state.chart.addCandlestickSeries({
    upColor: '#17824d',
    downColor: '#c9482c',
    borderVisible: false,
    wickUpColor: '#17824d',
    wickDownColor: '#c9482c',
  });

  resizeChart();
}

function renderSummary(symbolMeta) {
  const bar = document.getElementById('summary-bar');
  if (!symbolMeta) {
    bar.innerHTML = '<div class="pill">暂无信号</div>';
    return;
  }

  bar.innerHTML = `
    <div class="pill">股票: ${symbolMeta.symbol}</div>
    <div class="pill">最近日期: ${symbolMeta.latest_run_date || '-'}</div>
    <div class="pill">最近等级: ${symbolMeta.latest_level || '-'}</div>
    <div class="pill">最近动作: ${symbolMeta.latest_action || '-'}</div>
    <div class="pill">最近得分: ${symbolMeta.latest_score ?? '-'}</div>
  `;
}

function renderSignalDetail(signal) {
  const detail = document.getElementById('signal-detail');
  if (!signal) {
    detail.textContent = '暂无信号详情';
    return;
  }

  detail.textContent = [
    `signal_id: ${signal.signal_id || '-'}`,
    `run_date: ${signal.run_date || '-'}`,
    `selected_mode: ${signal.selected_mode || '-'}`,
    `strategy_source: ${signal.strategy_source || '-'}`,
    `market_state: ${signal.market_state || '-'}`,
    `rank: ${signal.rank ?? '-'}`,
    `score: ${signal.score ?? '-'}`,
    `level: ${signal.level || '-'}`,
    `action: ${signal.action || '-'}`,
    `option_bias: ${signal.option_bias || '-'}`,
    `option_horizon: ${signal.option_horizon || '-'}`,
    `option_reason: ${signal.option_reason || '-'}`,
    `option_risk: ${signal.option_risk || '-'}`,
    `close: ${signal.close ?? '-'}`,
    `day_change_pct: ${signal.day_change_pct ?? '-'}`,
    `intraday_pct: ${signal.intraday_pct ?? '-'}`,
    `amplitude_pct: ${signal.amplitude_pct ?? '-'}`,
    `amount_ratio_5: ${signal.amount_ratio_5 ?? '-'}`,
    `momentum_3_pct: ${signal.momentum_3_pct ?? '-'}`,
    `momentum_5_pct: ${signal.momentum_5_pct ?? '-'}`,
    `dist_to_high_20_pct: ${signal.dist_to_high_20_pct ?? '-'}`,
    `close_position: ${signal.close_position ?? '-'}`,
    `ret_1d: ${signal.ret_1d ?? '-'}`,
    `ret_3d: ${signal.ret_3d ?? '-'}`,
    `ret_5d: ${signal.ret_5d ?? '-'}`,
    `max_up_5d: ${signal.max_up_5d ?? '-'}`,
    `max_down_5d: ${signal.max_down_5d ?? '-'}`,
  ].join('\n');
}

function renderSignalList(signals) {
  const container = document.getElementById('signal-list');
  container.innerHTML = '';

  if (!signals.length) {
    container.innerHTML = '<div class="hint">该股票暂无信号记录。</div>';
    renderSignalDetail(null);
    return;
  }

  signals.forEach((signal, index) => {
    const item = document.createElement('div');
    item.className = `signal-item${index === 0 ? ' active' : ''}`;
    item.dataset.signalId = signal.signal_id;
    item.innerHTML = `
      <div class="signal-meta">
        <span>${signal.run_date || '-'}</span>
        <span class="tag ${signal.level || 'C'}">${signal.level || 'C'}</span>
      </div>
      <div>${signal.selected_mode || '-'} / ${signal.action || '-'}</div>
      <div class="hint">score=${signal.score ?? '-'} rank=${signal.rank ?? '-'}</div>
    `;
    item.addEventListener('click', () => {
      document.querySelectorAll('.signal-item').forEach((node) => node.classList.remove('active'));
      item.classList.add('active');
      renderSignalDetail(signal);
    });
    container.appendChild(item);
  });

  renderSignalDetail(signals[0]);
}

function setChartData(candles, signals) {
  const candleData = candles
    .map((item) => ({
      time: toBusinessDay(item.date),
      open: Number(item.open),
      high: Number(item.high),
      low: Number(item.low),
      close: Number(item.close),
    }))
    .filter(
      (item) =>
        item.time &&
        Number.isFinite(item.open) &&
        Number.isFinite(item.high) &&
        Number.isFinite(item.low) &&
        Number.isFinite(item.close)
    );

  state.candleSeries.setData(candleData);

  const markers = signals
    .map((signal) => ({
      time: toBusinessDay(signal.run_date),
      level: signal.level || 'C',
      score: signal.score,
      action: signal.action || '-',
    }))
    .filter((signal) => signal.time)
    .map((signal) => ({
      time: signal.time,
      position: 'belowBar',
      color: levelColor[signal.level] || levelColor.C,
      shape: 'circle',
      text: `${signal.level} ${signal.score ?? '-'} ${signal.action}`,
    }));

  state.candleSeries.setMarkers(markers);
  resizeChart();
  requestAnimationFrame(() => {
    resizeChart();
    state.chart.timeScale().fitContent();
  });
}

async function loadSymbol(symbol) {
  state.currentSymbol = symbol;
  const [candles, signals] = await Promise.all([
    fetchJson(`/api/chart/${symbol}`),
    fetchJson(`/api/signals/${symbol}`),
  ]);

  state.signals = signals;
  setChartData(candles, signals);
  renderSignalList(signals);
}

async function init() {
  initChart();

  const symbols = await fetchJson('/api/signals/symbols');
  state.symbols = symbols;

  const select = document.getElementById('symbol-select');
  select.innerHTML = '';

  if (!symbols.length) {
    select.innerHTML = '<option value="">暂无信号</option>';
    renderSummary(null);
    return;
  }

  symbols.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.symbol;
    option.textContent = `${item.symbol} | ${item.latest_level || '-'} | ${item.latest_action || '-'}`;
    select.appendChild(option);
  });

  renderSummary(symbols[0]);
  await loadSymbol(symbols[0].symbol);

  select.addEventListener('change', async (event) => {
    const symbol = event.target.value;
    const meta = state.symbols.find((item) => item.symbol === symbol);
    renderSummary(meta);
    await loadSymbol(symbol);
  });

  document.getElementById('reload-btn').addEventListener('click', async () => {
    const symbol = select.value;
    const meta = state.symbols.find((item) => item.symbol === symbol);
    renderSummary(meta);
    await loadSymbol(symbol);
  });

  window.addEventListener('resize', resizeChart);
}

init().catch((error) => {
  document.getElementById('signal-detail').textContent = `加载失败: ${error.message}`;
});
