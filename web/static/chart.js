const state = {
  symbols: [],
  groupedSignals: {},
  availableDates: [],
  currentDate: null,
  currentSymbol: null,
  currentSignalId: null,
  signals: [],
  chart: null,
  candleSeries: null,
};

const levelColor = {
  A: '#17824d',
  B: '#d2a11d',
  C: '#8d959c',
};

const strategyLabels = {
  breakout: '短线打板 / 追涨',
  trend: '趋势跟随',
  dip: '低吸反弹',
};

const strategySourceLabels = {
  auto: '自动选择',
  manual: '手动指定',
  scan: '系统扫描',
};

const levelLabels = {
  A: 'A级',
  B: 'B级',
  C: 'C级',
};

const actionLabels = {
  buy: '买入',
  watch: '观察',
  ignore: '忽略',
};

function displayValue(value, fallback = '-') {
  return value === null || value === undefined || String(value).trim() === '' ? fallback : value;
}

function displayStrategy(value) {
  return strategyLabels[value] || displayValue(value);
}

function displayStrategySource(value) {
  return strategySourceLabels[value] || displayValue(value);
}

function displayLevel(value) {
  return levelLabels[value] || displayValue(value);
}

function displayAction(value) {
  return actionLabels[value] || displayValue(value);
}

function groupSignalsByDate(records) {
  const grouped = {};
  records.forEach((item) => {
    const runDate = item.run_date || '-';
    if (!grouped[runDate]) {
      grouped[runDate] = [];
    }
    grouped[runDate].push(item);
  });
  return grouped;
}

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

function renderSummary(symbolMeta, currentSignal = null, signalCount = 0) {
  const bar = document.getElementById('summary-bar');
  if (!symbolMeta) {
    bar.innerHTML = '<div class="pill">暂无信号</div>';
    return;
  }

  const signal = currentSignal || {};
  bar.innerHTML = `
    <div class="pill">股票: ${symbolMeta.symbol}</div>
    <div class="pill">当前信号日期: ${displayValue(signal.run_date, symbolMeta.run_date || '-')}</div>
    <div class="pill">当前策略类型: ${displayStrategy(signal.selected_mode || symbolMeta.selected_mode)}</div>
    <div class="pill">当前信号等级: ${displayLevel(signal.level || symbolMeta.level)}</div>
    <div class="pill">当前操作建议: ${displayAction(signal.action || symbolMeta.action)}</div>
    <div class="pill">当前得分: ${displayValue(signal.score, symbolMeta.score ?? '-')}</div>
    <div class="pill">历史信号数: ${signalCount || 0}</div>
  `;
}

function renderSignalDetail(signal) {
  const detail = document.getElementById('signal-detail');
  const performance = document.getElementById('performance-detail');
  if (!signal) {
    detail.textContent = '暂无信号详情';
    performance.textContent = '暂无可用表现数据';
    return;
  }

  detail.textContent = [
    `当前查看信号日期: ${displayValue(signal.run_date)}`,
    `信号编号: ${displayValue(signal.signal_id)}`,
    `策略类型: ${displayStrategy(signal.selected_mode)}`,
    `策略来源: ${displayStrategySource(signal.strategy_source)}`,
    `市场状态: ${displayValue(signal.market_state)}`,
    `信号排序: ${displayValue(signal.rank)}`,
    `得分: ${displayValue(signal.score)}`,
    `信号等级: ${displayLevel(signal.level)}`,
    `操作建议: ${displayAction(signal.action)}`,
    `期权方向: ${displayValue(signal.option_bias, '暂无')}`,
    `参考周期: ${displayValue(signal.option_horizon, '暂无')}`,
    `期权逻辑: ${displayValue(signal.option_reason, '暂无')}`,
    `主要风险: ${displayValue(signal.option_risk, '暂无')}`,
    `公告信号: ${displayValue(signal.tdnet_signal, '暂无')}`,
    `公告标题: ${displayValue(signal.tdnet_title, '暂无')}`,
    `收盘价: ${displayValue(signal.close)}`,
    `当日涨跌幅: ${displayValue(signal.day_change_pct)}`,
    `日内波动: ${displayValue(signal.intraday_pct)}`,
    `振幅: ${displayValue(signal.amplitude_pct)}`,
    `成交额比5日均值: ${displayValue(signal.amount_ratio_5)}`,
    `3日动量: ${displayValue(signal.momentum_3_pct)}`,
    `5日动量: ${displayValue(signal.momentum_5_pct)}`,
    `距20日高点: ${displayValue(signal.dist_to_high_20_pct)}`,
    `收盘位置: ${displayValue(signal.close_position)}`,
  ].join('\n');

  performance.textContent = [
    `1日后收益: ${displayValue(signal.ret_1d, '暂无')}`,
    `3日后收益: ${displayValue(signal.ret_3d, '暂无')}`,
    `5日后收益: ${displayValue(signal.ret_5d, '暂无')}`,
    `5日最大上涨: ${displayValue(signal.max_up_5d, '暂无')}`,
    `5日最大回撤: ${displayValue(signal.max_down_5d, '暂无')}`,
  ].join('\n');
}

function renderSignalList(signals, selectedSignalId = null) {
  const container = document.getElementById('signal-list');
  container.innerHTML = '';
  const symbolMeta = state.symbols.find((item) => item.signal_id === state.currentSignalId) || null;

  if (!signals.length) {
    container.innerHTML = '<div class="hint">该股票暂无信号记录。</div>';
    renderSignalDetail(null);
    renderSummary(symbolMeta, null, 0);
    return;
  }

  const initialSignal = signals.find((signal) => signal.signal_id === selectedSignalId) || signals[0];
  signals.forEach((signal, index) => {
    const item = document.createElement('div');
    item.className = `signal-item${signal.signal_id === initialSignal.signal_id ? ' active' : ''}`;
    item.dataset.signalId = signal.signal_id;
    item.innerHTML = `
      <div class="signal-meta">
        <span>${signal.run_date || '-'}</span>
        <span class="tag ${signal.level || 'C'}">${displayLevel(signal.level)}</span>
      </div>
      <div>${displayStrategy(signal.selected_mode)} / ${displayAction(signal.action)}</div>
      <div class="hint">得分=${signal.score ?? '-'} 排名=${signal.rank ?? '-'}</div>
    `;
    item.addEventListener('click', () => {
      document.querySelectorAll('.signal-item').forEach((node) => node.classList.remove('active'));
      item.classList.add('active');
      renderSignalDetail(signal);
      state.currentSignalId = signal.signal_id;
      renderSummary(symbolMeta, signal, signals.length);
    });
    container.appendChild(item);
  });

  state.currentSignalId = initialSignal.signal_id;
  renderSignalDetail(initialSignal);
  renderSummary(symbolMeta, initialSignal, signals.length);
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

async function loadSymbol(symbol, selectedSignalId = null) {
  state.currentSymbol = symbol;
  const [candles, signals] = await Promise.all([
    fetchJson(`/api/chart/${symbol}`),
    fetchJson(`/api/signals/${symbol}`),
  ]);

  state.signals = signals;
  setChartData(candles, signals);
  renderSignalList(signals, selectedSignalId);
}

function populateDateSelect(dates) {
  const select = document.getElementById('date-select');
  select.innerHTML = '';

  if (!dates.length) {
    select.innerHTML = '<option value="">暂无日期</option>';
    return;
  }

  dates.forEach((runDate) => {
    const option = document.createElement('option');
    option.value = runDate;
    option.textContent = runDate;
    select.appendChild(option);
  });
}

function populateSignalSelect(runDate, selectedSignalId = null) {
  const select = document.getElementById('signal-select');
  select.innerHTML = '';

  const records = state.groupedSignals[runDate] || [];
  if (!records.length) {
    select.innerHTML = '<option value="">暂无信号</option>';
    return null;
  }

  records.forEach((item) => {
    const option = document.createElement('option');
    option.value = item.signal_id;
    option.textContent = `${displayStrategy(item.selected_mode)} | ${item.symbol} | ${displayLevel(item.level)} | ${displayAction(item.action)}`;
    select.appendChild(option);
  });

  const target = records.find((item) => item.signal_id === selectedSignalId) || records[0];
  select.value = target.signal_id;
  return target;
}

async function init() {
  initChart();

  const symbols = await fetchJson('/api/signals/symbols');
  state.symbols = symbols;
  state.groupedSignals = groupSignalsByDate(symbols);
  state.availableDates = Object.keys(state.groupedSignals);

  const dateSelect = document.getElementById('date-select');
  const signalSelect = document.getElementById('signal-select');

  if (!symbols.length) {
    dateSelect.innerHTML = '<option value="">暂无日期</option>';
    signalSelect.innerHTML = '<option value="">暂无信号</option>';
    renderSummary(null);
    return;
  }

  populateDateSelect(state.availableDates);
  state.currentDate = state.availableDates[0];
  dateSelect.value = state.currentDate;
  const initialRecord = populateSignalSelect(state.currentDate, symbols[0].signal_id);

  if (initialRecord) {
    state.currentSignalId = initialRecord.signal_id;
    await loadSymbol(initialRecord.symbol, initialRecord.signal_id);
  }

  dateSelect.addEventListener('change', async (event) => {
    state.currentDate = event.target.value;
    const target = populateSignalSelect(state.currentDate);
    if (!target) {
      renderSummary(null);
      renderSignalDetail(null);
      return;
    }
    state.currentSignalId = target.signal_id;
    await loadSymbol(target.symbol, target.signal_id);
  });

  signalSelect.addEventListener('change', async (event) => {
    const signalId = event.target.value;
    const records = state.groupedSignals[state.currentDate] || [];
    const target = records.find((item) => item.signal_id === signalId);
    if (!target) {
      return;
    }
    state.currentSignalId = signalId;
    await loadSymbol(target.symbol, signalId);
  });

  document.getElementById('reload-btn').addEventListener('click', async () => {
    const signalId = signalSelect.value;
    const records = state.groupedSignals[state.currentDate] || [];
    const meta = records.find((item) => item.signal_id === signalId);
    if (!meta) {
      renderSummary(null);
      return;
    }
    renderSummary(meta);
    await loadSymbol(meta.symbol, signalId);
  });

  window.addEventListener('resize', resizeChart);
}

init().catch((error) => {
  document.getElementById('signal-detail').textContent = `加载失败: ${error.message}`;
});
