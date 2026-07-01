/**
 * CineTrends - Plotly.js Chart Initialization
 * All charts use a consistent dark theme matching the CSS design system.
 */

// Shared Plotly dark theme config
const DARK_THEME = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    font: { family: 'Inter, sans-serif', color: '#94a3b8', size: 12 },
    margin: { l: 50, r: 20, t: 20, b: 50 },
    xaxis: {
        gridcolor: 'rgba(148,163,184,0.08)',
        zerolinecolor: 'rgba(148,163,184,0.1)',
        tickfont: { color: '#64748b' },
    },
    yaxis: {
        gridcolor: 'rgba(148,163,184,0.08)',
        zerolinecolor: 'rgba(148,163,184,0.1)',
        tickfont: { color: '#64748b' },
    },
    legend: { font: { color: '#94a3b8' }, bgcolor: 'rgba(0,0,0,0)' },
};

const COLORS = ['#7c3aed', '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#ec4899', '#06b6d4', '#8b5cf6', '#14b8a6', '#f97316'];

const PLOTLY_CONFIG = {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
};

/**
 * Genre Donut Chart
 */
function initGenreDonut(elementId, data) {
    const trace = {
        labels: data.labels,
        values: data.values,
        type: 'pie',
        hole: 0.55,
        marker: { colors: COLORS },
        textinfo: 'label+percent',
        textposition: 'outside',
        textfont: { color: '#94a3b8', size: 11 },
        hoverinfo: 'label+value+percent',
        hoverlabel: { bgcolor: '#252836', bordercolor: '#7c3aed', font: { color: '#e2e8f0' } },
    };

    const layout = {
        ...DARK_THEME,
        showlegend: false,
        margin: { l: 20, r: 20, t: 20, b: 20 },
        annotations: [{
            text: 'Genres',
            font: { size: 16, color: '#e2e8f0', family: 'Inter' },
            showarrow: false,
        }],
    };

    Plotly.newPlot(elementId, [trace], layout, PLOTLY_CONFIG);
}

/**
 * Popularity Timeline (multi-line chart)
 */
function initPopularityTimeline(elementId, data) {
    const traces = data.series.map((series, i) => ({
        x: data.dates,
        y: series.values,
        name: series.name,
        type: 'scatter',
        mode: 'lines+markers',
        line: { color: COLORS[i % COLORS.length], width: 2.5, shape: 'spline' },
        marker: { size: 5 },
        hoverlabel: { bgcolor: '#252836', bordercolor: COLORS[i % COLORS.length] },
    }));

    const layout = {
        ...DARK_THEME,
        xaxis: { ...DARK_THEME.xaxis, type: 'date' },
        yaxis: { ...DARK_THEME.yaxis, title: { text: 'Popularity', font: { color: '#64748b' } } },
        legend: { ...DARK_THEME.legend, orientation: 'h', y: -0.2 },
        hovermode: 'x unified',
    };

    Plotly.newPlot(elementId, traces, layout, PLOTLY_CONFIG);
}

/**
 * Position Bar Chart (horizontal bars showing avg position)
 */
function initPositionChart(elementId, data) {
    const barColors = data.changes.map(c => c > 0 ? '#10b981' : c < 0 ? '#ef4444' : '#64748b');

    const trace = {
        y: data.movies,
        x: data.positions,
        type: 'bar',
        orientation: 'h',
        marker: { color: barColors, borderRadius: 4 },
        text: data.changes.map(c => c > 0 ? `+${c.toFixed(1)}%` : `${c.toFixed(1)}%`),
        textposition: 'outside',
        textfont: { color: '#94a3b8', size: 11 },
        hoverlabel: { bgcolor: '#252836' },
    };

    const layout = {
        ...DARK_THEME,
        xaxis: { ...DARK_THEME.xaxis, title: { text: 'Avg Position', font: { color: '#64748b' } } },
        yaxis: { ...DARK_THEME.yaxis, autorange: 'reversed' },
        margin: { l: 150, r: 60, t: 20, b: 50 },
    };

    Plotly.newPlot(elementId, [trace], layout, PLOTLY_CONFIG);
}

/**
 * Budget vs Revenue Scatter Plot
 */
function initBudgetRevenueScatter(elementId, data) {
    const colors = data.roi.map(r => r > 0 ? '#10b981' : '#ef4444');
    const sizes = data.roi.map(r => Math.min(Math.max(Math.abs(r) / 10 + 8, 8), 30));

    const trace = {
        x: data.budgets,
        y: data.revenues,
        text: data.titles.map((t, i) => `${t}<br>ROI: ${data.roi[i].toFixed(0)}%`),
        mode: 'markers',
        type: 'scatter',
        marker: { color: colors, size: sizes, opacity: 0.8, line: { color: 'rgba(255,255,255,0.2)', width: 1 } },
        hoverinfo: 'text',
        hoverlabel: { bgcolor: '#252836', bordercolor: '#7c3aed', font: { color: '#e2e8f0' } },
    };

    // Break-even line
    const maxBudget = Math.max(...data.budgets);
    const maxRevenue = Math.max(...data.revenues);
    const breakEven = {
        x: [0, maxBudget],
        y: [0, maxBudget],
        mode: 'lines',
        type: 'scatter',
        line: { color: 'rgba(148,163,184,0.3)', width: 1, dash: 'dash' },
        name: 'Break-even',
        hoverinfo: 'skip',
    };

    const layout = {
        ...DARK_THEME,
        xaxis: { ...DARK_THEME.xaxis, title: { text: 'Budget ($M)', font: { color: '#64748b' } } },
        yaxis: { ...DARK_THEME.yaxis, title: { text: 'Revenue ($M)', font: { color: '#64748b' } } },
        showlegend: false,
    };

    Plotly.newPlot(elementId, [breakEven, trace], layout, PLOTLY_CONFIG);
}

/**
 * Genre Grouped Bar Chart (monthly)
 */
function initGenreHeatmap(elementId, data) {
    const traceCount = {
        x: data.genres,
        y: data.counts,
        name: 'Trending Films',
        type: 'bar',
        marker: { color: '#7c3aed', borderRadius: 4 },
        hoverlabel: { bgcolor: '#252836' },
    };

    const tracePopularity = {
        x: data.genres,
        y: data.popularity,
        name: 'Avg Popularity',
        type: 'bar',
        marker: { color: '#3b82f6', borderRadius: 4 },
        yaxis: 'y2',
        hoverlabel: { bgcolor: '#252836' },
    };

    const layout = {
        ...DARK_THEME,
        barmode: 'group',
        yaxis: { ...DARK_THEME.yaxis, title: { text: 'Trending Films', font: { color: '#64748b' } } },
        yaxis2: {
            ...DARK_THEME.yaxis,
            title: { text: 'Avg Popularity', font: { color: '#64748b' } },
            overlaying: 'y',
            side: 'right',
        },
        legend: { ...DARK_THEME.legend, orientation: 'h', y: 1.15 },
        margin: { l: 50, r: 50, t: 30, b: 80 },
        xaxis: { ...DARK_THEME.xaxis, tickangle: -30 },
    };

    Plotly.newPlot(elementId, [traceCount, tracePopularity], layout, PLOTLY_CONFIG);
}
