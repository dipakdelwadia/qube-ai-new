const SYNCFUSION_LICENSE_KEY = 'ORg4AjUWIQA/Gnt3VVhhQlJDfV5AQmBIYVp/TGpJfl96cVxMZVVBJAtUQF1hTH5Ud0JjW31YcnJdTmBaWkdy';

ej.base.registerLicense(SYNCFUSION_LICENSE_KEY);

const CHART_CONFIG = {
    // Default chart theme
    theme: 'Material',
    
    enabledCharts: {
        bar: true,
        column: true,
        line: true,
        area: true,
        pie: true,
        doughnut: true,
        scatter: true,
        stackedColumn: true,
        spline: false,
        bubble: false,
        stacked: false,
        stackedBar: false,
        radar: false,
        polar: false,
        funnel: false,
        pyramid: false,
        heatmap: false,
        boxplot: false,
        waterfall: false
    },
    
    // Default color palette for charts (custom set)
    palette: [
        '#003f5c', '#2f4b7c', '#665191', '#a05195', '#d45087', '#f95d6a', '#ff7c43', '#ffa600',
        '#2ca02c', '#17becf', '#1f77b4', '#8fbc8f', '#bcbd22', '#8c1622', '#c51b8a', '#e0ff4f',
        '#fb9a99', '#262626'
    ],
    
    // Enhanced chart type support - simplified to core set
    advancedCharts: {
        kpi: true,
        combo: true,
        map_scatter: false,
        map_bubble: false,
        map_pie: false,
        map_bubble_pie: false,
        pivot: false,
        web: false
    }
}; 