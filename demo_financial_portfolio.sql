
    -- Financial Portfolio Analysis with Complex Window Functions
    WITH daily_returns AS (
        SELECT 
            symbol,
            trade_date,
            closing_price,
            volume,
            LAG(closing_price) OVER (
                PARTITION BY symbol 
                ORDER BY trade_date
            ) as prev_closing_price,
            (closing_price - LAG(closing_price) OVER (
                PARTITION BY symbol 
                ORDER BY trade_date
            )) / LAG(closing_price) OVER (
                PARTITION BY symbol 
                ORDER BY trade_date
            ) * 100 as daily_return_pct
        FROM stock_prices
        WHERE trade_date >= '2023-01-01'
    ),
    
    volatility_metrics AS (
        SELECT 
            symbol,
            AVG(daily_return_pct) as avg_return,
            STDDEV(daily_return_pct) as volatility,
            MIN(daily_return_pct) as worst_day,
            MAX(daily_return_pct) as best_day,
            COUNT(*) as trading_days
        FROM daily_returns
        WHERE daily_return_pct IS NOT NULL
        GROUP BY symbol
    ),
    
    portfolio_weights AS (
        SELECT 
            p.portfolio_id,
            p.symbol,
            p.shares_held,
            s.current_price,
            p.shares_held * s.current_price as position_value,
            SUM(p.shares_held * s.current_price) OVER (
                PARTITION BY p.portfolio_id
            ) as total_portfolio_value
        FROM portfolio_holdings p
        INNER JOIN stocks s ON p.symbol = s.symbol
    ),
    
    risk_adjusted_returns AS (
        SELECT 
            pw.portfolio_id,
            pw.symbol,
            pw.position_value,
            pw.position_value / pw.total_portfolio_value as weight,
            vm.avg_return,
            vm.volatility,
            CASE 
                WHEN vm.volatility > 0 THEN vm.avg_return / vm.volatility
                ELSE 0
            END as sharpe_ratio
        FROM portfolio_weights pw
        INNER JOIN volatility_metrics vm ON pw.symbol = vm.symbol
    )
    
    SELECT 
        rar.portfolio_id,
        rar.symbol,
        rar.weight,
        rar.avg_return,
        rar.volatility,
        rar.sharpe_ratio,
        sector_performance.sector_name,
        sector_performance.sector_avg_return,
        market_cap.market_cap_category
    FROM risk_adjusted_returns rar
    LEFT JOIN (
        SELECT 
            s.symbol,
            s.sector_name,
            AVG(dr.daily_return_pct) as sector_avg_return
        FROM stocks s
        INNER JOIN daily_returns dr ON s.symbol = dr.symbol
        GROUP BY s.symbol, s.sector_name
    ) sector_performance ON rar.symbol = sector_performance.symbol
    LEFT JOIN (
        SELECT 
            symbol,
            CASE 
                WHEN market_cap > 10000000000 THEN 'Large Cap'
                WHEN market_cap > 2000000000 THEN 'Mid Cap'
                ELSE 'Small Cap'
            END as market_cap_category
        FROM stocks
    ) market_cap ON rar.symbol = market_cap.symbol
    WHERE rar.weight > 0.01  -- Only positions > 1% of portfolio
    ORDER BY rar.portfolio_id, rar.weight DESC;
    