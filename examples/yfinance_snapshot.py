#!/usr/bin/env python3
"""Print a guided, best-effort tour of yfinance's data APIs.

Install with: python -m pip install yfinance
Run with:     python examples/yfinance_snapshot.py
              python examples/yfinance_snapshot.py AAPL MSFT SPY

Yahoo Finance may omit fields or throttle individual endpoints. Each section is
isolated so the rest of the snapshot still prints when one request fails.
"""

from __future__ import annotations

import argparse
import threading
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Callable

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("Missing dependency. Install it with: python -m pip install yfinance")


def show(value: Any, max_rows: int = 5, max_items: int = 12) -> None:
    """Print compact previews and a short hint about the returned object."""
    if value is None:
        print("  (no data returned)")
    elif hasattr(value, "empty") and hasattr(value, "head"):
        if value.empty:
            print("  (empty table)")
        else:
            print(value.head(max_rows).to_string())
            if len(value) > max_rows:
                print(f"  ... {len(value) - max_rows} more rows")
            print(f"  Shape: {value.shape}; columns: {list(value.columns)}")
    elif isinstance(value, Mapping):
        items = list(value.items())
        for key, item in items[:max_items]:
            print(f"  {key}:")
            show(item, max_rows=3, max_items=6)
        if len(items) > max_items:
            print(f"  ... {len(items) - max_items} more fields")
    elif isinstance(value, (list, tuple)):
        if not value:
            print("  (empty list)")
        else:
            for item in value[:max_rows]:
                print(f"  - {item}")
            if len(value) > max_rows:
                print(f"  ... {len(value) - max_rows} more items")
            print(f"  Items: {len(value)}")
    else:
        print(f"  {value}")


def section(title: str, description: str, fetch: Callable[[], Any]) -> Any:
    print(f"\n{'=' * 78}\n{title}\n{'-' * 78}\n{description}")
    try:
        result = fetch()
        show(result)
        return result
    except Exception as exc:  # Yahoo endpoints can fail independently.
        print(f"  Unavailable: {type(exc).__name__}: {exc}")
        return None


def fields(**fetchers: Callable[[], Any]) -> dict[str, Any]:
    """Fetch related values independently, preserving partial results."""
    results: dict[str, Any] = {}
    for label, fetch in fetchers.items():
        try:
            results[label] = fetch()
        except Exception as exc:
            results[label] = f"Unavailable: {type(exc).__name__}: {exc}"
    return results


def run_snapshot(symbols: list[str], stream_seconds: int = 0) -> None:
    symbols = [symbol.upper() for symbol in symbols]
    primary = symbols[0]
    ticker = yf.Ticker(primary)
    tickers = yf.Tickers(" ".join(symbols))
    print(f"yfinance snapshot | {datetime.now().astimezone():%Y-%m-%d %H:%M:%S %Z}")
    print(f"Symbols: {', '.join(symbols)} | Library version: {getattr(yf, '__version__', 'unknown')}")
    print("Values are Yahoo Finance data; blanks and unsupported sections are normal.")

    section(
        "1. Historical prices and corporate actions",
        "Ticker.history returns a pandas DataFrame (OHLCV). It can include dividends, splits, and capital gains.",
        lambda: ticker.history(period="1mo", interval="1d", actions=True),
    )
    section(
        "2. Intraday prices and history metadata",
        "Short-interval bars show time-indexed prices; metadata describes the exchange/timezone and quote range.",
        lambda: ticker.history(period="5d", interval="1h"),
    )
    section("History metadata", "Exchange, currency, timezone, and available price-range details.", ticker.get_history_metadata)
    section("Fast quote fields", "A lightweight mapping of common quote values such as last price and market cap.", lambda: dict(ticker.fast_info))
    section(
        "3. Download several tickers at once",
        "yf.download combines price history into a DataFrame with ticker/field columns.",
        lambda: yf.download(symbols, period="5d", interval="1d", group_by="ticker", progress=False),
    )
    section(
        "Multiple Ticker objects",
        "yf.Tickers provides a mapping of per-symbol Ticker objects for requests that need ticker-specific fields.",
        lambda: {symbol: dict(tickers.tickers[symbol].fast_info) for symbol in symbols},
    )

    section("4. Company profile and key statistics", "Ticker.info is a dictionary; Yahoo may leave many fields out.", lambda: ticker.info)
    section("Company identifiers and shares", "ISIN and historical shares outstanding, when Yahoo provides them.", lambda: fields(
        isin=lambda: ticker.isin,
        shares_full_recent=lambda: ticker.get_shares_full(start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")),
    ))
    section("5. Financial statements", "Annual and quarterly income statement, balance sheet, and cash flow tables.", lambda: fields(
        income_statement=lambda: ticker.income_stmt,
        quarterly_income_statement=lambda: ticker.quarterly_income_stmt,
        balance_sheet=lambda: ticker.balance_sheet,
        quarterly_balance_sheet=lambda: ticker.quarterly_balance_sheet,
        cash_flow=lambda: ticker.cashflow,
        quarterly_cash_flow=lambda: ticker.quarterly_cashflow,
        trailing_12_month_income=lambda: ticker.ttm_income_stmt,
        trailing_12_month_cash_flow=lambda: ticker.ttm_cashflow,
    ))
    section("6. Dividends, splits, and fund capital gains", "Corporate-action time series; capital gains are typically relevant to funds.", lambda: fields(
        dividends=lambda: ticker.dividends,
        splits=lambda: ticker.splits,
        actions=lambda: ticker.actions,
        capital_gains=lambda: ticker.capital_gains,
    ))
    section("7. Calendar, earnings, and SEC filings", "Upcoming calendar events, earnings history/dates, and available SEC filing links.", lambda: fields(
        calendar=lambda: ticker.calendar,
        earnings_dates=lambda: ticker.get_earnings_dates(limit=4),
        earnings_history=lambda: ticker.earnings_history,
        SEC_filings=lambda: ticker.get_sec_filings(),
    ))
    section("8. Analyst estimates and opinions", "Recommendations, price targets, revisions, and consensus estimate tables.", lambda: fields(
        recommendation_summary=lambda: ticker.recommendations_summary,
        recommendations=lambda: ticker.recommendations,
        upgrades_downgrades=lambda: ticker.upgrades_downgrades,
        analyst_price_targets=lambda: ticker.analyst_price_targets,
        earnings_estimate=lambda: ticker.earnings_estimate,
        revenue_estimate=lambda: ticker.revenue_estimate,
        EPS_trend=lambda: ticker.eps_trend,
        EPS_revisions=lambda: ticker.eps_revisions,
        growth_estimates=lambda: ticker.growth_estimates,
    ))
    section("9. Ownership and sustainability", "Insider activity, institutional/fund ownership, major holders, and ESG data when available.", lambda: fields(
        insider_purchases=lambda: ticker.insider_purchases,
        insider_transactions=lambda: ticker.insider_transactions,
        insider_roster=lambda: ticker.insider_roster_holders,
        major_holders=lambda: ticker.major_holders,
        institutional_holders=lambda: ticker.institutional_holders,
        mutual_fund_holders=lambda: ticker.mutualfund_holders,
        sustainability=lambda: ticker.sustainability,
    ))
    section("10. News", "Recent ticker-specific headlines and publisher metadata.", lambda: ticker.news)

    def option_preview() -> Any:
        expiries = ticker.options
        if not expiries:
            return {"expirations": expiries, "option_chain": "No listed expiration dates returned."}
        chain = ticker.option_chain(expiries[0])
        return {"expirations": expiries, "first_expiration": expiries[0], "calls": chain.calls, "puts": chain.puts}

    section("11. Options chains", "Available expiration dates and sample calls/puts with strikes, volume, and implied volatility.", option_preview)

    # FundsData exists for fund tickers such as SPY; print representative fields.
    fund_symbol = next((symbol for symbol in symbols if symbol.upper() in {"SPY", "QQQ", "VTI", "VOO"}), "SPY")
    def fund_preview() -> Any:
        funds = yf.Ticker(fund_symbol).funds_data
        return {
            "symbol": fund_symbol,
            "description": funds.description,
            "top_holdings": funds.top_holdings,
            "equity_holdings": funds.equity_holdings,
            "bond_holdings": funds.bond_holdings,
            "sector_weightings": funds.sector_weightings,
        }
    section("12. ETF and mutual fund data", "Fund description, holdings, and asset/sector allocations (example fund: SPY or a supplied common ETF).", fund_preview)

    def sector_preview() -> Any:
        info = ticker.info
        sector_key = info.get("sectorKey")
        industry_key = info.get("industryKey")
        result: dict[str, Any] = {}
        if sector_key:
            sector_obj = yf.Sector(sector_key)
            result["sector_overview"] = sector_obj.overview
            result["sector_top_companies"] = sector_obj.top_companies
            result["sector_industries"] = sector_obj.industries
            result["sector_top_ETFs"] = sector_obj.top_etfs
        if industry_key:
            industry_obj = yf.Industry(industry_key)
            result["industry_overview"] = industry_obj.overview
            result["industry_top_performers"] = industry_obj.top_performing_companies
            result["industry_top_growth"] = industry_obj.top_growth_companies
        return result or "Yahoo did not return sector/industry keys for this ticker."
    section("13. Sector and industry", "Sector overview, industry listings, ETFs, and representative companies, derived from the ticker's profile.", sector_preview)

    section("14. Search and lookup", "Search returns quotes/news/research; Lookup searches tickers across asset classes.", lambda: fields(
        search_quotes=lambda: yf.Search(primary, max_results=5, news_count=3).quotes,
        search_news=lambda: yf.Search(primary, max_results=3, news_count=3).news,
        lookup_all=lambda: yf.Lookup(primary).get_all(count=5),
    ))
    section("15. Market summary", "Market status and broad summary groups (Yahoo market keys include US, EUROPE, RATES, and CRYPTOCURRENCIES).", lambda: fields(
        US_status=lambda: yf.Market("US").status,
        US_summary=lambda: yf.Market("US").summary,
    ))
    section("16. Economic and corporate calendars", "Upcoming earnings, IPOs, stock splits, and economic events in calendar DataFrames.", lambda: fields(
        earnings=lambda: yf.Calendars().get_earnings_calendar(limit=5),
        IPOs=lambda: yf.Calendars().get_ipo_info_calendar(limit=5),
        splits=lambda: yf.Calendars().get_splits_calendar(limit=5),
        economic_events=lambda: yf.Calendars().get_economic_events_calendar(limit=5),
    ))
    section("17. Market screeners", "Predefined Yahoo screeners return matching quote rows; this example shows day gainers.", lambda: yf.screen("day_gainers", count=5))

    if stream_seconds > 0:
        section(
            "18. Live WebSocket stream",
            f"Streams live messages for up to {stream_seconds} seconds (interrupt with Ctrl-C). Availability depends on Yahoo's stream endpoint.",
            lambda: stream_quotes(symbols, stream_seconds),
        )
    else:
        print("\n18. Live streaming: available via yf.WebSocket / yf.AsyncWebSocket; opt in with --stream-seconds N.")

    print("\nSnapshot finished. Table previews are truncated to keep the console readable.")
    print("Tip: pass --help for options. Endpoint availability and fields vary by symbol and Yahoo response.")


def stream_quotes(symbols: list[str], seconds: int) -> str:
    """Listen to yfinance's synchronous stream briefly, then close the socket."""
    messages: list[dict[str, Any]] = []
    ws = yf.WebSocket()
    timer = threading.Timer(seconds, ws.close)
    try:
        ws.subscribe(symbols)
        timer.start()
        ws.listen(lambda message: (messages.append(message), show(message, max_items=8)))
    except KeyboardInterrupt:
        print("  Stream stopped by user.")
    finally:
        timer.cancel()
        try:
            ws.close()
        except Exception:
            pass
    return f"Received {len(messages)} live message(s)."


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("symbols", nargs="*", default=["AAPL", "MSFT", "SPY"], help="Yahoo Finance symbols (default: AAPL MSFT SPY)")
    parser.add_argument("--stream-seconds", type=int, default=0, metavar="N", help="also sample the live WebSocket for N seconds")
    args = parser.parse_args()
    if args.stream_seconds < 0:
        parser.error("--stream-seconds must be zero or greater")
    run_snapshot(args.symbols, args.stream_seconds)


if __name__ == "__main__":
    main()
