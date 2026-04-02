#!/usr/bin/env python3
"""Pi42 webhook grid bot -- multi-symbol, SQLite persistence.

Logic per symbol:
1) Fetch live price from public klines endpoint (no API key needed).
2) Descending BUY grid based on grid_step_pct.
3) live <= next_buy  -> place MARKET BUY, advance grid.
4) live >= last_buy * (1 + exit_pct) -> place MARKET SELL.
5) Each symbol runs in its own thread concurrently.

Single symbol  : PI42_SYMBOL=ETHINR
Multiple symbols: PI42_SYMBOLS=ETHINR,BTCINR,SOLINR

Per-symbol overrides (prefix PI42_<SYMBOL>_):
  PI42_BTCINR_QTY=0.001
  PI42_BTCINR_GRID_START_PRICE=8500000
  PI42_BTCINR_GRID_STEP_PCT=0.5
  PI42_BTCINR_EXIT_PCT=1.0
Falls back to global PI42_* default for any unset per-symbol var.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests


PUBLIC_BASE_URL = "https://api.pi42.com"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS grid_state (
    symbol TEXT PRIMARY KEY,
    anchor_price REAL NOT NULL,
    next_buy_price REAL NOT NULL,
    last_buy_price REAL,
    step_pct REAL NOT NULL,
    levels_filled INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS grid_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    level_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    response_json TEXT
);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_env_file(path: str = ".env") -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        path,
        os.path.join(script_dir, path),
    ]

    env_path = next((p for p in candidates if os.path.exists(p)), None)
    if not env_path:
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if not key:
                continue
            # If env var exists but is blank, replace it from .env.
            if key not in os.environ or os.environ.get(key, "").strip() == "":
                os.environ[key] = value


def _genv(key: str, default: str = "") -> str:
    return os.getenv(key, default)


@dataclass
class SymbolConfig:
    """Per-symbol trading parameters."""
    symbol: str
    quantity: float
    margin_asset: str
    grid_start_price: float   # 0 -> auto-use live price on first init
    grid_step_pct: float
    exit_pct: float
    price_decimals: int
    kline_interval: str
    kline_limit: int
    kline_price_type: str


@dataclass
class GlobalConfig:
    """Shared settings applied to all symbols."""
    webhook_url: str
    webhook_uuid: str
    webhook_action: str
    poll_seconds: int
    run_mode: str
    dry_run: bool
    db_path: str
    symbols: list

    def validate(self) -> None:
        if not self.webhook_url:
            raise ValueError("PI42_WEBHOOK_URL is required")
        if not self.webhook_uuid:
            raise ValueError("PI42_WEBHOOK_UUID is required")
        if self.run_mode not in {"once", "forever"}:
            raise ValueError("PI42_RUN_MODE must be once or forever")
        if not self.symbols:
            raise ValueError(
                "No symbols. Set PI42_SYMBOLS=ETHINR,BTCINR or PI42_SYMBOL=ETHINR"
            )


def build_global_config() -> GlobalConfig:
    symbols_raw = _genv("PI42_SYMBOLS") or _genv("PI42_SYMBOL", "ETHINR")
    symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]
    return GlobalConfig(
        webhook_url=_genv("PI42_WEBHOOK_URL", "https://webhooks.pi42.com/9420b64e63e7494c"),
        webhook_uuid=_genv(
            "PI42_WEBHOOK_UUID",
            "c2796a87be4300c32ea9a527c65f6233429da8ab1a2d1ea1373bc005ae1f5f39",
        ),
        webhook_action=_genv("PI42_WEBHOOK_ACTION", "NEW_ORDER"),
        poll_seconds=int(_genv("PI42_POLL_SECONDS", "15")),
        run_mode=_genv("PI42_RUN_MODE", "forever"),
        dry_run=_genv("PI42_DRY_RUN", "false").lower() == "true",
        db_path=_genv("PI42_GRID_DB", "/tmp/grid_bot.db"),
        symbols=symbols,
    )


def build_symbol_config(symbol: str) -> SymbolConfig:
    """Build per-symbol config, falling back to global PI42_* env vars."""
    s = symbol.upper()
    prefix = f"PI42_{s}_"

    def get(key: str, default: str) -> str:
        return os.getenv(f"{prefix}{key}", _genv(f"PI42_{key}", default))

    return SymbolConfig(
        symbol=s,
        quantity=float(get("QTY", "0.015")),
        margin_asset=get("MARGIN_ASSET", "INR"),
        grid_start_price=float(get("GRID_START_PRICE", "0")),
        grid_step_pct=float(get("GRID_STEP_PCT", "1.0")),
        exit_pct=float(get("EXIT_PCT", "1.0")),
        price_decimals=int(get("PRICE_DECIMALS", "0")),
        kline_interval=get("KLINE_INTERVAL", "1m"),
        kline_limit=int(get("KLINE_LIMIT", "2")),
        kline_price_type=get("KLINE_PRICE_TYPE", "MARK_PRICE"),
    )


class GridStore:
    """SQLite wrapper. Each bot gets its own connection (WAL multi-writer safe)."""

    def __init__(self, db_path: str) -> None:
        db_dir = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._setup_schema()
        self.conn.commit()

    def _setup_schema(self) -> None:
        # Migrate old single-row schema (had 'id' column) to symbol-keyed schema
        cols = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(grid_state)").fetchall()
        }
        if "id" in cols:
            print("[db] migrating old schema -> symbol-keyed schema")
            self.conn.execute("DROP TABLE grid_state")
        self.conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self.conn.close()

    def get_state(self, symbol: str) -> Optional[dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT symbol, anchor_price, next_buy_price, last_buy_price,
                   step_pct, levels_filled, updated_at
            FROM grid_state WHERE symbol = ?
            """,
            (symbol.upper(),),
        ).fetchone()
        if not row:
            return None
        return {
            "symbol": row[0],
            "anchor_price": float(row[1]),
            "next_buy_price": float(row[2]),
            "last_buy_price": float(row[3]) if row[3] is not None else None,
            "step_pct": float(row[4]),
            "levels_filled": int(row[5]),
            "updated_at": row[6],
        }

    def init_state(self, *, symbol: str, anchor_price: float, step_pct: float) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO grid_state
                (symbol, anchor_price, next_buy_price, last_buy_price,
                 step_pct, levels_filled, updated_at)
            VALUES (?, ?, ?, NULL, ?, 0, ?)
            """,
            (symbol.upper(), anchor_price, anchor_price, step_pct, utc_now_iso()),
        )
        self.conn.commit()

    def advance_level(self, symbol: str, next_buy_price: float) -> None:
        self.conn.execute(
            """
            UPDATE grid_state
            SET next_buy_price = ?, levels_filled = levels_filled + 1, updated_at = ?
            WHERE symbol = ?
            """,
            (next_buy_price, utc_now_iso(), symbol.upper()),
        )
        self.conn.commit()

    def set_last_buy_price(self, symbol: str, price: float) -> None:
        self.conn.execute(
            "UPDATE grid_state SET last_buy_price = ?, updated_at = ? WHERE symbol = ?",
            (price, utc_now_iso(), symbol.upper()),
        )
        self.conn.commit()

    def clear_last_buy_price(self, symbol: str) -> None:
        self.conn.execute(
            "UPDATE grid_state SET last_buy_price = NULL, updated_at = ? WHERE symbol = ?",
            (utc_now_iso(), symbol.upper()),
        )
        self.conn.commit()

    def save_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float,
        level_index: int,
        status: str,
        response: dict[str, Any],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO grid_orders (
                created_at, symbol, side, order_type, quantity,
                price, level_index, status, response_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now_iso(),
                symbol.upper(),
                side.upper(),
                order_type.upper(),
                quantity,
                price,
                level_index,
                status,
                json.dumps(response),
            ),
        )
        self.conn.commit()


class Pi42Client:
    def __init__(self, gcfg: GlobalConfig, scfg: SymbolConfig) -> None:
        self.gcfg = gcfg
        self.scfg = scfg
        self.session = requests.Session()

    def fetch_live_price(self) -> float:
        url = f"{PUBLIC_BASE_URL}/v1/market/klines?priceType={self.scfg.kline_price_type}"
        payload = {
            "pair": self.scfg.symbol,
            "interval": self.scfg.kline_interval,
            "limit": self.scfg.kline_limit,
        }
        resp = self.session.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        candles: list = data if isinstance(data, list) else data.get("data", [])
        if not candles:
            raise RuntimeError(f"No kline data for {self.scfg.symbol}")
        close_raw = candles[-1].get("close") or candles[-1].get("c")
        return float(close_raw)

    def place_market_order(self, side: str) -> dict[str, Any]:
        payload = {
            "side": side.upper(),
            "type": "MARKET",
            "uuid": self.gcfg.webhook_uuid,
            "action": self.gcfg.webhook_action,
            "symbol": self.scfg.symbol,
            "quantity": self.scfg.quantity,
            "marginAsset": self.scfg.margin_asset,
        }
        resp = self.session.post(self.gcfg.webhook_url, json=payload, timeout=20)
        resp.raise_for_status()
        if resp.text.strip():
            try:
                return resp.json()
            except ValueError:
                return {"raw": resp.text}
        return {"ok": True}


class GridBot:
    def __init__(self, gcfg: GlobalConfig, scfg: SymbolConfig, store: GridStore) -> None:
        self.gcfg = gcfg
        self.scfg = scfg
        self.store = store
        self.client = Pi42Client(gcfg, scfg)
        self.tag = f"[{scfg.symbol}]"

    def _round(self, price: float) -> float:
        return round(price, self.scfg.price_decimals)

    def _next_level(self, price: float) -> float:
        return self._round(price * (1 - self.scfg.grid_step_pct / 100.0))

    def _exit_target(self, last_buy: float) -> float:
        return self._round(last_buy * (1 + self.scfg.exit_pct / 100.0))

    def _ensure_state(self, live_price: float) -> dict[str, Any]:
        state = self.store.get_state(self.scfg.symbol)
        if state:
            return state
        anchor = self._round(
            self.scfg.grid_start_price if self.scfg.grid_start_price > 0 else live_price
        )
        print(f"{self.tag} initialising grid anchor={anchor}")
        self.store.init_state(
            symbol=self.scfg.symbol,
            anchor_price=anchor,
            step_pct=self.scfg.grid_step_pct,
        )
        return self.store.get_state(self.scfg.symbol)  # type: ignore[return-value]

    def _place(
        self,
        *,
        side: str,
        trigger_price: float,
        levels_filled: int,
        on_success: Callable,
        label: str,
    ) -> None:
        if self.gcfg.dry_run:
            self.store.save_order(
                symbol=self.scfg.symbol,
                side=side,
                order_type="MARKET",
                quantity=self.scfg.quantity,
                price=trigger_price,
                level_index=levels_filled,
                status="DRY_RUN",
                response={"dryRun": True, "price": trigger_price},
            )
            print(f"{self.tag} DRY_RUN {label} @ {trigger_price}")
            return

        try:
            response = self.client.place_market_order(side)
            self.store.save_order(
                symbol=self.scfg.symbol,
                side=side,
                order_type="MARKET",
                quantity=self.scfg.quantity,
                price=trigger_price,
                level_index=levels_filled,
                status="SUCCESS",
                response=response,
            )
            on_success()
            print(f"{self.tag} {label} SUCCESS @ {trigger_price}")
        except Exception as exc:  # noqa: BLE001
            self.store.save_order(
                symbol=self.scfg.symbol,
                side=side,
                order_type="MARKET",
                quantity=self.scfg.quantity,
                price=trigger_price,
                level_index=levels_filled,
                status="FAILED",
                response={"error": str(exc)},
            )
            print(f"{self.tag} {label} FAILED: {exc}")

    def run_once(self) -> None:
        live_price = self.client.fetch_live_price()
        state = self._ensure_state(live_price)
        next_buy = float(state["next_buy_price"])
        last_buy = state.get("last_buy_price")
        levels_filled = int(state["levels_filled"])

        print(f"{self.tag} live={live_price} next_buy={next_buy} levels={levels_filled}")

        # EXIT check
        if last_buy is not None:
            exit_tgt = self._exit_target(float(last_buy))
            print(f"{self.tag} last_buy={last_buy} exit_target={exit_tgt}")
            if live_price >= exit_tgt:
                self._place(
                    side="SELL",
                    trigger_price=exit_tgt,
                    levels_filled=levels_filled,
                    on_success=lambda: self.store.clear_last_buy_price(self.scfg.symbol),
                    label="EXIT SELL",
                )
                return

        # BUY check
        if live_price <= next_buy:
            def _on_buy() -> None:
                new_next = self._next_level(next_buy)
                self.store.advance_level(self.scfg.symbol, new_next)
                self.store.set_last_buy_price(self.scfg.symbol, next_buy)
                print(f"{self.tag} next_buy -> {new_next}")

            self._place(
                side="BUY",
                trigger_price=next_buy,
                levels_filled=levels_filled,
                on_success=_on_buy,
                label="BUY",
            )
        else:
            print(f"{self.tag} no trigger")

    def run(self) -> None:
        print(
            f"{self.tag} started | qty={self.scfg.quantity} "
            f"step={self.scfg.grid_step_pct}% exit={self.scfg.exit_pct}% "
            f"start={self.scfg.grid_start_price or 'live'} dry_run={self.gcfg.dry_run}"
        )
        if self.gcfg.run_mode == "once":
            self.run_once()
            return
        while True:
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                print(f"{self.tag} loop error: {exc}")
            time.sleep(self.gcfg.poll_seconds)


def main() -> int:
    load_env_file(".env")
    gcfg = build_global_config()

    try:
        gcfg.validate()
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        return 1

    print(
        f"[main] symbols={gcfg.symbols} dry_run={gcfg.dry_run} "
        f"poll={gcfg.poll_seconds}s db={gcfg.db_path}"
    )

    # Build one bot per symbol; each gets its own DB connection (WAL multi-writer safe)
    bots = []
    for sym in gcfg.symbols:
        store = GridStore(gcfg.db_path)
        scfg = build_symbol_config(sym)
        bots.append(GridBot(gcfg, scfg, store))

    # once-mode: run all symbols sequentially
    if gcfg.run_mode == "once":
        try:
            for bot in bots:
                bot.run_once()
        except KeyboardInterrupt:
            print("\n[main] stopped")
        finally:
            for bot in bots:
                bot.store.close()
        return 0

    # forever-mode: one daemon thread per symbol, all run in parallel
    threads = [
        threading.Thread(target=bot.run, name=bot.scfg.symbol, daemon=True)
        for bot in bots
    ]
    for t in threads:
        t.start()

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[main] stopped")
    finally:
        for bot in bots:
            bot.store.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
