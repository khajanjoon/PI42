#!/usr/bin/env python3
"""
Pi42 webhook grid bot with SQLite persistence.

Logic:
1) Fetch latest live price from public klines endpoint (no API key needed).
2) Keep a descending BUY grid based on `grid_step_pct`.
3) When live price <= next grid level, place webhook MARKET BUY.
4) When live price >= last_buy_price * (1 + exit_pct), place webhook MARKET SELL.
5) Save grid state and all order attempts to SQLite.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import requests


PUBLIC_BASE_URL = "https://api.pi42.com"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS grid_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    symbol TEXT NOT NULL,
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


@dataclass
class GridConfig:
    webhook_url: Optional[str] = os.getenv("PI42_WEBHOOK_URL", "https://webhooks.pi42.com/9420b64e63e7494c")
    webhook_uuid: Optional[str] = os.getenv("PI42_WEBHOOK_UUID", "c2796a87be4300c32ea9a527c65f6233429da8ab1a2d1ea1373bc005ae1f5f39")
    webhook_action: str = os.getenv("PI42_WEBHOOK_ACTION", "NEW_ORDER")

    symbol: str = os.getenv("PI42_SYMBOL", "ETHINR")
    quantity: float = float(os.getenv("PI42_QTY", "0.015"))
    margin_asset: str = os.getenv("PI42_MARGIN_ASSET", "INR")

    # First grid anchor and grid spacing
    grid_start_price: float = float(os.getenv("PI42_GRID_START_PRICE", "185010"))
    grid_step_pct: float = float(os.getenv("PI42_GRID_STEP_PCT", "1.0"))
    exit_pct: float = float(os.getenv("PI42_EXIT_PCT", "1.0"))
    price_decimals: int = int(os.getenv("PI42_PRICE_DECIMALS", "0"))

    kline_interval: str = os.getenv("PI42_KLINE_INTERVAL", "1m")
    kline_limit: int = int(os.getenv("PI42_KLINE_LIMIT", "2"))
    kline_price_type: str = os.getenv("PI42_KLINE_PRICE_TYPE", "MARK_PRICE")

    poll_seconds: int = int(os.getenv("PI42_POLL_SECONDS", "15"))
    run_mode: str = os.getenv("PI42_RUN_MODE", "forever")
    dry_run: bool = os.getenv("PI42_DRY_RUN", "false").lower() == "true"

    db_path: str = os.getenv("PI42_GRID_DB", "/tmp/grid_bot.db")

    def validate(self) -> None:
        if not self.webhook_url:
            raise ValueError("PI42_WEBHOOK_URL is required")
        if not self.webhook_uuid:
            raise ValueError("PI42_WEBHOOK_UUID is required")
        if self.quantity <= 0:
            raise ValueError("PI42_QTY must be > 0")
        if self.grid_start_price <= 0:
            raise ValueError("PI42_GRID_START_PRICE must be > 0")
        if self.grid_step_pct <= 0:
            raise ValueError("PI42_GRID_STEP_PCT must be > 0")
        if self.exit_pct <= 0:
            raise ValueError("PI42_EXIT_PCT must be > 0")
        if self.price_decimals < 0:
            raise ValueError("PI42_PRICE_DECIMALS must be >= 0")
        if self.run_mode not in {"once", "forever"}:
            raise ValueError("PI42_RUN_MODE must be once or forever")


class GridStore:
    def __init__(self, db_path: str) -> None:
        db_dir = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.executescript(SCHEMA_SQL)
        self._migrate_schema()
        self.conn.commit()

    def _migrate_schema(self) -> None:
        cols = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(grid_state)").fetchall()
        }
        if "last_buy_price" not in cols:
            self.conn.execute("ALTER TABLE grid_state ADD COLUMN last_buy_price REAL")

    def close(self) -> None:
        self.conn.close()

    def get_state(self) -> Optional[dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT symbol, anchor_price, next_buy_price, last_buy_price, step_pct, levels_filled, updated_at
            FROM grid_state
            WHERE id = 1
            """
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
            INSERT OR REPLACE INTO grid_state (
                id, symbol, anchor_price, next_buy_price, last_buy_price, step_pct, levels_filled, updated_at
            ) VALUES (1, ?, ?, ?, NULL, ?, 0, ?)
            """,
            (symbol.upper(), anchor_price, anchor_price, step_pct, utc_now_iso()),
        )
        self.conn.commit()

    def advance_level(self, next_buy_price: float) -> None:
        self.conn.execute(
            """
            UPDATE grid_state
            SET next_buy_price = ?, levels_filled = levels_filled + 1, updated_at = ?
            WHERE id = 1
            """,
            (next_buy_price, utc_now_iso()),
        )
        self.conn.commit()

    def set_last_buy_price(self, last_buy_price: float) -> None:
        self.conn.execute(
            """
            UPDATE grid_state
            SET last_buy_price = ?, updated_at = ?
            WHERE id = 1
            """,
            (last_buy_price, utc_now_iso()),
        )
        self.conn.commit()

    def clear_last_buy_price(self) -> None:
        self.conn.execute(
            """
            UPDATE grid_state
            SET last_buy_price = NULL, updated_at = ?
            WHERE id = 1
            """,
            (utc_now_iso(),),
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


class Pi42PublicWebhookClient:
    def __init__(self, cfg: GridConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()

    def fetch_live_price(self) -> float:
        endpoint = f"/v1/market/klines?priceType={self.cfg.kline_price_type}"
        payload = {
            "pair": self.cfg.symbol.upper(),
            "interval": self.cfg.kline_interval,
            "limit": self.cfg.kline_limit,
        }

        url = f"{PUBLIC_BASE_URL}{endpoint}"
        resp = self.session.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        candles: list[dict[str, Any]] = []
        if isinstance(data, list):
            candles = data
        elif isinstance(data, dict) and isinstance(data.get("data"), list):
            candles = data["data"]

        if not candles:
            raise RuntimeError("No kline data returned")

        close_raw = candles[-1].get("close") or candles[-1].get("c")
        return float(close_raw)

    def place_webhook_market_order(self, side: str) -> dict[str, Any]:
        payload = {
            "side": side.upper(),
            "type": "MARKET",
            "uuid": self.cfg.webhook_uuid,
            "action": self.cfg.webhook_action,
            "symbol": self.cfg.symbol.upper(),
            "quantity": self.cfg.quantity,
            "marginAsset": self.cfg.margin_asset,
        }

        resp = self.session.post(self.cfg.webhook_url, json=payload, timeout=20)
        resp.raise_for_status()
        if resp.text.strip():
            try:
                return resp.json()
            except ValueError:
                return {"raw": resp.text}
        return {"ok": True}


class GridBot:
    def __init__(self, cfg: GridConfig, store: GridStore, client: Pi42PublicWebhookClient) -> None:
        self.cfg = cfg
        self.store = store
        self.client = client

    def _round_price(self, price: float) -> float:
        return round(price, self.cfg.price_decimals)

    def _next_level_price(self, current_level_price: float) -> float:
        return self._round_price(current_level_price * (1 - self.cfg.grid_step_pct / 100.0))

    def _exit_price(self, last_buy_price: float) -> float:
        return self._round_price(last_buy_price * (1 + self.cfg.exit_pct / 100.0))

    def _ensure_state(self) -> dict[str, Any]:
        state = self.store.get_state()
        if state:
            return state

        anchor = self._round_price(self.cfg.grid_start_price)
        self.store.init_state(symbol=self.cfg.symbol, anchor_price=anchor, step_pct=self.cfg.grid_step_pct)
        return self.store.get_state() or {
            "symbol": self.cfg.symbol.upper(),
            "anchor_price": anchor,
            "next_buy_price": anchor,
            "step_pct": self.cfg.grid_step_pct,
            "last_buy_price": None,
            "levels_filled": 0,
            "updated_at": utc_now_iso(),
        }

    def run_once(self) -> None:
        state = self._ensure_state()
        next_buy = float(state["next_buy_price"])
        last_buy_price = state.get("last_buy_price")
        levels_filled = int(state["levels_filled"])

        live_price = self.client.fetch_live_price()
        print(
            f"[grid] live={live_price} next_buy={next_buy} "
            f"levels_filled={levels_filled}"
        )

        if last_buy_price is not None:
            exit_price = self._exit_price(float(last_buy_price))
            print(f"[grid] last_buy={last_buy_price} exit_target={exit_price}")
            if live_price >= exit_price:
                if self.cfg.dry_run:
                    response = {
                        "dryRun": True,
                        "message": "exit trigger hit",
                        "symbol": self.cfg.symbol.upper(),
                        "quantity": self.cfg.quantity,
                        "price": exit_price,
                    }
                    self.store.save_order(
                        symbol=self.cfg.symbol,
                        side="SELL",
                        order_type="MARKET",
                        quantity=self.cfg.quantity,
                        price=exit_price,
                        level_index=levels_filled,
                        status="DRY_RUN",
                        response=response,
                    )
                    print("[trade] DRY_RUN exit logged; state unchanged")
                    return

                try:
                    response = self.client.place_webhook_market_order("SELL")
                    self.store.save_order(
                        symbol=self.cfg.symbol,
                        side="SELL",
                        order_type="MARKET",
                        quantity=self.cfg.quantity,
                        price=exit_price,
                        level_index=levels_filled,
                        status="SUCCESS",
                        response=response,
                    )
                    self.store.clear_last_buy_price()
                    print("[trade] EXIT SUCCESS; cleared last_buy_price")
                    return
                except Exception as exc:  # noqa: BLE001
                    response = {"error": str(exc)}
                    self.store.save_order(
                        symbol=self.cfg.symbol,
                        side="SELL",
                        order_type="MARKET",
                        quantity=self.cfg.quantity,
                        price=exit_price,
                        level_index=levels_filled,
                        status="FAILED",
                        response=response,
                    )
                    print(f"[trade] EXIT FAILED saved: {exc}")
                    return

        if live_price > next_buy:
            print("[grid] no trigger yet")
            return

        if self.cfg.dry_run:
            response = {
                "dryRun": True,
                "message": "price trigger hit",
                "symbol": self.cfg.symbol.upper(),
                "quantity": self.cfg.quantity,
                "price": next_buy,
            }
            self.store.save_order(
                symbol=self.cfg.symbol,
                side="BUY",
                order_type="MARKET",
                quantity=self.cfg.quantity,
                price=next_buy,
                level_index=levels_filled,
                status="DRY_RUN",
                response=response,
            )
            print("[trade] DRY_RUN buy logged; state unchanged")
            return

        try:
            response = self.client.place_webhook_market_order("BUY")
            self.store.save_order(
                symbol=self.cfg.symbol,
                side="BUY",
                order_type="MARKET",
                quantity=self.cfg.quantity,
                price=next_buy,
                level_index=levels_filled,
                status="SUCCESS",
                response=response,
            )
            new_next = self._next_level_price(next_buy)
            self.store.advance_level(new_next)
            self.store.set_last_buy_price(next_buy)
            print(f"[trade] SUCCESS; advanced next_buy -> {new_next}")
        except Exception as exc:  # noqa: BLE001
            response = {"error": str(exc)}
            self.store.save_order(
                symbol=self.cfg.symbol,
                side="BUY",
                order_type="MARKET",
                quantity=self.cfg.quantity,
                price=next_buy,
                level_index=levels_filled,
                status="FAILED",
                response=response,
            )
            print(f"[trade] FAILED saved: {exc}")

    def run(self) -> None:
        print(
            "[config] "
            f"symbol={self.cfg.symbol.upper()} qty={self.cfg.quantity} "
            f"start={self.cfg.grid_start_price} step={self.cfg.grid_step_pct}% exit={self.cfg.exit_pct}% "
            f"poll={self.cfg.poll_seconds}s dry_run={self.cfg.dry_run} "
            f"db={self.cfg.db_path}"
        )

        if self.cfg.run_mode == "once":
            self.run_once()
            return

        while True:
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                print(f"[loop] error: {exc}")
            time.sleep(self.cfg.poll_seconds)


def main() -> int:
    load_env_file(".env")

    cfg = GridConfig()
    # Refresh config from environment after .env has been loaded.
    cfg.webhook_url = os.getenv("PI42_WEBHOOK_URL", cfg.webhook_url)
    cfg.webhook_uuid = os.getenv("PI42_WEBHOOK_UUID", cfg.webhook_uuid)
    cfg.webhook_action = os.getenv("PI42_WEBHOOK_ACTION", cfg.webhook_action)
    cfg.symbol = os.getenv("PI42_SYMBOL", cfg.symbol)
    cfg.quantity = float(os.getenv("PI42_QTY", str(cfg.quantity)))
    cfg.margin_asset = os.getenv("PI42_MARGIN_ASSET", cfg.margin_asset)
    cfg.grid_start_price = float(os.getenv("PI42_GRID_START_PRICE", str(cfg.grid_start_price)))
    cfg.grid_step_pct = float(os.getenv("PI42_GRID_STEP_PCT", str(cfg.grid_step_pct)))
    cfg.exit_pct = float(os.getenv("PI42_EXIT_PCT", str(cfg.exit_pct)))
    cfg.price_decimals = int(os.getenv("PI42_PRICE_DECIMALS", str(cfg.price_decimals)))
    cfg.kline_interval = os.getenv("PI42_KLINE_INTERVAL", cfg.kline_interval)
    cfg.kline_limit = int(os.getenv("PI42_KLINE_LIMIT", str(cfg.kline_limit)))
    cfg.kline_price_type = os.getenv("PI42_KLINE_PRICE_TYPE", cfg.kline_price_type)
    cfg.poll_seconds = int(os.getenv("PI42_POLL_SECONDS", str(cfg.poll_seconds)))
    cfg.run_mode = os.getenv("PI42_RUN_MODE", cfg.run_mode)
    cfg.dry_run = os.getenv("PI42_DRY_RUN", str(cfg.dry_run).lower()).lower() == "true"
    cfg.db_path = os.getenv("PI42_GRID_DB", cfg.db_path)

    try:
        cfg.validate()
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        return 1

    store = GridStore(cfg.db_path)
    client = Pi42PublicWebhookClient(cfg)
    bot = GridBot(cfg, store, client)

    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n[bot] stopped")
    finally:
        store.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
