#!/usr/bin/env python3
"""Pi42 webhook grid bot - simple single symbol."""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import requests


PUBLIC_BASE_URL = "https://api.pi42.com"

# ============ ENV LOADING & CONFIG ============

def load_env():
    """Load .env file if exists."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for path in (".env", os.path.join(script_dir, ".env")):
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, val = line.split("=", 1)
                        key = key.strip()
                        val = val.strip().strip('"').strip("'")
                        if key and key not in os.environ:
                            os.environ[key] = val
            break

load_env()

# Global config
webhook_url = os.getenv("PI42_WEBHOOK_URL", "https://webhooks.pi42.com/9420b64e63e7494c")
webhook_uuid = os.getenv("PI42_WEBHOOK_UUID", "c2796a87be4300c32ea9a527c65f6233429da8ab1a2d1ea1373bc005ae1f5f39")
webhook_action = os.getenv("PI42_WEBHOOK_ACTION", "NEW_ORDER")
symbol = os.getenv("PI42_SYMBOL", "ETHINR").upper()
qty = float(os.getenv("PI42_QTY", "0.015"))
margin_asset = os.getenv("PI42_MARGIN_ASSET", "INR")
grid_start = float(os.getenv("PI42_GRID_START_PRICE", "0"))
grid_step_pct = float(os.getenv("PI42_GRID_STEP_PCT", "1.0"))
exit_pct = float(os.getenv("PI42_EXIT_PCT", "1.0"))
price_decimals = int(os.getenv("PI42_PRICE_DECIMALS", "0"))
poll_seconds = int(os.getenv("PI42_POLL_SECONDS", "15"))
run_mode = os.getenv("PI42_RUN_MODE", "forever")
dry_run = os.getenv("PI42_DRY_RUN", "false").lower() == "true"
db_path = os.getenv("PI42_GRID_DB", "/tmp/grid_bot.db")

SCHEMA = """
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


def utc_now():
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


# ============ DATABASE ============

class DB:
    """SQLite wrapper."""
    
    def __init__(self, path):
        """Initialize DB and create tables."""
        db_dir = os.path.dirname(os.path.abspath(path))
        os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(path)
        
        # Migrate old schema (had 'id' column) to symbol-keyed schema
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(grid_state)").fetchall()}
        if "id" in cols:
            print("[db] migrating old schema")
            self.conn.execute("DROP TABLE grid_state")
        
        self.conn.executescript(SCHEMA)
    
    def close(self):
        """Close connection."""
        self.conn.close()
    
    def get_state(self, sym):
        """Get grid state for symbol."""
        r = self.conn.execute(
            "SELECT symbol, anchor_price, next_buy_price, last_buy_price, step_pct, levels_filled, updated_at "
            "FROM grid_state WHERE symbol = ?",
            (sym.upper(),)
        ).fetchone()
        if not r:
            return None
        return {
            "symbol": r[0],
            "anchor_price": float(r[1]),
            "next_buy_price": float(r[2]),
            "last_buy_price": float(r[3]) if r[3] else None,
            "step_pct": float(r[4]),
            "levels_filled": int(r[5]),
            "updated_at": r[6],
        }
    
    def init_state(self, sym, anchor, step):
        """Initialize grid state."""
        self.conn.execute(
            "INSERT OR REPLACE INTO grid_state (symbol, anchor_price, next_buy_price, last_buy_price, step_pct, levels_filled, updated_at) "
            "VALUES (?, ?, ?, NULL, ?, 0, ?)",
            (sym.upper(), anchor, anchor, step, utc_now())
        )
        self.conn.commit()
    
    def advance(self, sym, next_price):
        """Advance to next grid level after BUY."""
        self.conn.execute(
            "UPDATE grid_state SET next_buy_price = ?, levels_filled = levels_filled + 1, updated_at = ? "
            "WHERE symbol = ?",
            (next_price, utc_now(), sym.upper())
        )
        self.conn.commit()
    
    def set_last_buy(self, sym, price):
        """Set last buy price."""
        self.conn.execute(
            "UPDATE grid_state SET last_buy_price = ?, updated_at = ? WHERE symbol = ?",
            (price, utc_now(), sym.upper())
        )
        self.conn.commit()
    
    def clear_last_buy(self, sym):
        """Clear last buy price after EXIT."""
        self.conn.execute(
            "UPDATE grid_state SET last_buy_price = NULL, updated_at = ? WHERE symbol = ?",
            (utc_now(), sym.upper())
        )
        self.conn.commit()
    
    def save_order(self, sym, side, price, level, status, resp):
        """Save order to history."""
        self.conn.execute(
            "INSERT INTO grid_orders (created_at, symbol, side, order_type, quantity, price, level_index, status, response_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (utc_now(), sym.upper(), side.upper(), "MARKET", qty, price, level, status, json.dumps(resp))
        )
        self.conn.commit()


# ============ API CALLS ============

def fetch_price(sym):
    """Fetch live price from Pi42 klines API."""
    url = f"{PUBLIC_BASE_URL}/v1/market/klines?priceType=MARK_PRICE"
    r = requests.post(url, json={"pair": sym, "interval": "1m", "limit": 2}, timeout=15)
    r.raise_for_status()
    data = r.json()
    candles = data if isinstance(data, list) else data.get("data", [])
    return float(candles[-1].get("close") or candles[-1].get("c"))


def place_order(side):
    """Place MARKET order via webhook."""
    payload = {
        "side": side,
        "type": "MARKET",
        "uuid": webhook_uuid,
        "action": webhook_action,
        "symbol": symbol,
        "quantity": qty,
        "marginAsset": margin_asset,
    }
    r = requests.post(webhook_url, json=payload, timeout=20)
    r.raise_for_status()
    return r.json() if r.text else {"ok": True}


def round_price(p):
    """Round price to grid decimals."""
    return round(p, price_decimals)


# ============ MAIN LOOP ============

def run_once(db):
    """Run one grid check cycle."""
    live = fetch_price(symbol)
    state = db.get_state(symbol)
    
    # Initialize grid if needed
    if not state:
        anchor = live if grid_start == 0 else grid_start
        db.init_state(symbol, anchor, grid_step_pct)
        state = db.get_state(symbol)
    
    next_buy = float(state["next_buy_price"])
    last_buy = state.get("last_buy_price")
    levels = int(state["levels_filled"])
    
    print(f"live={live} next_buy={next_buy} levels={levels}")
    
    # EXIT: if we have a last_buy and price >= exit_target, SELL
    if last_buy is not None:
        exit_tgt = round_price(last_buy * (1 + exit_pct / 100.0))
        if live >= exit_tgt:
            if dry_run:
                db.save_order(symbol, "SELL", exit_tgt, levels, "DRY_RUN", {})
                print(f"DRY EXIT @ {exit_tgt}")
            else:
                try:
                    resp = place_order("SELL")
                    db.save_order(symbol, "SELL", exit_tgt, levels, "SUCCESS", resp)
                    db.clear_last_buy(symbol)
                    print(f"EXIT SUCCESS @ {exit_tgt}")
                except Exception as e:
                    db.save_order(symbol, "SELL", exit_tgt, levels, "FAILED", {"error": str(e)})
                    print(f"EXIT FAILED: {e}")
            return
    
    # BUY: if no exit and live <= next_buy, BUY
    if live <= next_buy:
        if dry_run:
            db.save_order(symbol, "BUY", next_buy, levels, "DRY_RUN", {})
            print(f"DRY BUY @ {next_buy}")
        else:
            try:
                resp = place_order("BUY")
                db.save_order(symbol, "BUY", next_buy, levels, "SUCCESS", resp)
                new_next = round_price(next_buy * (1 - grid_step_pct / 100.0))
                db.advance(symbol, new_next)
                db.set_last_buy(symbol, next_buy)
                print(f"BUY SUCCESS @ {next_buy}, next={new_next}")
            except Exception as e:
                db.save_order(symbol, "BUY", next_buy, levels, "FAILED", {"error": str(e)})
                print(f"BUY FAILED: {e}")
    else:
        print("no trigger")


def main():
    """Main entry point."""
    print(f"symbol={symbol} qty={qty} start={grid_start or 'live'} step={grid_step_pct}% "
          f"exit={exit_pct}% dry_run={dry_run} poll={poll_seconds}s db={db_path}")
    
    db = DB(db_path)
    try:
        if run_mode == "once":
            run_once(db)
        else:
            while True:
                try:
                    run_once(db)
                except Exception as e:
                    print(f"error: {e}")
                time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        db.close()


if __name__ == "__main__":
    main()
