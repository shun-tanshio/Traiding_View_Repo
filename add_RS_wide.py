#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""add_RS_wide.py

RSスコア（ワイドCSV: 行=ticker, 列=日付）の「差分（ΔRS）」をワイドCSVとして保存します。

■ あなたの要望対応
- 既存の差分ファイルに“上書き保存”する（同名に保存）
- 余計なフォルダを勝手に作らない
  - --out 未指定なら、--scores のあるフォルダに rs_diffs_wide.csv を保存

■ 入出力
- 入力（--scores）: rs_scores_wide.csv（get_RS_wide.py が作るRSスコア蓄積）
- 出力（--out）: rs_diffs_wide.csv（デフォルト）

■ 差分モード
- previous_valid（デフォルト）: 直前の「非NaN」スコアとの差
  - 毎日計算してなくても、前回計算日のスコアからの増減が出る
- adjacent: 列として隣の前日列との差

"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


def _read_wide_csv(path: Path) -> pd.DataFrame:
    """行=ticker, 列=日付（文字列） を想定して読む。"""
    df = pd.read_csv(path, index_col=0)
    # 列（＝日付）をdatetime変換できるものだけ残す
    cols_dt = pd.to_datetime(df.columns, errors="coerce")
    ok = ~cols_dt.isna()
    df = df.loc[:, ok].copy()
    df.columns = cols_dt[ok]
    # 日付昇順に
    df = df.reindex(sorted(df.columns), axis=1)
    # 数値化（非数値はNaN）
    df = df.apply(lambda c: pd.to_numeric(c, errors="coerce"))
    return df


def _write_wide_csv(df: pd.DataFrame, path: Path) -> None:
    # 保存時は列を YYYY-MM-DD に戻す（見やすさと互換性）
    out = df.copy()
    out.columns = [d.strftime("%Y-%m-%d") for d in out.columns]
    out.to_csv(path)


def compute_diffs(scores: pd.DataFrame, mode: str) -> pd.DataFrame:
    """scores: columns=DatetimeIndex, index=ticker"""
    if scores.empty:
        return scores.copy()

    if mode == "adjacent":
        diffs = scores.diff(axis=1)
    elif mode == "previous_valid":
        # 直前の非NaNを ffill で埋め、その1列前（shift）との差を取る
        prev_valid = scores.ffill(axis=1).shift(1, axis=1)
        diffs = scores - prev_valid
    else:
        raise ValueError("mode must be 'previous_valid' or 'adjacent'")

    # 小数点第1位
    diffs = diffs.round(1)
    return diffs


def merge_with_existing(new_diffs: pd.DataFrame, out_path: Path) -> pd.DataFrame:
    """既存の差分CSVがあれば、保持しつつ新しい計算結果で更新する。"""
    if not out_path.exists():
        return new_diffs

    old = _read_wide_csv(out_path)

    # 行・列をユニオン
    all_idx = old.index.union(new_diffs.index)
    all_cols = old.columns.union(new_diffs.columns)

    merged = pd.DataFrame(index=all_idx, columns=all_cols, dtype=float)
    merged.loc[old.index, old.columns] = old
    merged.loc[new_diffs.index, new_diffs.columns] = new_diffs

    merged = merged.reindex(sorted(merged.columns), axis=1)
    return merged


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RSスコア（ワイド）の差分CSVを作成/更新（上書き保存）")
    p.add_argument(
        "--scores",
        type=str,
        default="rs_scores_wide.csv",
        help="入力RSスコア（ワイドCSV）",
    )
    p.add_argument(
        "--out",
        type=str,
        default=None,
        help="出力差分CSV。未指定なら scores と同じフォルダに rs_diffs_wide.csv",
    )
    p.add_argument(
        "--mode",
        type=str,
        default="previous_valid",
        choices=["previous_valid", "adjacent"],
        help="差分の取り方",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    scores_path = Path(args.scores).expanduser().resolve()
    if not scores_path.exists():
        raise FileNotFoundError(f"scores not found: {scores_path}")

    # out未指定: scoresと同じフォルダに保存（勝手に別フォルダ作らない）
    if args.out is None:
        out_path = scores_path.parent / "rs_diffs_wide.csv"
    else:
        out_path = Path(args.out).expanduser().resolve()

    scores = _read_wide_csv(scores_path)
    diffs_new = compute_diffs(scores, mode=args.mode)

    # 既存があるなら保持しつつ更新し、同じファイルに“上書き保存”
    diffs_merged = merge_with_existing(diffs_new, out_path)
    _write_wide_csv(diffs_merged, out_path)

    print(f"saved(overwrite): {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
