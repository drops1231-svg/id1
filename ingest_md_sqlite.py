# python ingest_md_sqlite.py --db "D:\原本\いちじいい\てすとじょう\実データ\nikki-path.db" --table documents --path-col path --content-col md_content

# ingest_md_sqlite.py
import argparse, sqlite3, sys
from pathlib import Path

def q(identifier: str) -> str:
    # SQLite用のシンプルなクォート（" を "" にエスケープ）
    return '"' + identifier.replace('"', '""') + '"'

def main():
    ap = argparse.ArgumentParser(description="Markdown本文をDBの新列へ取り込み（SQLite）")
    ap.add_argument("--db", required=True, help="SQLiteファイル（例: my.db）")
    ap.add_argument("--table", required=True)
    ap.add_argument("--path-col", dest="path_col", default="path")
    ap.add_argument("--content-col", dest="content_col", default="md_content")
    ap.add_argument("--encoding", default="utf-8")
    ap.add_argument("--batch-size", type=int, default=500)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    # パフォーマンス（必要に応じて有効化してください）
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.execute("PRAGMA synchronous = NORMAL;")

    # 列がなければ追加
    cols = [c[1] for c in cur.execute(f'PRAGMA table_info({q(args.table)})')]
    if args.content_col not in cols:
        cur.execute(f'ALTER TABLE {q(args.table)} ADD COLUMN {q(args.content_col)} TEXT')

    # 未取り込みの行を取得
    select_sql = (
        f"SELECT id, {q(args.path_col)} "
        f"FROM {q(args.table)} "
        f"WHERE {q(args.content_col)} IS NULL "
        f"AND {q(args.path_col)} IS NOT NULL "
        f"AND {q(args.path_col)} <> ''"
    )
    cur.execute(select_sql)
    rows = cur.fetchall()

    processed = 0
    missing = 0

    # バッチ更新のためトランザクションを抑制しすぎないようにコミット分割
    update_sql = f'UPDATE {q(args.table)} SET {q(args.content_col)} = ? WHERE id = ?'

    for doc_id, p in rows:
        try:
            text = Path(p).read_text(encoding=args.encoding, errors="replace")
        except Exception:
            missing += 1
            continue

        cur.execute(update_sql, (text, doc_id))
        processed += 1
        if processed % args.batch_size == 0:
            con.commit()

    con.commit()
    con.close()

    print(f"取り込み完了: {processed} 行更新")
    if missing:
        print(f"警告: {missing} 件のパスが無効または読み取り不可でした")

if __name__ == "__main__":
    # Windows の標準出力で日本語が化ける場合の対策（任意）
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
