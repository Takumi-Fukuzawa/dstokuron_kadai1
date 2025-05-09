#Pythonコード
import csv
import os
import requests
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque

URL_BASE = "https://db.netkeiba.com/race/"
CSV_DIR = "./data/"
OUTPUT_FILE = f"{CSV_DIR}v25y0005_data.csv"

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
]

# リクエスト間隔を管理するためのdeque
request_timestamps = deque(maxlen=5) # 直近5回のタイムスタンプを保持（例）

def rate_limited_request():
    """リクエストレートを制限する（最低5秒間隔）。5回連続で短い間隔だった場合、長めに待機する可能性も考慮"""
    now = time.time()
    if len(request_timestamps) == request_timestamps.maxlen:
        time_since_oldest = now - request_timestamps[0]
        # 例えば、直近5回のリクエストが25秒以内に行われていたら少し長めに待つ
        if time_since_oldest < 25:
            wait_extra = random.uniform(7, 12)
            print(f"[INFO] Short interval detected. Waiting an extra {wait_extra:.1f} seconds...")
            time.sleep(wait_extra)

    if request_timestamps and now - request_timestamps[-1] < 5:
        wait_time = max(0, 5 - (now - request_timestamps[-1])) + random.uniform(0.5, 2) # 最低5秒 + α
        print(f"[INFO] Waiting {wait_time:.1f} seconds before next request...")
        time.sleep(wait_time)
    request_timestamps.append(time.time()) # 実際の時間はリクエスト送信直前に入れるのが理想だが、ここでは簡略化

def get_headers():
    """ランダムなUser-Agentを返す"""
    return {'User-Agent': random.choice(USER_AGENTS)}

def get_race_data(race_id):
    """指定されたrace_idのレースデータをnetkeiba.comから取得して構造化する"""
    url = URL_BASE + race_id
    try:
        rate_limited_request() # リクエスト前に待機チェック
        print(f"[INFO] Accessing: {url}")
        res = requests.get(url, headers=get_headers(), timeout=15) # タイムアウトを少し延長
        res.raise_for_status() # ステータスコードが200以外なら例外を発生させる
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.text, 'lxml')

        race_info_box = soup.find("div", class_="data_intro")
        if not race_info_box:
            print(f"[WARN] Race info box not found for race_id: {race_id}")
            return []

        # --- レース情報の抽出 ---
        race_name_tag = race_info_box.find("h1")
        race_name = race_name_tag.text.strip() if race_name_tag else "レース名不明"

        race_data_p = race_info_box.find("p")
        race_data_text = race_data_p.text.strip().replace('\xa0', ' ') if race_data_p else ""

        lines = race_data_text.split("\n")
        race_date_str = lines[0].split("：")[-1].strip() if len(lines) > 0 and "： " in lines[0] else ""
        try:
            race_date = datetime.strptime(race_date_str, "%Y年%m月%d日").strftime("%Y-%m-%d") if race_date_str else ""
        except ValueError:
            print(f"[WARN] Could not parse date: {race_date_str} in race {race_id}")
            race_date = ""

        info_line = lines[1].strip() if len(lines) > 1 else ""
        開催 = info_line.split(" ")[0] if info_line else ""
        クラス, 距離, 芝ダート, 回り, 馬場, 天気 = "", "", "", "", "", ""

        # "クラス"情報が含まれているかチェックして抽出 (例: '3歳未勝利', 'G1' など)
        # これはページ構造によって異なるため、より堅牢な方法が必要になる場合がある
        details_span = race_info_box.find("span")
        if details_span:
            details_text = details_span.text.strip().replace('\xa0', ' ')
            details_parts = details_text.split('/')
            # --- 詳細情報の解析 ---
            # この部分はサイトの構造変更に弱い可能性があるため注意
            for part in details_parts:
                part = part.strip()
                if "m" in part:
                    芝ダート = "芝" if "芝" in part else "ダ" if "ダ" in part else ""
                    距離 = ''.join(filter(str.isdigit, part)) # 数字のみ抽出
                    回り = "右" if "右" in part else "左" if "左" in part else ""
                elif "天候" in part:
                    天気 = part.split("：")[-1]
                elif "馬場" in part:
                    # 馬場状態が詳細情報に含まれる場合 (例: 芝：良)
                    if '芝' in part or 'ダ' in part:
                        馬場 = part.split("：")[-1]
                    # 馬場状態が独立している場合 (例: 良)
                    elif any(b in part for b in ["稍", "良", "不", "重"]):
                        馬場 = part
        # クラス情報は h1 タグの隣など、別の場所にある可能性もあるため、必要に応じて別途取得ロジックを追加

        place_id = race_id[4:6]
        place_name = 開催.split("回")[-1].split("日")[0] if 開催 else "" # 例: "1回東京1日" -> "東京"

        # --- レース結果テーブルの抽出 ---
        race_table = soup.find("table", class_="race_table_01 nk_tb_common")
        if race_table is None:
            print(f"[WARN] Race result table not found for race_id: {race_id}")
            return []

        rows = race_table.find_all("tr")
        if len(rows) < 2: # ヘッダー行 + データ行が最低1つないと処理できない
            print(f"[WARN] No data rows found in table for race_id: {race_id}")
            return []

        rows = rows[1:] # ヘッダー行を除外
        race_data = []

        for i, row in enumerate(rows):
            cols = row.find_all("td")
            # === 修正点: 列数チェックを強化 ===
            # 必須データ(着順～タイム、単勝、人気、馬体重)が存在するであろうインデックス14までチェック
            if len(cols) < 15:
                print(f"[WARN] Row {i+1} in race {race_id} has less than 15 columns ({len(cols)}), skipping.")
                continue
            try:
                # --- 各列データの抽出 ---
                着順 = cols[0].text.strip()
                馬番 = cols[2].text.strip()
                馬名 = cols[3].text.strip()
                性齢 = cols[4].text.strip()
                斤量 = cols[5].text.strip()
                騎手 = cols[6].text.strip()
                走破時間 = cols[7].text.strip()

                # === 修正点: 正しいインデックスを参照 ===
                通過順 = cols[10].text.strip() if len(cols) > 10 else ""
                上がり = cols[11].text.strip() if len(cols) > 11 else ""
                オッズ = cols[12].text.strip() if len(cols) > 12 else ""
                人気 = cols[13].text.strip() if len(cols) > 13 else ""
                馬体重_データ = cols[14].text.strip()

                # 性別と年齢を分割
                sex, age = "", ""
                if len(性齢) >= 2:
                    sex = 性齢[0]
                    age = 性齢[1:]

                # 体重と体重変化を分割
                weight, weight_diff = "", ""
                if 馬体重_データ and '(' in 馬体重_データ and ')' in 馬体重_データ:
                    parts = 馬体重_データ.replace(")", "").split("(")
                    if len(parts) == 2:
                        weight = parts[0]
                        weight_diff = parts[1]
                elif 馬体重_データ: # 体重のみの場合 (例: 計不)
                    weight = 馬体重_データ

                # 取得データを辞書に格納
                data = {
                    "race_id": race_id,
                    "馬": 馬名,
                    "騎手": 騎手,
                    "馬番": 馬番,
                    "走破時間": 走破時間,
                    "オッズ": オッズ,
                    "通過順": 通過順, # 修正済み
                    "着順": 着順,
                    "体重": weight, # 修正済み (元データ参照先変更)
                    "体重変化": weight_diff, # 修正済み (元データ参照先変更)
                    "性": sex,
                    "齢": age,
                    "斤量": 斤量,
                    "上がり": 上がり, # 修正済み
                    "人気": 人気,
                    # --- レース情報 ---
                    "レース名": race_name,
                    "日付": race_date,
                    "開催": 開催,
                    "クラス": クラス, # 注意: クラス情報の取得は改善が必要な場合あり
                    "芝・ダート": 芝ダート,
                    "距離": 距離,
                    "回り": 回り,
                    "馬場": 馬場,
                    "天気": 天気,
                    "場id": place_id,
                    "場名": place_name
                }
                race_data.append(data)
            except Exception as e:
                print(f"[WARN] Parsing error in row {i+1} for race {race_id}: {e}")
                # エラーが発生した行のcols内容を出力するとデバッグに役立つ
                # print(f"[DEBUG] Problematic row data: {[c.text.strip() for c in cols]}")
                continue

        return race_data

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed for race {race_id}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred while processing race {race_id}: {e}")
        return []

def clean_data(data):
    """辞書のリストを受け取り、文字列型の値に含まれるNBSPをスペースに置き換える + 通過順に'を追加 + 走破時間を秒単位に変換 + 芝・ダートと回りを数値に変換 + 性と天気を数値に変換"""
    cleaned = []
    for row in data:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, str):
                v = v.replace('\xa0', ' ')
                # 通過順には先頭に ' を追加（ただし重複防止）
                if k == "通過順" and not v.startswith("'") and v:
                    v = f"'{v}"
                # 走破時間を秒単位に変換
                elif k == "走破時間" and v:
                    try:
                        if ":" in v:  # "分:秒" 形式の場合
                            minutes, seconds = v.split(":")
                            total_seconds = float(minutes) * 60 + float(seconds)
                            v = f"{total_seconds:.1f}"
                    except ValueError:
                        print(f"[WARN] Could not convert race time: {v}")
                # 芝・ダートを数値に変換（芝=1, ダート=0）
                elif k == "芝・ダート":
                    if v == "芝":
                        v = "1"
                    elif v == "ダ":
                        v = "0"
                # 回りを数値に変換（右=1, 左=0）
                elif k == "回り":
                    if v == "右":
                        v = "1"
                    elif v == "左":
                        v = "0"
                # 性を数値に変換（牡=1, 牝=0, セ=1）
                elif k == "性":
                    if v == "牡" or v == "セ":
                        v = "1"
                    elif v == "牝":
                        v = "0"
                # 天気を数値に変換（晴=1, 曇=0, 雨=-1）
                elif k == "天気":
                    if "晴" in v:
                        v = "1"
                    elif "曇" in v:
                        v = "0"
                    elif "雨" in v:
                        v = "-1"
                new_row[k] = v
            else:
                new_row[k] = v
        cleaned.append(new_row)
    return cleaned

def append_to_csv(data, filepath):
    if not data:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file_exists = os.path.isfile(filepath)

    data_to_write = clean_data(data)

    fieldnames = [
        "race_id", "着順", "馬番", "馬", "性", "齢", "斤量", "騎手", "走破時間",
        "通過順", "上がり", "人気", "オッズ", "体重", "体重変化",
        "レース名", "日付", "開催", "クラス", "芝・ダート", "距離",
        "回り", "馬場", "天気", "場id", "場名"
    ]

    try:
        with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore', quoting=csv.QUOTE_ALL)
            if not file_exists:
                writer.writeheader()
            writer.writerows(data_to_write)

        # --- 追加: txtファイルへの追記 ---
        txt_filepath = filepath.replace(".csv", ".txt")
        with open(txt_filepath, "a", encoding="utf-8") as txtfile:
            for row in data_to_write:
                line = "\t".join([str(row.get(col, "")) for col in fieldnames])
                txtfile.write(line + "\n")

    except IOError as e:
        print(f"[ERROR] Failed to write to CSV file {filepath}: {e}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during CSV writing: {e}")

def main():
    """メイン処理: 指定された範囲のレースIDのデータを取得しCSVに保存する"""
    # 取得したいレースIDの範囲を指定
    # 例: 2024年の京都(08)、阪神(09)、東京(05) の 1回開催1日目 の全レース
    target_year = "2024"
    target_kaisai = "01" # 開催回数
    target_nichi = "01" # 開催日目
    # 場所ID (01=札幌, 02=函館, 05=東京, 06=中山, 07=中京, 08=京都, 09=阪神)
    target_places = ["01", "02", "05", "06", "07", "08", "09"]

    all_race_data = [] # すべてのレースデータを一旦メモリに貯める場合 (非推奨：メモリ使用量大)

    for place_id_str in target_places:
        for race_num in range(1, 13): # 1レースから12レースまで
            race_id = f"{target_year}{place_id_str}{target_kaisai}{target_nichi}{race_num:02d}"
            print(f"[INFO] Processing race_id: {race_id}")
            data = get_race_data(race_id)
            if data:
                append_to_csv(data, OUTPUT_FILE) # 取得ごとにCSVに追記
                # all_race_data.extend(data) # メモリに貯める場合

    # # メモリに貯めたデータを最後に一括で書き込む場合 (メモリ注意)
    # if all_race_data:
    #     append_to_csv(all_race_data, OUTPUT_FILE)

    print(f"[完了] データを {OUTPUT_FILE} に保存しました (または追記しました)。")

if __name__ == "__main__":
    main()