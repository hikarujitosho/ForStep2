
# ダミーデータ可視化ダッシュボード（Streamlit）

PythonのStreamlitで、既存のCSV/Excelファイルを可視化するダッシュボードです。

## 1. セットアップ
```bash
# 仮想環境は任意
pip install -r requirements.txt
```

## 2. 起動
```bash
streamlit run app.py
```
ブラウザが開かない場合は http://localhost:8501 を開いてください。

## 3. 使い方
- 左のサイドバーから data/ に置いたCSV/Excelを選択、またはアップロードできます。
- カテゴリ列・数値列・日時列・集計方法を選ぶだけで自動でグラフ化されます。
- 表示された集計テーブルはCSVでダウンロード可能です。

## 4. よくある日本語環境の注意
- 文字化けする場合はCSVのエンコーディングをUTF-8（BOM付）にするか、Excel形式（.xlsx）に変換してください。
- 列名に「日付」「日時」を含む列は自動的に日付に変換を試行します。

## 5. GitHub へのプッシュ例
```bash
git init
git add .
git commit -m "feat: 初期Streamlitダッシュボード"
git branch -M main
git remote add origin https://github.com/<YOUR_ACCOUNT>/<YOUR_REPO>.git
git push -u origin main
```

## 6. Cursor / ChatGPT 活用のヒント
- Cursorで `app.py` を開き、`# ==========` で区切ったブロック単位に編集。
- ChatGPTに「この列の型推定を強化して」「Altair版の可視化を追加して」など具体的に依頼。
- 追加した要件は README に追記し、コミットメッセージも日本語でOKです。
