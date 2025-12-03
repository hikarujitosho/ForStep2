import pandas as pd

# 現在の販売額と原価額
current_data = {
    '車種': ['ACD-CV1', 'RDG-YF6', 'NOE-JG4', 'PRO-ZC5', 'HDE-ZC1', 'ENP-ENP1'],
    '販売額': [1596823081, 2617807837, 1872382681, 2839644865, 3259177186, 3835849843],
    '現在原価': [5751155002, 8260272033, 5679565558, 8117898379, 8018881110, 9284557699]
}

df = pd.DataFrame(current_data)

print("=" * 100)
print("原価調整計画")
print("=" * 100)

# 目標粗利率を設定（健全な車種を参考に）
# VZL-RV3: 59.57%, ODY-RC1: 45.81%, PLT-YF3: 42.11%, SWN-RP6: 33.37%, CRV-RT5: 31.96%
# 平均的には30-40%が適正と考えられる

print("\n【推奨される目標粗利率】")
print("  - 高級車（プレミアムセグメント）: 35-40%")
print("  - 中級車（スタンダードセグメント）: 30-35%")
print("  - 小型車（コンパクト/EV）: 25-30%")

# 各車種の目標粗利率を設定
target_margins = {
    'ACD-CV1': 35,  # プレミアムセダン
    'RDG-YF6': 35,  # 大型SUV
    'NOE-JG4': 25,  # コンパクトEV
    'PRO-ZC5': 35,  # プレミアムSUV
    'HDE-ZC1': 30,  # ハイブリッド
    'ENP-ENP1': 35  # 大型SUV
}

df['目標粗利率'] = df['車種'].map(target_margins)

# 目標原価を計算: 目標原価 = 販売額 × (1 - 目標粗利率)
df['目標原価'] = df['販売額'] * (1 - df['目標粗利率'] / 100)

# 調整率を計算
df['調整率'] = df['目標原価'] / df['現在原価']
df['削減額'] = df['現在原価'] - df['目標原価']
df['削減率'] = (1 - df['調整率']) * 100

print("\n" + "=" * 100)
print(f"{'車種':<12} {'販売額':>15} {'現在原価':>15} {'目標粗利率':>10} {'目標原価':>15} {'調整率':>10} {'削減率':>10}")
print("-" * 100)

for idx, row in df.iterrows():
    print(f"{row['車種']:<12} {row['販売額']:>15,.0f} {row['現在原価']:>15,.0f} "
          f"{row['目標粗利率']:>9.0f}% {row['目標原価']:>15,.0f} {row['調整率']:>9.4f} {row['削減率']:>9.2f}%")

print("=" * 100)

# 合計も表示
total_current_cost = df['現在原価'].sum()
total_target_cost = df['目標原価'].sum()
total_reduction = df['削減額'].sum()

print(f"\n合計削減額: {total_reduction:,.0f}円 ({(1 - total_target_cost/total_current_cost)*100:.2f}%削減)")

print("\n【次のステップ】")
print("この調整率を使用して、調達伝票_itemの各明細の単価を調整します。")
print("調整方法: 新単価 = 現在の単価 × 調整率")
print("\n例: ACD-CV1の部品単価を全て約{:.2f}%に調整（約{:.1f}%削減）".format(
    df[df['車種']=='ACD-CV1']['調整率'].values[0] * 100,
    df[df['車種']=='ACD-CV1']['削減率'].values[0]
))

# 結果を保存
output_path = 'output/原価調整計画.csv'
df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"\n調整計画を {output_path} に保存しました。")
