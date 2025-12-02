# テーブル定義書
---
#### 月次商品別粗利率テーブル
**概要** : 各完成品（製品）について月次単位で売上と粗利を集計し、完成品ごとの粗利率を保持するゴールド層の集計テーブルです。販売分析や商品別採算管理、製品ポートフォリオ評価、価格改定の判断材料として利用します。テーブルは時系列の参照（年月）と製品識別子をキーに、再計算やトレーサビリティのために売上金額も併せて保存します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 製品を一意に識別するコード。外部の品目マスターの主キーまたは業務上の製品コードを格納する |
| product_name | VARCHA‌R |  | レポート用途のための製品表示名 |
| gross_margin_rate | DECIMAL |  | 完成品ごとの粗利率（profit margin ratio）。計算式は通常 (売上 - 売上原価) / 売上 |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨） |
| revenue | DECIMAL |  | 上金額（通貨単位: JPY を想定） |

#### 月次EV販売率テーブル
**概要** : 月次単位で全体の売上とEV（電気自動車）カテゴリの売上を集計し、EVの販売構成比（販売率）を算出・保存するゴールド層の集計テーブルです。販売チャネルやカテゴリ別分析のための上位集計ソースとして利用します。EV販売率は将来の製品戦略・電動化の進捗を可視化する主要KPIです。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| total_revenue | DECIMAL |  | 全体売上金額（通貨単位: JPY を想定）。当月のすべての車両カテゴリ合計の売上。 |
| ev_revenue | DECIMAL |  | EVカテゴリに該当する売上金額（当月）。カテゴリ判定ロジックは ETL の仕様で明示すること。 |
| ev_sales_share | DECIMAL |  | EV販売率（比率: 0〜1 を想定）。計算式: ev_revenue / NULLIF(total_revenue, 0)。total_revenue=0 の場合は NULL を格納。保存時は小数6桁で丸め。 |

#### 月次エリア別EV販売率テーブル
**概要** : 月次単位で拠点別（地域別）にEV販売率を集計・分析するゴールド層テーブルです。全社的なEV化進捗の地域差を可視化し、拠点ごとの販売戦略や投資優先度の判断に利用します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| total_revenue | DECIMAL |  | 拠点別の全体売上金額（通貨単位: JPY を想定）。当月の当該拠点におけるすべての車両カテゴリ合計の売上。 |
| ev_revenue | DECIMAL |  | 拠点別のEVカテゴリ売上金額（当月）。拠点内のEV車両販売に対応する売上。 |
| ev_sales_share | DECIMAL |  | 拠点別EV販売率（比率: 0〜1 を想定）。計算式: ev_revenue / NULLIF(total_revenue, 0)。分母がゼロの場合は NULL を格納。保存時は小数6桁で丸め。 |
| location_id | VARCHA‌R |  | 拠点を識別するコード。外部の拠点マスター（D_Plant等）への参照キーとして使用。例: 'A1', 'A2' など。 |

#### 月次先進安全装置適用率テーブル
**概要** : 月次単位で全体の売上と先進安全装置（ADAS等）を搭載した車両の売上を集計し、当装置の装備率を算出・保存するゴールド層の集計テーブルです。製品の安全性向上への取り組み進捗、市場での受け入れ状況を可視化する主要KPIです。規制対応や顧客ニーズの分析に利用されます。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| total_revenue | DECIMAL |  | 全体売上金額（通貨単位: JPY を想定）。当月のすべての車両合計の売上。 |
| safety_equipped_revenue | DECIMAL |  | 先進安全装置（ADAS等）を搭載した車両の売上金額（当月）。搭載判定ロジックは ETL の仕様で明示すること。 |
| safety_equipment_adoption_rate | DECIMAL |  | 先進安全装置適用率（比率: 0〜1 を想定）。計算式: safety_equipped_revenue / NULLIF(total_revenue, 0)。total_revenue=0 の場合は NULL を格納。保存時は小数6桁で丸め。 |

#### 月次エリア別先進安全装置適用率テーブル
**概要** : 月次単位で拠点別（地域別）に先進安全装置の装備率を集計・分析するゴールド層テーブルです。安全装備化の地域差を可視化し、拠点ごとのマーケティング施策や投資優先度の判断に利用します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| total_revenue | DECIMAL |  | 拠点別の全体売上金額（通貨単位: JPY を想定）。当月の当該拠点におけるすべての車両合計の売上。 |
| safety_equipped_revenue | DECIMAL |  | 拠点別の先進安全装置搭載車両売上金額（当月）。拠点内の安全装備車販売に対応する売上。 |
| safety_equipment_adoption_rate | DECIMAL |  | 拠点別先進安全装置適用率（比率: 0〜1 を想定）。計算式: safety_equipped_revenue / NULLIF(total_revenue, 0)。分母がゼロの場合は NULL を格納。保存時は小数6桁で丸め。 |
| location_id | VARCHA‌R |  | 拠点を識別するコード。外部の拠点マスター（D_Plant等）への参照キーとして使用。例: 'A1', 'A2' など。 |

#### 棚卸資産回転期間テーブル
**概要** : 月次単位で在庫資産の効率性を分析するゴールド層テーブルです。月あたりの売上原価と在庫金額から、在庫が平均的にどの程度の期間で販売・消費されるかを示す「棚卸資産回転期間」を算出・保存します。在庫最適化、キャッシュフロー改善、ROICの向上判断に利用される重要なサプライチェーン KPI です。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| monthly_cogs | DECIMAL |  | 当月の売上原価（Cost of Goods Sold）。日次値の月間合計。通貨単位: JPY を想定。 |
| inventory_value | DECIMAL |  | 月末時点の在庫金額（評価額）。通貨単位: JPY を想定。簿価または市価で評価。 |
| inventory_rotation_period | DECIMAL |  | 棚卸資産回転期間（単位: 日数）。計算式: (inventory_value / monthly_cogs) * 30 （月を30日で近似）。monthly_cogs=0 の場合は NULL を格納。在庫が平均的に何日で回転するかを示す。 |

#### 完成品別棚卸資産回転期間テーブル
**概要** : 月次単位で製品別（完成品別）に在庫回転期間を集計・分析するゴールド層テーブルです。製品ごとの在庫効率の差異を可視化し、製品別の在庫政策や生産計画の最適化、製品ミックス戦略の判断に利用します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 製品を一意に識別するコード。外部の品目マスター（D_Item_Master等）への参照キーとして使用。 |
| product_name | VARCHA‌R |  | 製品表示名（当月時点のスナップショット）。レポート表示用途。 |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| monthly_cogs | DECIMAL |  | 当該製品の当月売上原価（Cost of Goods Sold）。製品別の日次値の月間合計。通貨単位: JPY を想定。 |
| inventory_value | DECIMAL |  | 当該製品の月末時点の在庫金額（評価額）。通貨単位: JPY を想定。簿価または市価で評価。 |
| inventory_rotation_period | DECIMAL |  | 製品別棚卸資産回転期間（単位: 日数）。計算式: (inventory_value / monthly_cogs) * 30 。monthly_cogs=0 の場合は NULL を格納。当該製品の在庫が平均的に何日で回転するかを示す。 |

#### 月次EBITDAテーブル
**概要** : 月次単位で営業利益（EBITDA: Earnings Before Interest, Taxes, Depreciation and Amortization）を算出・保存するゴールド層テーブルです。売上、粗利、営業費を集計し、営業効率を分析する重要な財務指標です。事業採算性、キャッシュ生成能力の評価、経営層の意思決定支援に利用されます。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| revenue | DECIMAL |  | 全体売上金額（通貨単位: JPY を想定）。当月の全商品/カテゴリ合計の売上。 |
| gross_margin_amount | DECIMAL |  | 粗利額（通貨単位: JPY を想定）。計算式: revenue - cogs（売上原価）。 |
| operating_expenses | DECIMAL |  | 販売管理費およびその他営業費（通貨単位: JPY を想定）。給与、家賃、販売促進費等の営業管理諸費。 |
| ebitda | DECIMAL |  | EBITDA（営業利益、減価償却・償却前）。計算式: gross_margin_amount - operating_expenses。営業キャッシュフロー評価の基準。 |

#### 完成品別月次EBITDAテーブル
**概要** : 月次単位で製品別（完成品別）に営業利益（EBITDA）を集計・分析するゴールド層テーブルです。製品ごとの営業効率、採算性の差異を可視化し、製品別の価格設定、販売施策、投資優先度の判断に利用します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 製品を一意に識別するコード。外部の品目マスター（D_Item_Master等）への参照キーとして使用。 |
| product_name | VARCHA‌R |  | 製品表示名（当月時点のスナップショット）。レポート表示用途。 |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| revenue | DECIMAL |  | 当該製品の売上金額（通貨単位: JPY を想定）。当月の当該製品の売上合計。 |
| gross_margin_amount | DECIMAL |  | 当該製品の粗利額（通貨単位: JPY を想定）。計算式: revenue - cogs（製品別売上原価）。 |
| operating_expenses | DECIMAL |  | 当該製品に配賦された販売管理費およびその他営業費（通貨単位: JPY を想定）。配賦ロジックは ETL の仕様で明示すること。 |
| ebitda | DECIMAL |  | 製品別EBITDA。計算式: gross_margin_amount - operating_expenses。当該製品の営業キャッシュ生成能力を示す。 |

#### 完成品出荷リードタイム遵守率テーブル
**概要** : 月次単位で製品別の出荷リードタイム遵守状況を集計・分析するゴールド層テーブルです。受注から出荷までのリードタイム遵守率を可視化し、製品ごとの納期管理レベル、サプライチェーン効率、顧客満足度への影響を評価するための主要 KPI です。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 製品を一意に識別するコード。外部の品目マスター（D_Item_Master等）への参照キーとして使用。 |
| product_name | VARCHA‌R |  | 製品表示名（当月時点のスナップショット）。レポート表示用途。 |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨、例: '2025-08'）。時系列結合・グルーピング用のキー。 |
| orders_received | DECIMAL |  | 受注件数（単位: 件）。当該製品の当月受注の総件数。 |
| orders_shipped | DECIMAL |  | 出荷件数（単位: 件）。当該製品の当月出荷のうち、リードタイム内に出荷できた件数。 |
| on_time_delivery_rate | DECIMAL |  | 出荷リードタイム遵守率（比率: 0〜1 を想定）。計算式: orders_shipped / NULLIF(orders_received, 0)。orders_received=0 の場合は NULL を格納。保存時は小数6桁で丸め。1.0（100%）に近いほど納期遵守が良好。 |

#### 取引先別部品入荷リードタイム遵守率テーブル
**概要** : 取引先（サプライヤー）別に部品の入荷リードタイム遵守状況を月次で集計するゴールド層のテーブルです。サプライヤーごとの納期遵守性能を定量化し、調達戦略やサプライヤー評価、緊急対応の必要性の判定に利用します。入荷遅延の傾向分析やSLA監視にも使える主要KPIです。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| supplier_id | VARCHA‌R | FK | 取引先（サプライヤー）を識別するコード。外部のサプライヤーマスター（D_Supplier_Master等）への参照キーとして使用。 |
| supplier_name | VARCHA‌R |  | サプライヤーの名称（スナップショット）。表示・レポート用途。 |
| part_id | VARCHA‌R | PK | 部品を一意に識別するコード。ソースシステムの部品コード。 |
| part_name | VARCHA‌R |  | 部品名（スナップショット）。表示用途。 |
| product_id | VARCHA‌R |  | 関連する完成品の製品コード（存在する場合）。分析上の参照キー。 |
| product_name | VARCHA‌R |  | 関連完成品の名称（スナップショット）。 |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨）。時系列結合・グルーピング用のキー。 |
| purchase_orders_count | DECIMAL |  | 発注件数（単位: 件）。当該サプライヤー・部品・年月に対する発注の総件数。 |
| receipts_count | DECIMAL |  | 入荷件数（単位: 件）。当該期間に実際に受領した入荷の件数（リードタイム遵守とみなされたもの／あるいは総入荷件数の定義はETL仕様に記載）。 |
| inbound_lead_time_compliance_rate | DECIMAL |  | 入荷リードタイム遵守率（比率: 0〜1 を想定）。計算式: receipts_count / NULLIF(purchase_orders_count, 0)。purchase_orders_count=0 の場合は NULL を格納。保存時は小数6桁で丸め。 |

#### 緊急輸送費率テーブル
**概要** : 緊急輸送（ expedite / urgent shipping）にかかる費用比率を月次で把握するゴールド層のテーブルです。通常の輸送コストに対する緊急輸送コストの割合を把握することで、調達・生産計画のリスク評価や緊急対応コストの抑制施策の効果測定に利用します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| year_month | VARCHA‌R |  | 集計年月（フォーマット: YYYY-MM 推奨）。時系列結合・グルーピング用のキー。 |
| transportation_cost_total | DECIMAL |  | 当月の輸送費総額（通常輸送＋緊急輸送を含む）。通貨単位: JPY を想定。 |
| emergency_transportation_cost_total | DECIMAL |  | 当月に発生した緊急輸送費の合計（緊急対応分のみ）。通貨単位: JPY を想定。費目の定義は ETL 仕様で明示すること。 |
| emergency_transportation_cost_share | DECIMAL |  | 緊急輸送費率（比率: 0〜1 を想定）。計算式: emergency_transportation_cost_total / NULLIF(transportation_cost_total, 0)。transportation_cost_total=0 の場合は NULL を格納。保存時は小数6桁で丸め。 | 
