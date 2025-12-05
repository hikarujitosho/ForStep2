# リレーション定義書 - ERP（受発注）システム

## 概要
本書は、Bronze層のERP（受発注）システムの各テーブル間のリレーション（関連）を定義します。
各リレーションは、親テーブル → 子テーブルの参照関係を示し、データの整合性維持とクエリの結合条件を明確にします。

---

## リレーション一覧

### リレーション1: sales_order_header → location_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_HEADER_to_LOCATION |
| **親テーブル** | location_master |
| **子テーブル** | sales_order_header |
| **親テーブルキー** | location_id (PK) |
| **子テーブルキー** | location_id |
| **カーディナリティ** | 1:N（1つの拠点に複数の受注が存在可能） |
| **結合条件** | sales_order_header.location_id = location_master.location_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | location_masterに存在する location_id のみが受注伝票に記録される |
| **説明** | 受注を処理した拠点の識別。location_masterから拠点の詳細情報（拠点名、住所、営業所等）を参照する。 |

---

### リレーション2: sales_order_header → partner_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_HEADER_to_PARTNER |
| **親テーブル** | partner_master |
| **子テーブル** | sales_order_header |
| **親テーブルキー** | partner_id (PK) |
| **子テーブルキー** | customer_id |
| **カーディナリティ** | 1:N（1つの取引先（ディーラー）に複数の受注が存在可能） |
| **結合条件** | sales_order_header.customer_id = partner_master.partner_id |
| **フィルタ条件** | partner_master.partner_type = 'dealer' |
| **制約タイプ** | Foreign Key (FK) with Domain Constraint |
| **参照整合性ルール** | partner_type が 'dealer' である取引先の partner_id のみが受注伝票に記録される |
| **説明** | 受注元の顧客（販売先・ディーラー）の識別。partner_masterから取引先の詳細情報（名称、住所、担当者、支払条件等）を参照する。partner_type が 'dealer' 以外の場合は受注対象にならない。 |

---

### リレーション3: sales_order_item → sales_order_header

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_ITEM_to_ORDER_HEADER |
| **親テーブル** | sales_order_header |
| **子テーブル** | sales_order_item |
| **親テーブルキー** | order_id (PK) |
| **子テーブルキー** | order_id (FK) |
| **カーディナリティ** | 1:N（1つの受注に複数の明細行が存在） |
| **複合主キー** | sales_order_item (order_id, line_number) |
| **結合条件** | sales_order_item.order_id = sales_order_header.order_id |
| **制約タイプ** | Foreign Key (FK) - Composite Key |
| **参照整合性ルール** | sales_order_header に存在する order_id のみが明細行に記録される |
| **説明** | 受注伝票のヘッダーと明細行の関連付け。1つの受注に複数の商品が記載される。 |

---

### リレーション4: sales_order_item → product_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_ITEM_to_PRODUCT |
| **親テーブル** | product_master |
| **子テーブル** | sales_order_item |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N（1つの品目に複数の受注明細が存在） |
| **結合条件** | sales_order_item.product_id = product_master.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | product_masterに存在する product_id のみが受注明細に記録される |
| **説明** | 受注明細の商品情報を参照。product_masterから商品の詳細情報（品目名、単位、分類等）を取得する。 |

---

### リレーション5: sales_order_item → pricing_conditions（価格参照）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_ITEM_to_PRICING |
| **親テーブル** | pricing_conditions |
| **子テーブル** | sales_order_item |
| **親テーブルキー** | (product_id, customer_id, valid_from) - Composite Key |
| **子テーブルキー** | (product_id, customer_id [from header], pricing_date) |
| **カーディナリティ** | N:1（複数の受注明細が同一の価格条件を参照可能） |
| **結合条件（3条件AND）** | sales_order_item.product_id = pricing_conditions.product_id AND sales_order_header.customer_id = pricing_conditions.customer_id AND pricing_conditions.valid_from <= sales_order_item.pricing_date AND sales_order_item.pricing_date <= pricing_conditions.valid_to |
| **時間条件** | 両方閉じ: valid_from ≤ pricing_date ≤ valid_to |
| **制約タイプ** | Foreign Key (FK) - Composite Key with Temporal Condition |
| **参照整合性ルール** | 受注時点で有効な価格条件が存在する必要あり。同じ product_id, customer_id 組み合わせで時間的に重複する価格条件は存在しない |
| **説明** | 受注明細の価格参照。customer_id は sales_order_header を経由して取得。pricing_date は注文日に基づいて自動設定される。複数の時系列的な価格条件から最適な有効期間のレコードを参照する。 |

---

### リレーション6: pricing_conditions → product_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PRICING_to_PRODUCT |
| **親テーブル** | product_master |
| **子テーブル** | pricing_conditions |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N（1つの品目に複数の価格条件が存在） |
| **結合条件** | pricing_conditions.product_id = product_master.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | product_masterに存在する product_id のみが価格条件に記録される |
| **説明** | 価格条件で参照する商品情報。product_masterから商品の詳細情報を取得する。 |

---

### リレーション7: pricing_conditions → partner_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PRICING_to_PARTNER |
| **親テーブル** | partner_master |
| **子テーブル** | pricing_conditions |
| **親テーブルキー** | partner_id (PK) |
| **子テーブルキー** | customer_id (FK) |
| **カーディナリティ** | 1:N（1つの取引先に複数の価格条件が存在） |
| **結合条件** | pricing_conditions.customer_id = partner_master.partner_id |
| **フィルタ条件** | customer_id = NULL または customer_id = 'DEFAULT' の場合、partner_masterを参照しない（標準価格） |
| **制約タイプ** | Foreign Key (FK) - Optional Reference |
| **参照整合性ルール** | customer_id が NULL/'DEFAULT' でない場合、partner_masterに存在する partner_id のみが記録される |
| **説明** | ディーラー別の特別価格条件を参照。customer_id が NULL または 'DEFAULT' の場合は標準価格（すべてのディーラー共通）を示す。 |

---

### リレーション8: product_master → partner_master（import_export_group による制約）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PRODUCT_to_PARTNER_by_REGION |
| **親テーブル** | product_master, partner_master |
| **子テーブル** | sales_order_item |
| **親テーブルキー** | product_id (product_master), partner_id (partner_master) |
| **制約キー** | import_export_group (product_master) vs region (partner_master) |
| **カーディナリティ** | M:N（論理的な制約） |
| **結合条件** | product_master.import_export_group = '国内市場向け' ⇒ partner_master.region = 'domestic' の取引先のみが対象 product_master.import_export_group = '海外市場向け' ⇒ partner_master.region = 'overseas' の取引先のみが対象 |
| **制約タイプ** | Business Logic Constraint（データベースレベルではなくアプリケーションロジックで実装） |
| **参照整合性ルール** | sales_order_item で記録される product_id の import_export_group が、partner_masterの region と合致する必要あり |
| **説明** | 商品の国内/海外向けフラグと取引先の地域区分の整合性を確保。国内向け商品は国内ディーラーのみ、海外向け商品は海外ディーラーのみが取り扱う。 |

---

### リレーション9: bom_master → product_master（親品目）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_PRODUCT_PARENT |
| **親テーブル** | product_master |
| **子テーブル** | bom_master |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N（1つの親品目に複数のBOMレコードが存在） |
| **結合条件** | bom_master.product_id = product_master.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | product_masterに存在する product_id のみが BOM の親品目として記録される |
| **説明** | BOM の親品目（完成品または組立品）の参照。product_masterから親品目の詳細情報を取得。 |

---

### リレーション10: bom_master → product_master（構成部品）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_PRODUCT_COMPONENT |
| **親テーブル** | product_master |
| **子テーブル** | bom_master |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | component_product_id (FK) |
| **カーディナリティ** | 1:N（1つの構成部品が複数のBOMで使用される可能性） |
| **結合条件** | bom_master.component_product_id = product_master.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | product_masterに存在する product_id のみが BOM の構成部品として記録される |
| **説明** | BOM の構成部品（子品目）の参照。product_masterから部品の詳細情報を取得。 |

---

### リレーション11: bom_master → location_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_LOCATION |
| **親テーブル** | location_master |
| **子テーブル** | bom_master |
| **親テーブルキー** | location_id (PK) |
| **子テーブルキー** | site_id (FK) |
| **カーディナリティ** | 1:N（1つの拠点に複数のBOMが存在） |
| **結合条件** | bom_master.site_id = location_master.location_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | location_masterに存在する location_id のみが BOM の生産拠点として記録される |
| **説明** | BOM が適用される生産拠点の参照。拠点ごとに異なるBOM構成を持つ場合に使用。 |

---

## ER図（テキスト表記）

```
┌─────────────────────┐
│  location_master    │
│  (location_id: PK)  │
└──────────┬──────────┘
           │ 1:N
           │ (location_id)
           ↓
┌─────────────────────────────────────┐
│  sales_order_header                 │
│  (order_id: PK)                     │
│  (location_id: FK)                  │
│  (customer_id: FK → partner_id)     │
└──────────┬──────────────────────────┘
           │ 1:N
           │ (order_id)
           ↓
┌─────────────────────────────────────┐
│  sales_order_item                   │
│  (order_id, line_number: PK)        │
│  (order_id: FK)                     │
│  (product_id: FK)                   │
│  (pricing_date + product_id +       │
│   customer_id: FK → pricing_cond.)  │
└──────────┬──────────────────────────┘
           │ N:1
           │ (product_id)
           ↓
┌─────────────────────┐
│  product_master     │
│  (product_id: PK)   │
└─────┬───────────────┘
      │ 1:N
      │ (product_id)
      ↓
┌────────────────────────────────┐
│  pricing_conditions            │
│ (price_condition_id: PK)       │
│ (product_id: FK)               │
│ (customer_id: FK → partner_id) │
│ (valid_from, valid_to)         │
└────────────┬───────────────────┘
             │ N:1
             │ (customer_id)
             ↓
┌──────────────────────┐
│  partner_master      │
│  (partner_id: PK)    │
│  (region: binary)    │
└──────────────────────┘

┌─────────────────────┐
│  bom_master         │
│  (bom_id: PK)       │
│  (product_id: FK)   │
│  (component_product_│
│   _id: FK)          │
│  (site_id: FK)      │
└──────────┬──────────┘
           ├──→ product_master (product_id)
           ├──→ product_master (component_product_id)
           └──→ location_master (location_id)
```

---

## 参照整合性チェック項目

| チェック項目 | 説明 |
| ------ | ------ |
| **孤立レコード** | 親テーブルに存在しない外部キー値を持つ子レコード |
| **時間的整合性** | pricing_conditionsの有効期間が重複していないか |
| **ドメイン制約** | partner_masterのpartner_type が 'dealer' か確認 |
| **地域制約** | product_masterの import_export_group と partner_masterの region の対応を確認 |
| **NULL値の扱い** | pricing_conditionsの customer_id が NULL/'DEFAULT' の場合は標準価格 |

