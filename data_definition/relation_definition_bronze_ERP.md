# リレーション定義書 - ERP（受発注）システム

## 概要
本書は、Bronze層のERP（受発注）システムの各テーブル間のリレーション（関連）を定義します。
各リレーションは、親テーブル → 子テーブルの参照関係を示し、データの整合性維持とクエリの結合条件を明確にします。

---

## リレーション一覧

### リレーション1: 受注伝票_header → 拠点マスタ

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_HEADER_to_LOCATION |
| **親テーブル** | 拠点マスタ |
| **子テーブル** | 受注伝票_header |
| **親テーブルキー** | location_id (PK) |
| **子テーブルキー** | location_id |
| **カーディナリティ** | 1:N（1つの拠点に複数の受注が存在可能） |
| **結合条件** | 受注伝票_header.location_id = 拠点マスタ.location_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | 拠点マスタに存在する location_id のみが受注伝票に記録される |
| **説明** | 受注を処理した拠点の識別。拠点マスタから拠点の詳細情報（拠点名、住所、営業所等）を参照する。 |

---

### リレーション2: 受注伝票_header → 取引先マスタ

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_HEADER_to_PARTNER |
| **親テーブル** | 取引先マスタ |
| **子テーブル** | 受注伝票_header |
| **親テーブルキー** | partner_id (PK) |
| **子テーブルキー** | customer_id |
| **カーディナリティ** | 1:N（1つの取引先（ディーラー）に複数の受注が存在可能） |
| **結合条件** | 受注伝票_header.customer_id = 取引先マスタ.partner_id |
| **フィルタ条件** | 取引先マスタ.partner_type = 'dealer' |
| **制約タイプ** | Foreign Key (FK) with Domain Constraint |
| **参照整合性ルール** | partner_type が 'dealer' である取引先の partner_id のみが受注伝票に記録される |
| **説明** | 受注元の顧客（販売先・ディーラー）の識別。取引先マスタから取引先の詳細情報（名称、住所、担当者、支払条件等）を参照する。partner_type が 'dealer' 以外の場合は受注対象にならない。 |

---

### リレーション3: 受注伝票_item → 受注伝票_header

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_ITEM_to_ORDER_HEADER |
| **親テーブル** | 受注伝票_header |
| **子テーブル** | 受注伝票_item |
| **親テーブルキー** | order_id (PK) |
| **子テーブルキー** | order_id (FK) |
| **カーディナリティ** | 1:N（1つの受注に複数の明細行が存在） |
| **複合主キー** | 受注伝票_item (order_id, line_number) |
| **結合条件** | 受注伝票_item.order_id = 受注伝票_header.order_id |
| **制約タイプ** | Foreign Key (FK) - Composite Key |
| **参照整合性ルール** | 受注伝票_header に存在する order_id のみが明細行に記録される |
| **説明** | 受注伝票のヘッダーと明細行の関連付け。1つの受注に複数の商品が記載される。 |

---

### リレーション4: 受注伝票_item → 品目マスタ

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_ITEM_to_PRODUCT |
| **親テーブル** | 品目マスタ |
| **子テーブル** | 受注伝票_item |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N（1つの品目に複数の受注明細が存在） |
| **結合条件** | 受注伝票_item.product_id = 品目マスタ.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | 品目マスタに存在する product_id のみが受注明細に記録される |
| **説明** | 受注明細の商品情報を参照。品目マスタから商品の詳細情報（品目名、単位、分類等）を取得する。 |

---

### リレーション5: 受注伝票_item → 条件マスタ（価格参照）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | ORDER_ITEM_to_PRICING |
| **親テーブル** | 条件マスタ |
| **子テーブル** | 受注伝票_item |
| **親テーブルキー** | (product_id, customer_id, valid_from) - Composite Key |
| **子テーブルキー** | (product_id, customer_id [from header], pricing_date) |
| **カーディナリティ** | N:1（複数の受注明細が同一の価格条件を参照可能） |
| **結合条件（3条件AND）** | 受注伝票_item.product_id = 条件マスタ.product_id AND 受注伝票_header.customer_id = 条件マスタ.customer_id AND 条件マスタ.valid_from <= 受注伝票_item.pricing_date AND 受注伝票_item.pricing_date <= 条件マスタ.valid_to |
| **時間条件** | 両方閉じ: valid_from ≤ pricing_date ≤ valid_to |
| **制約タイプ** | Foreign Key (FK) - Composite Key with Temporal Condition |
| **参照整合性ルール** | 受注時点で有効な価格条件が存在する必要あり。同じ product_id, customer_id 組み合わせで時間的に重複する価格条件は存在しない |
| **説明** | 受注明細の価格参照。customer_id は受注伝票_header を経由して取得。pricing_date は注文日に基づいて自動設定される。複数の時系列的な価格条件から最適な有効期間のレコードを参照する。 |

---

### リレーション6: 条件マスタ → 品目マスタ

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PRICING_to_PRODUCT |
| **親テーブル** | 品目マスタ |
| **子テーブル** | 条件マスタ |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N（1つの品目に複数の価格条件が存在） |
| **結合条件** | 条件マスタ.product_id = 品目マスタ.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | 品目マスタに存在する product_id のみが価格条件に記録される |
| **説明** | 価格条件で参照する商品情報。品目マスタから商品の詳細情報を取得する。 |

---

### リレーション7: 条件マスタ → 取引先マスタ

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PRICING_to_PARTNER |
| **親テーブル** | 取引先マスタ |
| **子テーブル** | 条件マスタ |
| **親テーブルキー** | partner_id (PK) |
| **子テーブルキー** | customer_id (FK) |
| **カーディナリティ** | 1:N（1つの取引先に複数の価格条件が存在） |
| **結合条件** | 条件マスタ.customer_id = 取引先マスタ.partner_id |
| **フィルタ条件** | customer_id = NULL または customer_id = 'DEFAULT' の場合、取引先マスタを参照しない（標準価格） |
| **制約タイプ** | Foreign Key (FK) - Optional Reference |
| **参照整合性ルール** | customer_id が NULL/'DEFAULT' でない場合、取引先マスタに存在する partner_id のみが記録される |
| **説明** | ディーラー別の特別価格条件を参照。customer_id が NULL または 'DEFAULT' の場合は標準価格（すべてのディーラー共通）を示す。 |

---

### リレーション8: 品目マスタ → 取引先マスタ（import_export_group による制約）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PRODUCT_to_PARTNER_by_REGION |
| **親テーブル** | 品目マスタ, 取引先マスタ |
| **子テーブル** | 受注伝票_item |
| **親テーブルキー** | product_id (品目マスタ), partner_id (取引先マスタ) |
| **制約キー** | import_export_group (品目マスタ) vs region (取引先マスタ) |
| **カーディナリティ** | M:N（論理的な制約） |
| **結合条件** | 品目マスタ.import_export_group = '国内市場向け' ⇒ 取引先マスタ.region = 'domestic' の取引先のみが対象 品目マスタ.import_export_group = '海外市場向け' ⇒ 取引先マスタ.region = 'overseas' の取引先のみが対象 |
| **制約タイプ** | Business Logic Constraint（データベースレベルではなくアプリケーションロジックで実装） |
| **参照整合性ルール** | 受注伝票_item で記録される product_id の import_export_group が、取引先マスタの region と合致する必要あり |
| **説明** | 商品の国内/海外向けフラグと取引先の地域区分の整合性を確保。国内向け商品は国内ディーラーのみ、海外向け商品は海外ディーラーのみが取り扱う。 |

---

### リレーション9: BOM マスタ → 品目マスタ（親品目）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_PRODUCT_PARENT |
| **親テーブル** | 品目マスタ |
| **子テーブル** | BOMマスタ |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N（1つの親品目に複数のBOMレコードが存在） |
| **結合条件** | BOMマスタ.product_id = 品目マスタ.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | 品目マスタに存在する product_id のみが BOM の親品目として記録される |
| **説明** | BOM の親品目（完成品または組立品）の参照。品目マスタから親品目の詳細情報を取得。 |

---

### リレーション10: BOM マスタ → 品目マスタ（構成部品）

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_PRODUCT_COMPONENT |
| **親テーブル** | 品目マスタ |
| **子テーブル** | BOMマスタ |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | component_product_id (FK) |
| **カーディナリティ** | 1:N（1つの構成部品が複数のBOMで使用される可能性） |
| **結合条件** | BOMマスタ.component_product_id = 品目マスタ.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | 品目マスタに存在する product_id のみが BOM の構成部品として記録される |
| **説明** | BOM の構成部品（子品目）の参照。品目マスタから部品の詳細情報を取得。 |

---

### リレーション11: BOM マスタ → 拠点マスタ

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_LOCATION |
| **親テーブル** | 拠点マスタ |
| **子テーブル** | BOMマスタ |
| **親テーブルキー** | location_id (PK) |
| **子テーブルキー** | site_id (FK) |
| **カーディナリティ** | 1:N（1つの拠点に複数のBOMが存在） |
| **結合条件** | BOMマスタ.site_id = 拠点マスタ.location_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | 拠点マスタに存在する location_id のみが BOM の生産拠点として記録される |
| **説明** | BOM が適用される生産拠点の参照。拠点ごとに異なるBOM構成を持つ場合に使用。 |

---

## ER図（テキスト表記）

```
┌─────────────────────┐
│    拠点マスタ       │
│  (location_id: PK)  │
└──────────┬──────────┘
           │ 1:N
           │ (location_id)
           ↓
┌─────────────────────────────────────┐
│    受注伝票_header                  │
│  (order_id: PK)                     │
│  (location_id: FK)                  │
│  (customer_id: FK → partner_id)     │
└──────────┬──────────────────────────┘
           │ 1:N
           │ (order_id)
           ↓
┌─────────────────────────────────────┐
│    受注伝票_item                    │
│  (order_id, line_number: PK)        │
│  (order_id: FK)                     │
│  (product_id: FK)                   │
│  (pricing_date + product_id +       │
│   customer_id: FK → 条件マスタ)     │
└──────────┬──────────────────────────┘
           │ N:1
           │ (product_id)
           ↓
┌─────────────────────┐
│    品目マスタ       │
│  (product_id: PK)   │
└─────┬───────────────┘
      │ 1:N
      │ (product_id)
      ↓
┌────────────────────────────────┐
│      条件マスタ                │
│ (price_condition_id: PK)       │
│ (product_id: FK)               │
│ (customer_id: FK → partner_id) │
│ (valid_from, valid_to)         │
└────────────┬───────────────────┘
             │ N:1
             │ (customer_id)
             ↓
┌──────────────────────┐
│    取引先マスタ      │
│  (partner_id: PK)    │
│  (region: binary)    │
└──────────────────────┘

┌─────────────────────┐
│    BOMマスタ        │
│  (bom_id: PK)       │
│  (product_id: FK)   │
│  (component_product_│
│   _id: FK)          │
│  (site_id: FK)      │
└──────────┬──────────┘
           ├──→ 品目マスタ (product_id)
           ├──→ 品目マスタ (component_product_id)
           └──→ 拠点マスタ (location_id)
```

---

## 参照整合性チェック項目

| チェック項目 | 説明 |
| ------ | ------ |
| **孤立レコード** | 親テーブルに存在しない外部キー値を持つ子レコード |
| **時間的整合性** | 条件マスタの有効期間が重複していないか |
| **ドメイン制約** | 取引先のpartner_type が 'dealer' か確認 |
| **地域制約** | 品目の import_export_group と取引先の region の対応を確認 |
| **NULL値の扱い** | 条件マスタの customer_id が NULL/'DEFAULT' の場合は標準価格 |

