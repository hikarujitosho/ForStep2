# リレーション定義書 - P2P（購買）システム

## 概要
本書は、Bronze層のP2P（Purchase to Pay：購買）システムの各テーブル間のリレーション（関連）を定義します。
各リレーションは、親テーブル → 子テーブルの参照関係を示し、データの整合性維持とクエリの結合条件を明確にします。
P2Pシステムは、直接材（製造部品）と間接材（MRO資材・消耗品）の両方を管理し、原価計算や調達リードタイム分析に利用されます。

---

## リレーション一覧

### リレーション1: procurement_header → location_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PURCHASE_HEADER_to_LOCATION |
| **親テーブル** | location_master |
| **子テーブル** | procurement_header |
| **親テーブルキー** | location_id (PK) |
| **子テーブルキー** | location_id |
| **カーディナリティ** | 1:N（1つの拠点に複数の調達発注が存在可能） |
| **結合条件** | procurement_header.location_id = location_master.location_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | location_masterに存在する location_id のみが調達伝票に記録される |
| **説明** | 調達発注を処理した拠点の識別。location_masterから拠点の詳細情報（拠点名、住所、製造拠点等）を参照する。拠点別の調達分析に利用。 |
| **備考** | P2P専用のlocation_masterを参照。ERPのlocation_masterと同じ構造を持つ。 |

---

### リレーション2: procurement_header → partner_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PURCHASE_HEADER_to_SUPPLIER |
| **親テーブル** | partner_master |
| **子テーブル** | procurement_header |
| **親テーブルキー** | partner_id (PK) |
| **子テーブルキー** | supplier_id |
| **カーディナリティ** | 1:N(1つの取引先(サプライヤー)に複数の調達発注が存在可能) |
| **結合条件** | procurement_header.supplier_id = partner_master.partner_id |
| **フィルタ条件** | partner_master.partner_type = 'supplier' |
| **制約タイプ** | Foreign Key (FK) with Domain Constraint |
| **参照整合性ルール** | partner_type が 'supplier' である取引先の partner_id のみが調達伝票に記録される |
| **説明** | 調達先(サプライヤー)の識別。partner_masterから仕入先の詳細情報(名称、住所、担当者、支払条件、Tier区分等)を参照する。partner_type が 'supplier' 以外の場合は調達対象にならない。 |

---

### リレーション3: procurement_item → procurement_header

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PURCHASE_ITEM_to_PURCHASE_HEADER |
| **親テーブル** | procurement_header |
| **子テーブル** | procurement_item |
| **親テーブルキー** | purchase_order_id (PK) |
| **子テーブルキー** | purchase_order_id (FK) |
| **カーディナリティ** | 1:N(1つの調達発注に複数の明細行が存在) |
| **複合主キー** | procurement_item (purchase_order_id, line_number) |
| **結合条件** | procurement_item.purchase_order_id = procurement_header.purchase_order_id |
| **制約タイプ** | Foreign Key (FK) - Composite Key |
| **参照整合性ルール** | procurement_header に存在する purchase_order_id のみが明細行に記録される |
| **説明** | 調達伝票のヘッダーと明細行の関連付け。1つの調達発注に複数の資材（部品・材料）が記載される。 |

---

### リレーション4: procurement_item → product_master(完成品連携)

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PURCHASE_ITEM_to_PRODUCT |
| **親テーブル** | product_master |
| **子テーブル** | procurement_item |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N(1つの完成品に複数の調達明細が存在) |
| **結合条件** | procurement_item.product_id = product_master.product_id |
| **フィルタ条件** | procurement_item.material_type = 'direct' の場合のみ product_id が設定される |
| **制約タイプ** | Foreign Key (FK) - Conditional Reference |
| **参照整合性ルール** | product_masterに存在する product_id のみが調達明細に記録される。直接材の場合のみ設定され、間接材の場合は NULL または空文字列。 |
| **説明** | 調達明細と完成車（製品）の連携。直接材（製造部品）の場合、どの完成車向けの部品調達かをトレース可能にする。原価計算時に車種別（product_id）で集計される。間接材の場合は product_id は設定されない。 |

---

### リレーション5: procurement_item → bom_master(直接材参照)

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | PURCHASE_ITEM_to_BOM_COMPONENT |
| **親テーブル** | bom_master |
| **子テーブル** | procurement_item |
| **親テーブルキー** | component_product_id (part of composite key) |
| **子テーブルキー** | material_id |
| **カーディナリティ** | N:1(複数の調達明細が同一の部品を参照可能) |
| **結合条件** | procurement_item.material_id = bom_master.component_product_id |
| **フィルタ条件** | procurement_item.material_type = 'direct' の場合のみ結合 |
| **制約タイプ** | Logical Foreign Key (FK) - Conditional Reference |
| **参照整合性ルール** | material_type が 'direct' の場合、material_id は bom_masterの component_product_id に存在する必要がある。間接材の場合は bom_masterを参照しない。 |
| **説明** | 直接材(製造部品)の場合、material_id は bom_masterの構成部品 component_product_id を参照する。bom_masterから部品の詳細情報(bom_name、必要数量、サプライヤー等)を取得。間接材の場合は material_id は自然発番され、bom_masterを参照しない。 |
| **備考** | material_name には直接材の場合 bom_masterの bom_name が格納される。間接材の場合は間接材の品名が格納される。 |

---

### リレーション6: bom_master → product_master(親品目)

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_PRODUCT_PARENT |
| **親テーブル** | product_master |
| **子テーブル** | bom_master |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | product_id (FK) |
| **カーディナリティ** | 1:N(1つの親品目に複数のBOMレコードが存在) |
| **結合条件** | bom_master.product_id = product_master.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | product_masterに存在する product_id のみが BOM の親品目として記録される |
| **説明** | BOM の親品目(完成品または組立品)の参照。product_masterから親品目の詳細情報(品目名、単位、分類等)を取得。P2Pシステムでは調達管理の観点から製品構成を参照する。 |

---

### リレーション7: bom_master → product_master(構成部品)

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_PRODUCT_COMPONENT |
| **親テーブル** | product_master |
| **子テーブル** | bom_master |
| **親テーブルキー** | product_id (PK) |
| **子テーブルキー** | component_product_id (FK) |
| **カーディナリティ** | 1:N(1つの構成部品が複数のBOMで使用される可能性) |
| **結合条件** | bom_master.component_product_id = product_master.product_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | product_masterに存在する product_id のみが BOM の構成部品として記録される |
| **説明** | BOM の構成部品(子品目)の参照。product_masterから部品の詳細情報を取得。procurement_item.material_id が component_product_id を参照することで、どの部品を調達しているかを識別。 |

---

### リレーション8: bom_master → location_master

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_LOCATION |
| **親テーブル** | location_master |
| **子テーブル** | bom_master |
| **親テーブルキー** | location_id (PK) |
| **子テーブルキー** | site_id (FK) |
| **カーディナリティ** | 1:N(1つの拠点に複数のBOMが存在) |
| **結合条件** | bom_master.site_id = location_master.location_id |
| **制約タイプ** | Foreign Key (FK) |
| **参照整合性ルール** | location_masterに存在する location_id のみが BOM の生産拠点として記録される |
| **説明** | BOM が適用される生産拠点の参照。拠点ごとに異なるBOM構成を持つ場合に使用。location_masterから拠点の詳細情報を取得。 |
| **備考** | P2P専用のlocation_masterを参照。 |

---

### リレーション9: bom_master → partner_master(サプライヤー)

| 属性 | 値 |
| ------ | ------ |
| **リレーション名** | BOM_to_SUPPLIER |
| **親テーブル** | partner_master |
| **子テーブル** | bom_master |
| **親テーブルキー** | partner_id (PK) |
| **子テーブルキー** | supplier_id (FK) |
| **カーディナリティ** | 1:N(1つのサプライヤーが複数の部品を供給) |
| **結合条件** | bom_master.supplier_id = partner_master.partner_id |
| **フィルタ条件** | partner_master.partner_type = 'supplier' |
| **制約タイプ** | Foreign Key (FK) with Domain Constraint |
| **参照整合性ルール** | partner_type が 'supplier' である取引先の partner_id のみが BOM に記録される |
| **説明** | BOMに登録された部品の主要サプライヤーの参照。P2P固有項目。部品ごとの調達先を管理し、調達計画やサプライヤー評価に利用。partner_masterからサプライヤーの詳細情報(Tier区分、リードタイム、与信等)を取得。 |
| **備考** | P2Pシステム固有のBOM拡張項目。ERPのbom_masterには存在しない。 |

---

## 特殊なリレーション・制約

### 条件付きリレーション: procurement_item.material_type による参照先の切り替え

| 属性 | 値 |
| ------ | ------ |
| **制約名** | MATERIAL_TYPE_REFERENCE_CONSTRAINT |
| **対象テーブル** | procurement_item |
| **制約キー** | material_type ('direct' または 'indirect') |
| **制約内容** | material_type = 'direct' の場合:<br>・material_id は bom_master.component_product_id を参照<br>・material_name は bom_master.bom_name が格納される<br>・product_id は product_master.product_id を参照(完成車との連携)<br><br>material_type = 'indirect' の場合:<br>・material_id はどのマスタも参照せず、自然発番される<br>・material_name は間接材の品名が格納される<br>・product_id は NULL または空文字列 |
| **制約タイプ** | Business Logic Constraint（アプリケーションロジックで実装） |
| **説明** | 直接材（製造部品）と間接材（MRO資材・消耗品）で参照先が異なる。直接材は製品構成（BOM）と連携し原価計算に使用される。間接材は製品と紐付かず、間接費として管理される。 |

---

## ER図（テキスト表記）

```
┌─────────────────────┐
│   location_master   │
│  (location_id: PK)  │
│  [P2P専用]          │
└──────────┬──────────┘
           │ 1:N
           │ (location_id)
           ↓
┌─────────────────────────────────────┐
│   procurement_header                │
│  (purchase_order_id: PK)            │
│  (location_id: FK)                  │
│  (supplier_id: FK → partner_id)     │
└──────────┬──────────────────────────┘
           │ 1:N
           │ (purchase_order_id)
           ↓
┌─────────────────────────────────────────────┐
│    procurement_item                         │
│  (purchase_order_id, line_number: PK)       │
│  (purchase_order_id: FK)                    │
│  (material_id: 条件付きFK)                  │
│  (product_id: FK - direct のみ)            │
│  (material_type: 'direct' | 'indirect')     │
└──────────┬──────────────────────────────────┘
           │ N:1 (direct のみ)
           │ (material_id → component_product_id)
           ↓
┌──────────────────────────────────┐
│      bom_master                  │
│  (bom_id: PK)                    │
│  (product_id: FK)                │
│  (component_product_id: FK)      │
│  (site_id: FK → location_id)     │
│  (supplier_id: FK → partner_id)  │
└─────┬────────────┬───────────────┘
      │ N:1        │ N:1
      │            │ (supplier_id)
      │            ↓
      │      ┌──────────────────────┐
      │      │   partner_master     │
      │      │  (partner_id: PK)    │
      │      │  (partner_type)      │
      │      └──────────────────────┘
      │                ↑
      │ (product_id)   │ 1:N
      │                │ (supplier_id)
      ↓                │
┌─────────────────────┴────┐
│    product_master        │
│  (product_id: PK)        │
└──────────────────────────┘
      ↑
      │ N:1 (direct のみ)
      │ (product_id)
      │
  procurement_item
```

---

## 参照整合性チェック項目

| チェック項目 | 説明 |
| ------ | ------ |
| **孤立レコード** | 親テーブルに存在しない外部キー値を持つ子レコード |
| **サプライヤー制約** | 取引先のpartner_type が 'supplier' か確認 |
| **直接材・間接材の整合性** | material_type='direct' の場合、material_id が bom_masterに存在し、product_id が設定されているか確認 |
| **間接材のproduct_id** | material_type='indirect' の場合、product_id が NULL または空文字列であることを確認 |
| **BOMサプライヤー整合性** | bom_masterの supplier_id が partner_masterの partner_type='supplier' であることを確認 |
| **リードタイム計算** | expected_delivery_date = order_date + リードタイム（サプライヤー別）が正しく計算されているか |
| **拠点参照整合性** | procurement_header.location_id と bom_master.site_id が P2P専用のlocation_masterに存在するか確認 |

---

## クロスシステム参照

P2Pシステムは以下のERPシステムのマスタテーブルを参照します：

| ERPテーブル | P2Pでの使用目的 | 参照キー |
| ------ | ------ | ------ |
| product_master | 完成品(親品目・構成部品)の参照 | product_id, component_product_id |
| partner_master | サプライヤー情報の参照 | supplier_id → partner_id (partner_type='supplier') |

注：location_masterはP2P専用のものを使用し、ERPのlocation_masterとは別管理。ただし、構造は同一。

---

## 業務ルール・制約

### 直接材と間接材の区分

| material_type | 定義 | 用途 | product_id | material_id参照先 |
| ------ | ------ | ------ | ------ | ------ |
| **direct** | 直接材(製造部品) | 完成車の製造に直接使用される部品・材料。原価計算の対象。 | 必須(完成車ID) | bom_master.component_product_id |
| **indirect** | 間接材（MRO資材） | 工場運営に必要な消耗品・設備・オフィス用品等。間接費として処理。 | NULL/空文字列 | 自然発番(マスタ参照なし) |

### 原価計算フロー

1. procurement_item で material_type='direct' のレコードを抽出
2. product_id（完成車ID）でグルーピング
3. line_subtotal_ex_tax（明細小計税抜）を合計
4. 車種別（product_id）の売上原価として集計

### サプライヤー評価指標

- リードタイム遵守率: expected_delivery_date と received_date を比較
- 品質: receiving_status が 'rejected' の比率
- Tier区分: partner_master.partner_category (tier1_supplier, tier2_supplier)
- 重要部品: bom_master.is_critical フラグによる管理

---

## 変更履歴

| 日付 | バージョン | 変更内容 | 作成者 |
| ------ | ------ | ------ | ------ |
| 2025-12-03 | 1.0 | 初版作成 | - |
| 2025-01-XX | 1.1 | 全テーブル名を英語に変換(procurement_header/item, partner_master, bom_master, product_master, location_master) | - |
