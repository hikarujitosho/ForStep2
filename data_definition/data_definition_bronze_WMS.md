# テーブル定義書 - WMS（在庫管理）システム
---

## 概要
WMSシステムは倉庫および在庫管理を行うシステムです。現在の在庫状況と月次の在庫履歴を管理し、在庫最適化、棚卸資産回転期間の算出に利用されます。

---

#### current_inventoryテーブル
**概要** : 現在の在庫状況（現物数・ステータス）をリアルタイムまたはバッチで記録するブロンズ層テーブルです。在庫数量や保管状態を保存し、在庫照会や引当・補充ロジックの基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 品目コード（完成品ID）。外部マスター参照のキー。 |
| product_name | VARCHA‌R |  | 製品表示名（スナップショット）。 |
| location_id | VARCHA‌R |  | 拠点コード（在庫を保管している拠点の識別子）。ソースシステムの拠点コードをそのまま保持。拠点別の在庫分析に利用。 |
| inventory_quantity | DECIMAL |  | 在庫数量（所定単位）。 |
| inventory_status | VARCHA‌R |  | 在庫ステータス（例: available, reserved, damaged）。 |
| last_updated_timestamp | TIMESTAMP |  | 最終更新日時（在庫更新のタイムスタンプ）。 |

#### monthly_inventoryテーブル
**概要** : 月末時点の在庫状況を履歴として記録するブロンズ層テーブルです。現在在庫テーブルの月次スナップショットとして、製品・拠点・ステータス別の在庫数量を時系列で保持します。在庫推移の分析や過去時点の在庫再現、トレンド分析の基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 品目コード（完成品ID）。外部マスター参照のキー。 |
| product_name | VARCHA‌R |  | 製品表示名（スナップショット時点）。 |
| location_id | VARCHA‌R | PK | 拠点コード（在庫を保管している拠点の識別子）。ソースシステムの拠点コードをそのまま保持。拠点別の在庫分析に利用。 |
| year_month | VARCHA‌R | PK | 在庫記録の対象年月（フォーマット: YYYY-MM 推奨、例: '2025-11'）。月末時点のスナップショットを表す。 |
| inventory_quantity | DECIMAL |  | 在庫数量（所定単位）。 |
| inventory_status | VARCHA‌R | PK | 在庫ステータス（例: available, reserved, damaged）。 |
| snapshot_date | DATE |  | スナップショット取得日（月末日）。データ取得タイミングの記録。YYYY-MM-DD 形式。 |

#### location_masterテーブル
**概要** : 社内拠点（製造拠点、倉庫、営業所等）の基本情報を保持するブロンズ層マスタテーブルです。受注・出荷・在庫・生産などの各業務プロセスで使用される拠点コードの参照マスタとなります。拠点別の業務分析、コスト配賦、在庫管理に利用されます。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| location_id | VARCHA‌R | PK | 拠点ID（拠点の一意識別子）。社内の拠点コード。 |
| location_name | VARCHA‌R |  | 拠点名称（正式名称、例: 川崎製作所、狭山工場、鈴鹿製作所）。 |
| location_type | VARCHA‌R |  | 拠点区分（例: manufacturing_plant, warehouse, distribution_center, sales_office）。製造拠点、倉庫、物流センター、営業所等を区別。 |
| address | VARCHA‌R |  | 住所（番地以降の詳細住所）。 |
| city | VARCHA‌R |  | 市区町村。 |
| state_province | VARCHA‌R |  | 都道府県/州（日本の場合は都道府県、海外の場合はState/Province）。 |
| postal_code | VARCHA‌R |  | 郵便番号。 |
| country | VARCHA‌R |  | 国コード（ISO 3166、例: 'JP', 'US', 'CN'）。 |
| contact_person | VARCHA‌R |  | 担当者名（拠点責任者または窓口担当者のフルネーム）。 |
| contact_phone | VARCHA‌R |  | 電話番号（国際電話番号形式推奨）。 |
| contact_email | VARCHA‌R |  | 担当者メールアドレス。 |
| is_active | VARCHA‌R |  | 有効フラグ（例: active, inactive）。稼働中/休止中を示す。 |
| valid_from | DATE |  | 有効開始日。拠点開設日または稼働開始日。 |
| valid_to | DATE |  | 有効終了日。拠点閉鎖日。現在稼働中の場合は NULL または '9999-12-31'。 |
