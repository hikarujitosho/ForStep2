# テーブル定義書 - MES（生産実績）システム
---

## 概要
MESシステムは製造実行および出荷実績を管理するシステムです。生産された製品の出荷情報、配送トラッキング、リードタイム管理を行います。

---

#### 出荷伝票_headerテーブル
**概要** : 出荷伝票のヘッダー情報を保持するブロンズ層テーブルです。出荷識別子と出荷日時など、伝票レベルのメタデータを記録します。配送トラッキングや出荷実績の元データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| shipment_id | VARCHA‌R | PK | 出荷伝票を一意に識別するID（ソースシステムの出荷ID）。 |
| shipment_timestamp | TIMESTAMP |  | 伝票作成日時／出荷日時のタイムスタンプ。 |
| location_id | VARCHA‌R |  | 拠点コード（出荷元の拠点識別子）。ソースシステムの拠点コードをそのまま保持。Silver層で拠点マスターとの紐付けを行う。 |
| customer_id | VARCHA‌R |  | 取引先コード（出荷先の顧客や販売店の識別子）。ソースシステムの取引先IDを保持。後続処理で取引先マスターへの参照に利用。 |

#### 出荷伝票_itemテーブル
**概要** : 出荷伝票の明細情報を格納するブロンズ層テーブルです。出荷ごとの品目、数量、実績日時、配送ステータスなどを保持し、物流のトレーサビリティに利用します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| shipment_id | VARCHA‌R | FK | 出荷伝票ヘッダーの `shipment_id` を参照。 |
| order_id | VARCHA‌R |  | 元受注伝票の `order_id`（関連がある場合）。 |
| line_number | VARCHA‌R |  | 明細番号（出荷伝票内の行識別子）。 |
| product_id | VARCHA‌R |  | 出荷品目のコード。 |
| product_name | VARCHA‌R |  | 出荷品目の名称（スナップショット）。 |
| quantity | DECIMAL |  | 出荷数量（単位はマスタに従う）。 |
| carrier_name | VARCHA‌R |  | 運送業者名（キャリア）。 |
| transportation_mode | VARCHA‌R |  | 輸送モード（例: road, sea, air）。 |
| planned_ship_date | DATE |  | 計画出荷日（日付）。 |
| actual_ship_timestamp | TIMESTAMP |  | 実績出荷日時。 |
| expected_ship_date | DATE |  | 期待出荷日（予測）。 |
| actual_arrival_timestamp | TIMESTAMP |  | 実到着日時（受領日時）。 |
| delivery_status | VARCHA‌R |  | 配送ステータス（例: pending, in_transit, delivered, delayed）。 |

#### 取引先マスタテーブル
**概要** : 取引先（サプライヤー、ディーラー、顧客等）の基本情報を保持するブロンズ層マスタテーブルです。調達先、販売先、物流業者など、すべての取引先を統合管理します。取引先区分により、サプライヤー向けの調達処理や、ディーラー向けの受注・出荷処理に利用されます。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| partner_id | VARCHA‌R | PK | 取引先ID（取引先の一意識別子）。ソースシステムの取引先コード。 |
| partner_name | VARCHA‌R |  | 取引先名称（正式名称）。 |
| partner_type | VARCHA‌R |  | 取引先区分（例: supplier, dealer, customer, logistics_provider）。サプライヤー（仕入先）、ディーラー（販売店）、直販顧客、物流業者等を区別。 |
| partner_category | VARCHA‌R |  | 取引先カテゴリ（例: tier1_supplier, tier2_supplier, authorized_dealer, independent_dealer）。Tier1サプライヤー、正規ディーラー、代理店等の分類。 |
| tax_id | VARCHA‌R |  | 税務登録番号（法人番号、VAT番号、TIN等）。請求書発行や税務申告に使用。 |
| address | VARCHA‌R |  | 住所（番地以降の詳細住所）。 |
| city | VARCHA‌R |  | 市区町村。 |
| state_province | VARCHA‌R |  | 都道府県/州（日本の場合は都道府県、海外の場合はState/Province）。 |
| postal_code | VARCHA‌R |  | 郵便番号。 |
| country | VARCHA‌R |  | 国コード（ISO 3166、例: 'JP', 'US', 'CN'）。 |
| contact_person | VARCHA‌R |  | 担当者名（窓口となる担当者のフルネーム）。 |
| contact_email | VARCHA‌R |  | 担当者メールアドレス。 |
| contact_phone | VARCHA‌R |  | 電話番号（国際電話番号形式推奨）。 |
| payment_terms | VARCHA‌R |  | 支払条件（例: NET30, NET60, COD, prepayment）。締め日・支払日のルール。 |
| credit_limit | DECIMAL |  | 与信限度額（通貨単位: 取引通貨に従う）。販売先の場合の与信枠、または調達先の場合の発注限度額。 |
| currency | VARCHA‌R |  | 取引通貨（ISO 4217、例: 'JPY', 'USD', 'EUR'）。この取引先との標準取引通貨。 |
| is_active | VARCHA‌R |  | 有効フラグ（例: active, inactive）。取引中/取引停止を示す。 |
| account_group | VARCHA‌R |  | アカウントグループ（社内の管理区分や購買組織コード）。 |
| region | VARCHA‌R |  | 地域区分（例: domestic, overseas, north_america, europe, asia）。国内/海外、エリア別の分類。 |
| valid_from | DATE |  | 有効開始日。取引開始日または取引先登録日。 |
| valid_to | DATE |  | 有効終了日。取引終了日。現在取引中の場合は NULL または '9999-12-31'。 |

#### 拠点マスタテーブル
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
