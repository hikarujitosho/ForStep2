# テーブル定義書 - P2P（間接材購買）システム
---

## 概要
P2Pシステムは間接材の購買業務（Purchase to Pay）を管理するシステムです。調達発注から検収・支払までのプロセスを統合管理し、調達リードタイム遵守率の分析、サプライヤー評価、支払管理に利用されます。

---

#### 調達伝票_headerテーブル
**概要** : 調達発注のヘッダー情報（注文全体の情報）を保持するブロンズ層テーブルです。発注日、サプライヤー、支払情報など、1つの発注単位を表します。明細行は別テーブル `調達伝票_item` に格納されます。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| purchase_order_id | VARCHA‌R | PK | 調達発注ID（発注番号の一意識別子）。 |
| order_date | DATE |  | 発注日。YYYY-MM-DD 形式。 |
| expected_delivery_date | DATE |  | 納品予定日（expected_delivery_date）。発注日にサプライヤー種別ごとのリードタイムを加算して算出。部品入荷リードタイム遵守率の算出に使用（expected_delivery_dateとreceived_dateを比較）。YYYY-MM-DD 形式。 |
| supplier_id | VARCHA‌R | FK | サプライヤーID（供給元の識別子）。 |
| supplier_name | VARCHA‌R |  | サプライヤー名（スナップショット）。 |
| account_group | VARCHA‌R |  | アカウントグループ（管理区分）。 |
| location_id | VARCHA‌R |  | 拠点コード（発注拠点の識別子）。ソースシステムの拠点コードをそのまま保持。拠点別の調達分析に利用。 |
| purchase_order_number | VARCHA‌R | UNIQUE | 発注伝票番号（外部システムの伝票番号）。ビジネスキー。 |
| currency | VARCHA‌R |  | 通貨コード（ISO 4217、例: 'JPY'）。 |
| order_subtotal_ex_tax | DECIMAL |  | 小計（税抜）。 |
| shipping_fee_ex_tax | DECIMAL |  | 送料（税抜）。 |
| tax_amount | DECIMAL |  | 消費税額。 |
| discount_amount_incl_tax | DECIMAL |  | 値引額（税込）。 |
| order_total_incl_tax | DECIMAL |  | 合計金額（税込）。 |
| order_status | VARCHA‌R |  | 発注ステータス（例: submitted, confirmed, received, cancelled）。 |
| approver | VARCHA‌R |  | 承認者名（発注承認を行った責任者）。 |
| payment_method | VARCHA‌R |  | 支払方法（例: bank_transfer, credit_card, check）。 |
| payment_confirmation_id | VARCHA‌R |  | 支払確認ID（決済システムとの連携用）。 |
| payment_date | DATE |  | 支払実施日。YYYY-MM-DD 形式。 |
| payment_amount | DECIMAL |  | 支払金額（支払済みの金額。前払・分割の場合もある）。 |

#### 調達伝票_itemテーブル
**概要** : 調達発注明細の情報（明細行単位の情報）を保持するブロンズ層テーブルです。発注した品目、数量、単価、納品日などを格納します。headerテーブルと1:Nのリレーションです。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| line_item_id | VARCHA‌R | PK | 明細行ID（明細行の一意識別子）。 |
| purchase_order_id | VARCHA‌R | FK | 発注ID（ヘッダーテーブルとの連携キー）。 |
| product_id | VARCHA‌R | FK | 品目ID（製品マスタまたは部品マスタの識別子）。 |
| product_name | VARCHA‌R |  | 品目名（スナップショット）。 |
| quantity_ordered | DECIMAL |  | 発注数量（単位は `unit_of_measure` 参照）。 |
| unit_of_measure | VARCHA‌R |  | 単位（例: 個, kg, m2, ケース）。 |
| unit_price_ex_tax | DECIMAL |  | 単価（税抜、通貨単位は header の `currency` に従う）。 |
| line_total_ex_tax | DECIMAL |  | 明細合計（税抜、= quantity_ordered × unit_price_ex_tax）。 |
| received_date | DATE |  | 実際の納品日。リードタイム遵守率算出に使用。YYYY-MM-DD 形式。 |
| quantity_received | DECIMAL |  | 受領済数量（検収済み数量）。 |
| inspection_status | VARCHA‌R |  | 検品ステータス（例: pending, passed, failed）。 |

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
| region | VARCHA‌R | FK | 地域区分、domestic/overseasのbinary。受注データにおいて、この項目が"domestic"の場合は品目マスタのimport_export_groupが"domestic"の商品のみ扱う。この項目が"overseas"の場合は品目マスタのimport_export_groupが"export"の商品のみ扱う。 |
| valid_from | DATE |  | 有効開始日。取引開始日または取引先登録日。 |
| valid_to | DATE |  | 有効終了日。取引終了日。現在取引中の場合は NULL または '9999-12-31'。 |

#### BOMマスタテーブル
**概要** : Bill of Materials（部品表）の構成情報を保持するブロンズ層マスタテーブルです。製品を構成する部品・材料の関係、必要数量、有効期間を管理します。製造計画や所要量計算（MRP）の基礎データとなります。P2Pシステムでは調達管理用にサプライヤー情報を追加保持します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| bom_id | VARCHA‌R | PK | BOMレコードの一意識別子（ID）。製品ID、拠点ID、構成部品IDの組み合わせで生成されることが多い。 |
| bom_name | VARCHA‌R |  | BOMレコードの名称（表示用）。親品目名や工程名を含む識別しやすい名称。 |
| product_id | VARCHA‌R |  | 親品目コード（完成品または組立品のID）。製品マスタの `product_id` を参照。 |
| site_id | VARCHA‌R |  | 拠点コード（生産拠点の識別子）。拠点ごとに異なるBOM構成を持つ場合に使用。 |
| production_process_id | VARCHA‌R |  | 生産プロセスID（製造工程や作業指示の識別子）。工程別のBOM管理に利用。 |
| component_product_id | VARCHA‌R |  | 構成部品コード（子品目のID）。この親品目を構成する部品・材料のID。 |
| component_quantity_per | DECIMAL |  | 構成部品の必要数量（親品目1単位あたりの使用数量）。 |
| component_quantity_uom | VARCHA‌R |  | 構成部品の数量単位（例: EACHES, KG, LITER）。 |
| supplier_id | VARCHA‌R | FK | サプライヤーID（この部品の主要供給元）。P2P固有項目。取引先マスタを参照。 |
| component_category | VARCHA‌R |  | 部品カテゴリ（例: engine_parts, electronic_components, body_parts, trim_parts）。P2P固有項目。部品種別の分類。 |
| is_critical | VARCHA‌R |  | 重要部品フラグ（例: yes, no）。P2P固有項目。サプライチェーン上のリスク管理対象かどうか。 |
| eff_start_date | DATE |  | 有効開始日。このBOM構成が有効になる開始日付。 |
| eff_end_date | DATE |  | 有効終了日。このBOM構成が有効な終了日付。将来の設計変更管理に利用。 |
