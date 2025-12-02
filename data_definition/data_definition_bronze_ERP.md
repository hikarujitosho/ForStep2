# テーブル定義書 - ERP（受発注）システム
---

## 概要
ERPシステムは受発注業務を管理するコアシステムです。顧客からの受注、製品マスタ、価格条件、部品表（BOM）、取引先情報、拠点情報を統合管理します。

---

#### 受注伝票_headerテーブル
**概要** : 受注伝票のヘッダー情報を格納するブロンズ層の生データ格納テーブルです。受注伝票番号や受注日時など、伝票単位のメタ情報を保持します。上位の Silver 層で正規化・日時整形・参照ID付与が行われます。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| order_id | VARCHA‌R | PK | 受注伝票番号（ソースシステムの伝票ID）。ブロンズ段階では文字列として保持。 |
| order_timestamp | TIMESTAMP |  | 伝票日付／受注日時。タイムゾーン情報がある場合は原本を保持、ETLで標準化する。 |
| location_id | VARCHA‌R |  | 拠点コード（受注を処理した拠点の識別子）。ソースシステムの拠点コードをそのまま保持。Silver層で拠点マスターとの紐付けを行う。 |
| customer_id | VARCHA‌R |  | 取引先コード（受注元の顧客や販売店の識別子）。ソースシステムの取引先IDを保持。後続処理で取引先マスターへの参照に利用。 |

#### 受注伝票_itemテーブル
**概要** : 受注伝票の明細行を格納するブロンズ層テーブルです。受注ごとの品目、数量、納期などの詳細を保持します。後続処理で品目コードの正規化や数量単位の変換を実施します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| order_id | VARCHA‌R | FK | 受注伝票番号（ヘッダー側の `order_id` を参照）。 |
| line_number | VARCHA‌R | PK | 明細番号（伝票内の行識別子）。 |
| product_id | VARCHA‌R |  | 完成品の品目コード（ソースシステムのID）。 |
| product_name | VARCHA‌R |  | 明細の製品名（スナップショット）。 |
| quantity | DECIMAL |  | 注文数量（単位はマスタに従う）。 |
| promised_delivery_date | TIMESTAMP |  | 納入予定日または納入日時。 |

#### 品目マスタテーブル
**概要** : 品目マスタ（原データ）を保持するテーブルです。品目コード、名称、単位、分類情報などを格納します。Silver 層で正規化したマスターに変換され、Gold 層のディメンションとして利用されます。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 品目コード（完成品ID）。主キーとして使用。 |
| product_name | VARCHA‌R |  | 品目名称（表示用）。 |
| base_unit_quantity | DECIMAL |  | 基本数量単位（パッケージや単位の基準）。 |
| brand_name | VARCHA‌R |  | ブランド名称。 |
| item_group | VARCHA‌R |  | 品目グループ（カテゴリ）。 |
| item_hierarchy | VARCHA‌R |  | 品目階層情報（カテゴリツリー）。 |
| detail_category | VARCHA‌R |  | 明細カテゴリ。 |
| tax_classification | VARCHA‌R |  | 税分類コード。 |
| transport_group | VARCHA‌R |  | 輸送グループ。 |
| import_export_group | VARCHA‌R |  | 輸入/輸出グループ。 |
| country_of_origin | VARCHA‌R |  | 原産地（国名またはコード）。 |

#### 条件マスタテーブル
**概要** : 価格・条件の原データを保持するマスタテーブルです。商品ごとの標準価格やディーラー別の販売価格を保存し、価格計算や請求処理の基になるデータです。ディーラーの注文実績（取引量、取引期間等）に応じた価格設定が可能で、価格改定履歴を管理するため有効期間を持つ時系列データとして保持します。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| price_condition_id | VARCHA‌R | PK | 価格条件ID（価格条件レコードの一意識別子）。product_id、customer_id、valid_fromの組み合わせで生成されることが多い。 |
| product_id | VARCHA‌R |  | 品目コード（完成品ID）。 |
| product_name | VARCHA‌R |  | 品目名称（表示用）。 |
| customer_id | VARCHA‌R |  | 取引先コード（価格条件が適用されるディーラーや販売店の識別子）。特定ディーラー向けの特別価格設定や、標準価格の場合は NULL または 'DEFAULT' を格納。取引先マスタの `partner_id` を参照。 |
| customer_name | VARCHA‌R |  | 取引先名称（スナップショット）。 |
| list_price_ex_tax | DECIMAL |  | 税抜標準価格（単位: JPY）。メーカー希望小売価格や基準価格。 |
| selling_price_ex_tax | DECIMAL |  | 税抜販売価格（単位: JPY）。実際の卸売価格。ディーラーごとの注文実績を加味して設定される。標準価格の場合は `list_price_ex_tax` と同額。 |
| discount_rate | DECIMAL |  | 割引率（例: 5%の場合 0.05）。標準価格からの割引率。`(list_price_ex_tax - selling_price_ex_tax) / list_price_ex_tax` で計算。 |
| price_type | VARCHA‌R |  | 価格区分（例: standard, volume_discount, dealer_special, promotional）。標準価格、ボリュームディスカウント、特約店特別価格、プロモーション価格等を区別。 |
| minimum_order_quantity | DECIMAL |  | 最低注文数量。この価格条件を適用するための最低注文ロット。 |
| currency | VARCHA‌R |  | 通貨コード（ISO 4217、例: 'JPY', 'USD'）。 |
| valid_from | DATE |  | 有効開始日。この価格条件が適用開始される日付。価格改定履歴を管理するためのキー。 |
| valid_to | DATE |  | 有効終了日。この価格条件が適用終了される日付。現在有効な価格の場合は NULL または '9999-12-31' などの最大値を格納。 |
| remarks | VARCHA‌R |  | 備考（価格設定の理由や特記事項）。「年間1000台以上の取引実績による特別価格」等の情報を記録。 |

#### BOMマスタテーブル
**概要** : Bill of Materials（部品表）の構成情報を保持するブロンズ層マスタテーブルです。製品を構成する部品・材料の関係、必要数量、有効期間を管理します。製造計画や所要量計算（MRP）の基礎データとなります。
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
| eff_start_date | DATE |  | 有効開始日。このBOM構成が有効になる開始日付。 |
| eff_end_date | DATE |  | 有効終了日。このBOM構成が有効な終了日付。将来の設計変更管理に利用。 |

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
