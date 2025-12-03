# テーブル定義書 - TMS（輸送管理）システム
---

## 概要
TMSシステムは輸送・物流の管理を行うシステムです。輸送コストの明細、運送業者情報、拠点情報を管理し、物流コスト分析、緊急輸送費率の算出に利用されます。

---

#### 輸送コストテーブル
**概要** : 出荷に紐づく輸送コストの明細を保持するブロンズ層テーブルです。運賃や保険、通関費などの費目別金額を格納します。請求照合やコスト分析の基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| cost_id | VARCHA‌R | FK | コスト明細の識別子（外部請求ID等）。 |
| shipment_id | VARCHA‌R | FK | 関連する出荷ID。 |
| location_id | VARCHA‌R |  | 拠点コード（輸送コストが発生した拠点の識別子）。ソースシステムの拠点コードをそのまま保持。拠点別の輸送コスト分析に利用。 |
| cost_type | VARCHA‌R |  | コスト種別（例: freight, insurance, customs）。 |
| cost_amount | DECIMAL |  | コスト金額（通貨単位は `currency` カラム参照）。 |
| currency | VARCHA‌R |  | 通貨コード（ISO 4217 推奨、例: 'JPY'）。 |
| billing_date | DATE |  | 請求日（請求書の日付）。 |

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
