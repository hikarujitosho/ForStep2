# テーブル定義書
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

#### 現在在庫テーブル
**概要** : 現在の在庫状況（現物数・ステータス）をリアルタイムまたはバッチで記録するブロンズ層テーブルです。在庫数量や保管状態を保存し、在庫照会や引当・補充ロジックの基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| product_id | VARCHA‌R | PK | 品目コード（完成品ID）。外部マスター参照のキー。 |
| product_name | VARCHA‌R |  | 製品表示名（スナップショット）。 |
| location_id | VARCHA‌R |  | 拠点コード（在庫を保管している拠点の識別子）。ソースシステムの拠点コードをそのまま保持。拠点別の在庫分析に利用。 |
| inventory_quantity | DECIMAL |  | 在庫数量（所定単位）。 |
| inventory_status | VARCHA‌R |  | 在庫ステータス（例: available, reserved, damaged）。 |
| last_updated_timestamp | TIMESTAMP |  | 最終更新日時（在庫更新のタイムスタンプ）。 |

#### 月次在庫履歴テーブル
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

#### 調達伝票_headerテーブル
**概要** : 調達伝票（購買発注）のヘッダー情報を格納するブロンズ層テーブルです。調達先への発注情報を保持し、直接材（生産用部品・原材料）と間接材（消耗品・設備）の両方を対象とします。購買取引の基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| purchase_order_id | VARCHA‌R | PK | 調達伝票番号（購買発注番号）。ソースシステムの発注ID。 |
| order_date | DATE |  | 発注日（注文日）。 |
| expected_delivery_date | DATE |  | 期待納期（計画入荷日）。発注時にサプライヤーと合意した納期。リードタイム遵守率の算出に使用。 |
| supplier_id | VARCHA‌R |  | 仕入先コード（調達先の識別子）。サプライヤーマスターへの参照キー。 |
| supplier_name | VARCHA‌R |  | 仕入先名称（スナップショット）。 |
| account_group | VARCHA‌R |  | アカウントグループ（社内の購買組織や部門コード）。 |
| location_id | VARCHA‌R |  | 拠点コード（発注元の拠点識別子）。納品先拠点の情報。 |
| purchase_order_number | VARCHA‌R |  | 発注番号（社内管理用の発注番号）。 |
| currency | VARCHA‌R |  | 通貨コード（ISO 4217、例: 'JPY'）。 |
| order_subtotal_ex_tax | DECIMAL |  | 注文小計（税抜）。明細行の合計金額。 |
| shipping_fee_ex_tax | DECIMAL |  | 配送料および手数料（税抜）。 |
| tax_amount | DECIMAL |  | 消費税額。 |
| discount_amount_incl_tax | DECIMAL |  | 割引額（税込）。 |
| order_total_incl_tax | DECIMAL |  | 注文合計金額（税込）。最終支払額。 |
| order_status | VARCHA‌R |  | 注文ステータス（例: pending, approved, completed, cancelled）。 |
| approver | VARCHA‌R |  | 承認者名。 |
| payment_method | VARCHA‌R |  | 支払方法（例: credit_card, bank_transfer, invoice）。 |
| payment_confirmation_id | VARCHA‌R |  | 支払認証ID／請求書番号。 |
| payment_date | DATE |  | 支払確定日。 |
| payment_amount | DECIMAL |  | 支払金額。 |

#### 調達伝票_itemテーブル
**概要** : 調達伝票の明細情報を格納するブロンズ層テーブルです。発注ごとの品目、数量、単価、金額などの詳細を保持します。直接材・間接材の分類、受領記録、配送ステータスなどを管理し、調達実績分析の基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| purchase_order_id | VARCHA‌R | FK | 調達伝票ヘッダーの `purchase_order_id` を参照。 |
| line_number | VARCHA‌R | PK | 明細番号（伝票内の行識別子）。 |
| material_id | VARCHA‌R |  | 資材コード（ASIN、部品番号など）。調達品目の識別子。 |
| material_name | VARCHA‌R |  | 資材名称（商品名、部品名）。 |
| material_category | VARCHA‌R |  | 資材カテゴリ（例: Personal Computer, Photography, Electronics）。 |
| material_type | VARCHA‌R |  | 資材区分（direct: 直接材、indirect: 間接材）。 |
| unspsc_code | VARCHA‌R |  | UNSPSC分類コード（国際標準製品・サービス分類）。 |
| quantity | DECIMAL |  | 発注数量。 |
| unit_price_ex_tax | DECIMAL |  | 単価（税抜）。 |
| line_subtotal_incl_tax | DECIMAL |  | 明細小計（税込）。 |
| line_subtotal_ex_tax | DECIMAL |  | 明細小計（税抜）。 |
| line_tax_amount | DECIMAL |  | 明細消費税額。 |
| line_tax_rate | DECIMAL |  | 明細税率（例: 10% の場合 0.10）。 |
| line_shipping_fee_incl_tax | DECIMAL |  | 明細配送料および手数料（税込）。 |
| line_discount_incl_tax | DECIMAL |  | 明細割引額（税込）。 |
| line_total_incl_tax | DECIMAL |  | 明細合計金額（税込）。商品+配送料-割引。 |
| reference_price_ex_tax | DECIMAL |  | 参考価格（税抜）。カタログ価格や標準価格。 |
| purchase_rule | VARCHA‌R |  | 購買ルール（購買ポリシーや承認ルールの識別子）。 |
| ship_date | DATE |  | 出荷日。 |
| shipping_status | VARCHA‌R |  | 出荷ステータス（例: pending, shipped, delivered）。 |
| carrier_tracking_number | VARCHA‌R |  | 配送業者の問い合わせ番号（追跡番号）。 |
| shipped_quantity | DECIMAL |  | 発送商品の数量。 |
| carrier_name | VARCHA‌R |  | 配送業者名。 |
| delivery_address | VARCHA‌R |  | 配送先住所（納品先の住所情報）。 |
| receiving_status | VARCHA‌R |  | 受領記録ステータス（例: not_received, partially_received, received）。 |
| received_quantity | DECIMAL |  | 受領記録数量（検収済み数量）。 |
| received_date | DATE |  | 受領記録日（検収日）。 |
| receiver_name | VARCHA‌R |  | 受領記録者名。 |
| receiver_email | VARCHA‌R |  | 受領記録者Eメールアドレス。 |
| cost_center | VARCHA‌R |  | コストセンター（原価センタコード）。 |
| project_code | VARCHA‌R |  | プロジェクトコード（プロジェクト別の費用管理）。 |
| department_code | VARCHA‌R |  | 部署コード。 |
| account_user | VARCHA‌R |  | アカウントユーザー（発注者名）。 |
| user_email | VARCHA‌R |  | ユーザーのEメール（発注者のメールアドレス）。 |

#### 給与テーブル
**概要** : 給与計算結果の明細情報を保持するブロンズ層テーブルです。従業員ごとの給与項目（基本給、残業代、手当、控除等）の金額を記録します。人件費分析、コスト配賦、会計仕訳の基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| payroll_id | VARCHA‌R | PK | 給与ID（給与明細レコードの一意識別子）。給与計算バッチIDと社員IDと項目コードの組み合わせで生成されることが多い。 |
| employee_id | VARCHA‌R |  | 社員ID（従業員の識別子）。人事マスタへの参照キー。 |
| item_code | VARCHA‌R |  | 給与項目コード（例: basic_salary, overtime_pay, commute_allowance, health_insurance_deduction）。基本給、残業代、通勤費、社会保険料控除等の項目識別子。 |
| amount | DECIMAL |  | 金額（通貨単位: JPY を想定）。支給項目の場合は正の値、控除項目の場合は負の値または別途区分管理。 |
| is_taxable | VARCHA‌R |  | 課税区分（例: taxable, non_taxable）。所得税・住民税の課税対象かどうかを示すフラグ。 |
| gl_account | VARCHA‌R |  | 勘定科目コード（例: salaries_and_wages, statutory_welfare_expenses, welfare_expenses）。会計仕訳での勘定科目を指定。給与手当、法定福利費、福利厚生費等。 |
| cost_center | VARCHA‌R |  | コストセンター（原価センタコード）。部門別の人件費配賦に利用。 |
| project_id | VARCHA‌R |  | プロジェクトID（任意）。プロジェクト別の人件費管理に利用。プロジェクトに紐付かない場合は NULL。 |
| posting_date | DATE |  | 計上日（会計上の計上日付）。給与支給日または計上基準日。 |

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