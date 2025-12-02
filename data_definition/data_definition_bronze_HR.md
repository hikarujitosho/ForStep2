# テーブル定義書 - HR（人事給与）システム
---

## 概要
HRシステムは人事・給与計算を管理するシステムです。給与テーブルには社員の給与計算結果が記録され、製造原価や販管費の人件費配賦、EBITDA算出の基礎データとして利用されます。

---

#### 給与テーブル
**概要** : 社員の給与計算結果を保持するブロンズ層テーブルです。毎月の基本給、残業代、諸手当、控除額、支給総額などを記録します。人件費分析、原価配賦、EBITDA計算の基礎データとなります。
| カラム名 | 型 | 制約 | 説明 |
| ------ | ------ | ------ | ------ |
| payroll_id | VARCHA‌R | PK | 給与支給明細ID（給与レコードの一意識別子）。 |
| employee_id | VARCHA‌R | FK | 社員ID（社員マスタの識別子）。 |
| employee_name | VARCHA‌R |  | 社員名（スナップショット）。 |
| department | VARCHA‌R |  | 部門名（例: manufacturing, sales, R&D, administration）。製造、営業、研究開発、管理部門など。 |
| position | VARCHA‌R |  | 役職（例: manager, supervisor, engineer, operator）。 |
| payment_period | VARCHA‌R |  | 支給年月（フォーマット: YYYY-MM、例: '2025-11'）。 |
| base_salary | DECIMAL |  | 基本給（通貨単位: 'JPY'）。 |
| overtime_pay | DECIMAL |  | 残業代（時間外労働手当）。 |
| allowances | DECIMAL |  | 諸手当（住宅手当、通勤手当、家族手当など）。 |
| deductions | DECIMAL |  | 控除額（社会保険料、所得税、住民税など）。 |
| net_salary | DECIMAL |  | 支給総額（手取り額）。 |
| payment_date | DATE |  | 支給日。YYYY-MM-DD 形式。 |
| currency | VARCHA‌R |  | 通貨コード（ISO 4217、例: 'JPY'）。 |
| employment_type | VARCHA‌R |  | 雇用形態（例: full_time, part_time, contract, temporary）。正社員、パート、契約社員等。 |
| cost_center | VARCHA‌R |  | コストセンター（原価配賦の対象部門コード）。製造原価や販管費への配賦に使用。 |
