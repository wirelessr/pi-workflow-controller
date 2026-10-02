# Jira triage

**現況：`jira-triage` 尚未註冊，不可執行。**原本以 Planner 自主派工的版本未通過 alpha，已刪除；設計與實作可從版本歷史取得。新版本在 `internal/workflows/triagev2` 逐步實作，完成並通過驗收後才註冊，屆時本文件以新設計完整重寫。

以下遵循 repo 根目錄的 [專案憲法](../AGENTS.md)。該文件規範本專案的開發／review，不是另行注入 workflow nodes 的共通指令或工具限制。

## 新版方向

固定階段，而不是 Planner 決定下一步：

1. 取得整張票（intake）與文字來源的候選事實（facts），經 Go 結構驗收與獨立 validator 判讀（V0）。
2. 身分以資料庫查詢為準確認後，才做其他唯讀 runtime 查詢。
3. 調查 rounds 由 Investigator 在單一 session 內自主完成，不建立 subagent；每輪結束由 validator 稽核本輪的 session 觀測（V1）。
4. Steward 在固定觸發點質疑或導向；候選結論經 pro／con／cross 對抗驗證後產出報告。

Controller 只負責階段邊界的前置條件、exact committed Refs、確定性算術、預算與 cleanup；領域判斷由 Agent 與 validator 負責。Workflow 專用的判斷規則放在 repo 外的私有 skill 目錄，執行時展開到 run 目錄，repo 內只有通用描述。

## 已落地的共用能力

- Controller artifact：`Scope.Attach`（見 [DESIGN](../DESIGN.md) §4.2）。
- Step 稽核觀測：`StepSpec.Observe`（見 [DESIGN](../DESIGN.md) §6.3）。
- 私有 skill 目錄的受限展開與過期偵測：`triagev2.PrepareSkills`（見 [WORKFLOW-REUSE](WORKFLOW-REUSE.md) §4）。

匿名測試只證明結構與流程；真 Pi／provider／skills／live 能力尚未驗證。
