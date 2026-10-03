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

## 已落地的階段（尚未接成可執行的 workflow）

- S0（`runS0`）：caller prompt 以 Attach 記錄；intake Step 取得整張票並由 Go 驗完整性；facts Step 只從文字來源宣告候選事實與時間錨點，每項附可解析的定位；獨立 fact-check Step 逐項判讀。未被接受的項目以 feedback 有限重試，之後由 Controller 記錄為缺口。
- Rounds（`runRounds`）：每輪一個 round contract。Go 驗收 id、引用與定位、時間錨點重算、身分 decision 必須等於所引用 lookup 的 row（home stack 等於該 lookup 的 stack）、runtime 查詢收據的 UTC 窗口非零，以及 gap disposition 指向輸入中確實存在的 gap；不合格時同 session 修復一次。宣告的事實、錨點與 confirmed 身分 decision 交 fresh 的 fact-check 判讀（request 列出要判讀的 id）；同一項目累計被退回達上限即記為缺口，之後不得再宣告。Runtime 權限寫在每輪 request：home stack 的 confirmed decision 經判讀支持前為 identity-only（只做唯讀身分查詢），之後為 open（唯讀）。同一輪若沒有經判讀支持的 confirmed decision，任何 unconfirmed 或 conflict 的 home stack decision 會再關閉 runtime；被退回的 confirmed decision 不改變現狀。候選結論的程式碼引用須標明讀取的 revision 與其和部署版本的關係，讀部署版本時須指向 deployed build，程式碼片段須在 allowed evidence 內。這是 Agent 操作規則，不是阻擋；跨 stack 搜尋是否完整由 validator 判讀，不是 Controller 保證。
- Session：容量低於門檻時下一輪沿用同一 session；達門檻或用量未知時的處理（strict close 後 fresh，或沿用並告知）寫在 feedback。逾時的一輪（或其 fact-check）從相同 committed inputs 以 fresh session 重跑，有限次數，會計不重置；逾時 attempt 的落地檔不採用。Run 的 session／attempt 額度不足以涵蓋下一輪最壞情況時，rounds 提前結束並標明原因。
- Vision：facts 與 continue 的 round 可列出圖片請求；Controller 在下一輪之前派發 vision Step（不載入私有 skill，requirements 明寫自己用 read 讀圖、不委派），每個請求一個 fresh session，同時至多設定的數量，總數受 run 的上限與剩餘 session／attempt 額度限制。Vision 產物只是轉錄證據（自己的 evidence 檔），由引用它的 Agent 判斷；未執行的請求在 Controller 的 batch 紀錄中列為缺口。
- 在 Steward 與對抗驗證落地前，candidate／stuck／blocked 一律結束 rounds；round 上限用盡也結束。

匿名測試只證明結構與流程；真 Pi／provider／skills／live 能力尚未驗證。
