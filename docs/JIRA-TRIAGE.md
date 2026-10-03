# Jira triage

`jira-triage` 已註冊（`internal/workflows/triage`）。原本以 Planner 自主派工的版本未通過 alpha，已刪除（可從版本歷史取得）；本文件描述的是取代它的固定階段版本。目前只有匿名結構驗收（真 engine／runtime／RPC protocol，fake Pi 代寫 candidate），尚未以真實票券、模型與私有 skill 做 live 驗證。

執行：`PWC_TRIAGE_SKILLS_DIR=<私有 skill 目錄> pi-workflow-controller run jira-triage "<TICKET-KEY> 其他提示"`。Prompt 必須以票號開頭；其後的文字只是候選來源，不是授權。模型與限額在 `triage.DefaultConfig()` 與 `triage.RunPolicy()`，數值是保守起點，待 live 調整。

以下遵循 repo 根目錄的 [專案憲法](../AGENTS.md)。該文件規範本專案的開發／review，不是另行注入 workflow nodes 的共通指令或工具限制。

## 方向

固定階段，而不是 Planner 決定下一步：

1. 取得整張票（intake）與文字來源的候選事實（facts），經 Go 結構驗收與獨立 validator 判讀（V0）。
2. 身分以資料庫查詢為準確認後，才做其他唯讀 runtime 查詢。
3. 調查 rounds 由 Investigator 在單一 session 內自主完成，不建立 subagent；每輪結束由 validator 稽核本輪的 session 觀測（V1）。
4. Steward 在固定觸發點質疑或導向；候選結論經 pro／con／cross 對抗驗證後產出報告。

Controller 只負責階段邊界的前置條件、exact committed Refs、確定性算術、預算與 cleanup；領域判斷由 Agent 與 validator 負責。Workflow 專用的判斷規則放在 repo 外的私有 skill 目錄，執行時展開到 run 目錄，repo 內只有通用描述。

## 已落地的共用能力

- Controller artifact：`Scope.Attach`（見 [DESIGN](../DESIGN.md) §4.2）。
- Step 稽核觀測：`StepSpec.Observe`（見 [DESIGN](../DESIGN.md) §6.3）。
- 私有 skill 目錄的受限展開與過期偵測：`triage.PrepareSkills`（見 [WORKFLOW-REUSE](WORKFLOW-REUSE.md) §4）。

## 階段

- S0（`runS0`）：caller prompt 以 Attach 記錄；intake Step 取得整張票並由 Go 驗完整性；facts Step 只從文字來源宣告候選事實與時間錨點，每項附可解析的定位；獨立 fact-check Step 逐項判讀。未被接受的項目以 feedback 有限重試，之後由 Controller 記錄為缺口。
- Rounds（`runRounds`）：每輪一個 round contract。Go 驗收 id、引用與定位、時間錨點重算、身分 decision 必須等於所引用 lookup 的 row（home stack 等於該 lookup 的 stack）、runtime 查詢收據的 UTC 窗口非零，以及 gap disposition 指向輸入中確實存在的 gap；不合格時同 session 修復一次。宣告的事實、錨點與 confirmed 身分 decision 交 fresh 的 fact-check 判讀（request 列出要判讀的 id）；同一項目累計被退回達上限即記為缺口，之後不得再宣告。Runtime 權限寫在每輪 request：home stack 的 confirmed decision 經判讀支持前為 identity-only（只做唯讀身分查詢），之後為 open（唯讀）。同一輪若沒有經判讀支持的 confirmed decision，任何 unconfirmed 或 conflict 的 home stack decision 會再關閉 runtime；被退回的 confirmed decision 不改變現狀。候選結論的程式碼引用須標明讀取的 revision 與其和部署版本的關係，讀部署版本時須指向 deployed build，程式碼片段須在 allowed evidence 內。這是 Agent 操作規則，不是阻擋；跨 stack 搜尋是否完整由 validator 判讀，不是 Controller 保證。
- Session：容量低於門檻時下一輪沿用同一 session；達門檻或用量未知時的處理（strict close 後 fresh，或沿用並告知）寫在 feedback。逾時的一輪（或其 fact-check）從相同 committed inputs 以 fresh session 重跑，有限次數，會計不重置；逾時 attempt 的落地檔不採用。Run 的 session／attempt 額度不足以涵蓋下一輪最壞情況時，rounds 提前結束並標明原因。
- Vision：facts 與 continue 的 round 可列出圖片請求；Controller 在下一輪之前派發 vision Step（不載入私有 skill，requirements 明寫自己用 read 讀圖、不委派），每個請求一個 fresh session，同時至多設定的數量，總數受 run 的上限與剩餘 session／attempt 額度限制。Vision 產物只是轉錄證據（自己的 evidence 檔），由引用它的 Agent 判斷；未執行的請求（額度用盡或已是最後一輪）在 Controller 的 batch 紀錄中列為缺口；vision 並行數必須留一個 live session 給 investigator。
- 稽核（V1）：每輪的 Step 開啟稽核觀測；輪末 Controller 以 Attach 固定該輪所有 attempt（含逾時失敗的）的原始 entries 與 tool call 索引，交 fresh 的 validator 對照 receipts 稽核。每個 finding 須有可解析的定位（記錄中的 entry id、該輪的 receipt 或證據）；findings 是回饋，不要求重做已提交的輪次。覆蓋不完整時 request 會明說。
- Steward：身分輪後 T1、candidate 時 T2a、stuck 時 T3（只有 redirect），各為 fresh session；challenge 作為下一輪的 feedback，T1 與 T2a 的 challenge 計入上限，T3 不計。T2a pass 或 blocked 結束 rounds（對抗驗證落地前）；challenge 額度用盡時 candidate 也結束 rounds 並標明原因。
- 對抗驗證（S2）：T2a pass 後，Controller 以 Attach 記錄 claim（round 的 candidate 原樣複製，自己的證據改以 round 的 Ref 引用，綁定 round 與 T2a），三個 verifier（pro／con／cross）各在 fresh session 平行判讀，只拿 claim 與其 allowed evidence 的 owners，不拿 steward notes、不載私有 skill。驗收沿用舊版：claim／role／allowed evidence 回聲、assessment 必填、basis 只能是 exact allowed evidence、結果來自該角色與模型的 closed fresh session。某角色的可恢復失敗用盡重試時記為 unavailable，其他失敗即 run 失敗。Controller 記錄 delivery 後由 steward T2b 判讀：pass 結束調查，challenge 回到 rounds；S2 次數有上限，用完後 T2a 仍會判讀新的 candidate，T2a pass 的 candidate 結束調查並標明原因。Candidate 的 allowed evidence 不得引用其他角色的判讀（steward、audit、fact check、claim、verification、delivery）或 session 觀測，避免交給 verifier。
- 相對舊版的放寬（已列）：verifier 允許同 session 修復一次（舊版不修復），owner 檢查改為「該 session 只服務這個 verifier 的 attempts，且在這次 verification 的 group 內」；delivery 由 Controller 在程序內產生，不再驗證 Agent 抄寫的失敗史。
- Verifier 的 inputs 含 allowed evidence 所在的 contract（例如 round 本身），因此能讀到其中的敘述；隔離靠 requirements，不是硬保證。
- 結束條件：T2b pass、blocked，或任一上限（round 數、run 額度、steward challenge、S2 次數）。
- 報告（S3）：fresh 的 report Step（不載私有 skill）拿到所有已提交結果，必須原樣抄寫 caller prompt 與 Controller 給的 claim 物件（最後一次驗證的 claim、delivery、T2b 與 passed／not-passed／none），對 Controller 從已提交 contract 組出的每個 gap（以 Ref 與 id 指名）恰好給一次處置，並宣告完整性：上限結束、claim 未通過或有 open gap 時必須是 incomplete。報告檔由 run 內展開的 renderer 產生，Go 以內嵌的同一 renderer 對已提交 inputs 重算並逐 byte 比對；結果須來自 closed fresh report session。每個處置為 resolved／not-applicable 的 gap 須附可解析的證據。報告還會呈現 T2a 的 notes、提前結束的原因（原樣抄寫）、各輪稽核發現與每個 gap 的原文。Session 觀測覆蓋不完整時，觀測紀錄以 `audit-coverage-<n>` gap 進入報告的涵蓋清單。Rounds、vision 與驗證的額度檢查都替報告保留一個最壞情況的 Step。舊版報告的 M6 dispositions、budget 段落與 report failures 由 rounds 的 Limit 取代；recovery 紀錄目前只在記憶體中（已列放寬，U10 決定是否提交）。

匿名測試只證明結構與流程；真 Pi／provider／skills／live 能力尚未驗證。
