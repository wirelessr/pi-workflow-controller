# Pi Workflow Controller 產品需求

本文件定義通用產品需求；精確 API、錯誤 disposition 與工程預設見 [DESIGN](DESIGN.md)，維護入口見 [IMPLEMENTATION](IMPLEMENTATION.md)，測試方法見 [VERIFICATION](docs/VERIFICATION.md)。這不是執行或驗收歷史。

## 1. 產品目標

建立本地、輕量、以 Go 開發並交付單一 Controller binary 的 workflow controller，使用 Bubble Tea TUI。Controller 管理獨立 Headless Pi processes，提供流程控制、context 隔離、逐角色模型綁定、檔案 contract 交接、檢核與 feedback loop。

多種 workflow 以 Go 程式碼預先定義，不提供外部 workflow config、動態流程修改、DSL、GUI 編排或通用 DAG interpreter。「零配置」只指 workflow 定義方式，不代表不需要 Pi／Node、provider 認證或工具環境。

目前產品 workflows 為 `smoke-echo` 與固定 deep `code-review`；後者的角色、模型、contracts、權限與業務結果見 [CODE-REVIEW](docs/CODE-REVIEW.md)。任何具體 workflow 的角色數量、名稱及順序都不是框架限制。

## 2. 輸入、部署與配置

- CLI 固定為 `pi-workflow-controller list`、`pi-workflow-controller run <workflow> "一行 Prompt"`。
- 不要求 task 名稱、input file 或 JSON 輸入；不從 stdin 讀 Prompt。CLI 只選 workflow 與提供 Prompt，不承載流程配置。
- Prompt 必須非空、單行 UTF-8，並符合程式碼內的大小限制；Controller 保存原始 Prompt 與執行 metadata，不將輸入作 shell 展開或目錄名稱。
- Task ID 由 Controller 自動產生，所有執行資料落在 `~/WIP/<task-id>/runs/<run-id>/`，啟動時顯示實際路徑。
- 每個 Controller process 執行一個 run；不同 Controller 的 task、Refs 與 cleanup ownership 必須分離。共享 discovery 並不因此隔離。
- 目標平台為 macOS arm64，Pi runtime 明確綁定支援版本；不自動宣稱其他平台／版本相容。Build 前提見 [README](README.md)。
- Workflow、RoleSpec、timeouts 與防禦參數在程式碼定義，變更須同步設計及測試，不新增隱含 CLI 配置。

## 3. Context 與模型隔離

- 各角色可以使用不同 Pi process／session 及明確的 provider/model/thinking 配置，不強迫所有角色繼承同一模型。
- 一個 handle 終身對應一個 Pi process 及持久化 session。保留 handle 可重用 context；`OpenSession` 建立新 handle 以隔離階段或輪次。
- Session 重用／新建的邊界由 workflow 決定，不硬編碼為全部常駐或每步重建。
- 同一 handle 一次只能服務一個 Step，lease 涵蓋派送、等待、檢核與 publication。
- Fresh session 隔離對話歷史，不隔離 host filesystem、工具、認證、全域規則或外部副作用。

## 4. Contract 交接與驗收

- Process 間的業務交接使用實體 JSON contract；RPC 可承載控制訊息。
- 原始證據與分析結論分開落地，contract 透過 file ID 引用 evidence/artifacts，不必複製完整對話。
- Workflow 定義自己的 data schema；框架驗 envelope、identity、JSON/schema、路徑、limits 與 digests，不硬編碼業務 verdict。
- 每個 attempt 有獨立目錄、identity 與 dispatch token；舊 candidate、同名檔案或檔案存在不可當作本次成功。
- Stage 建立固定 bytes 快照，Confirm 核對本次 execution，Publish 原子發布。Controller 自己產生的 bytes（例如原始 Prompt）走同一條 Stage／Publish／commit 取得 Ref，沒有 execution 可 Confirm，ownership 標為 Controller。**Publish 不等於 engine commit**；必要 journal append+Sync 與持久化提交完成後才授權下游 Ref。
- Inputs、feedback refs、Decision、Decode／ReadContract、Retry／Parallel results 與最終結果使用同一 committed resolver；不能用自行計算 hash、掃描目錄或跨 run Ref 繞過。
- 結構驗收失敗不交給下游 reviewer；業務語意及接受／退回條件由 workflow 明確驗收，schema 通過不證明內容正確。

## 5. 流程控制與 feedback

框架必須能表達順序、條件、平行／join、跨輪隔離及有限 retry。一般流程用 Go 函式與條件式，不由模型或 TUI 選下一節點。

通用的 produce/review/deliver 模式：

```text
Worker → contract 結構檢核
  不合格 → 有額度則把格式 feedback 交回 Worker
  合格   → Reviewer → reviewer contract 檢核
    reject → 有額度則把本輪 Ref 與 feedback 交回 Worker
    pass   → Deliverer → 最終驗收
```

- 這是能力範例，不規定所有 workflow 都有三個角色或 pass/reject 欄位。
- Retry 是首次執行之外的重試；count 初始為 0，只在 `< maxRetries` 時增加並重跑。預算、重跑範圍與 recipient 由 workflow 定義。
- Schema 修正與內容退回可共用 budget；Provider auto retry 屬同 attempt 的另一層，不消耗 workflow retry budget。
- Feedback 必須落地，含退回原因、來源 attempt/error code 及可選 committed refs；新 request 明確帶入，不只靠 session 記憶或畫面摘要。
- Reviewer verdict 必須對應本輪具體 Ref，後續只使用這一組通過驗收的輸出，不搜尋「最新」檔案。
- `FailFast` 取消 siblings 但仍 join，保留原始根因；`CollectAll` 返回宣告順序的每個結果，workflow 必須處理必要分支失敗。
- Nested retry 有各自 activation／epoch／budget；可重跑祖先結束前，invocation 結果是 provisional，不提前定案。
- 不自動重送 completion 不明、pipe 斷線或 ack timeout 的 prompt，不承諾 exactly-once。

## 6. Runtime、hub 與完成判斷

- Controller 自己啟動 `pi --mode rpc`，保留 persisted session 與既有 WebUI extension／hub discovery，不重建另一套 hub 整合。
- Controller 是執行期間唯一派工來源，使用者只觀看。人工 prompt、steer/follow-up、Stop、model/thinking/session 切換不列必要相容性或驗收。
- 此前提不要求修改或禁用 hub 的既有功能；對話／進度觀看、discovery 與 history 保留仍在範圍內。
- RPC `prompt` ack 只表示接受，不表示完成；`agent_end`、silence 或 artifact 存在也不足夠。
- 完成必須有本次 token 的 prompt／entry evidence、合格 terminal outcome、`agent_settled`、不變的 session/model/thinking binding 及 Stage 後 Confirm；無法證明歸屬就失敗，不猜測。
- 已觀測的 abort／retry cancellation／compaction failure 不得被無法證明是恢復的後續成功覆蓋。Controller 自身取消、timeout、SIGINT／SIGTERM 仍是必要處理範圍。
- `get_entries` 是記憶體觀測，不是磁碟持久化證明；session JSONL 寫檔仍由 Pi 負責。Workflow 可對個別 Step 開啟稽核觀測：entries 落地成 run 自有檔案與 tool call 索引；timeout／失敗時 Close 前 best-effort 再讀一次，Close 確認退出後讀 session JSONL 補齊並以 entry id 去重，取不到只標記覆蓋缺失，不蓋過原錯誤、不阻塞 cleanup。
- 啟動 persisted Pi 可能觸發共享 discovery recovery。操作前須唯讀確認父 `pid` 與 `.recovering`，可能影響他人 session 時停止，不代清理。Preflight 不是鎖，獨立 task 不是 discovery 隔離。

## 7. 狀態、錯誤及收尾

- Run、scope、invocation、attempt、session 狀態與失敗原因必須明確表示；attempt 終態最多提交一次。
- 錯誤使用 typed cause/origin、root/collateral provenance 及本次 dispatch acceptance，不靠字串或 error chain 深度猜因果。
- StorageFailed、JournalFailed、run-level LimitExceeded 為 sticky run-fatal；workflow 吞錯不能繼續成功。一般可處理 branch error 不一律升為 run failure。
- 成功鎖定前須完成 final Ref 驗收、必要結果落地及 deadline 仲裁；outcome 鎖定後的取消或 cleanup deadline 不追溯改寫業務 outcome。
- Cleanup 必須使用獨立期限，取消與停止 process 不等待被堵塞的 journal 或 renderer。Finalization failures 分開記錄，未持久化狀態明示 `StatePersisted=false`。
- 成功、失敗、timeout、明確取消及可處理 signals 都清理本次自有 Pi；partial startup 也不能遺漏已啟動的 child。
- 先 bounded abort／abort_bash，再 SIGKILL 自有 process group、唯一 Wait；只移除 ownership 匹配的 discovery，不掃殺其他 session。
- 未確認 process exit 不釋放 live slot；自有 workspace 清理須等待 workflow join 及 Pi exit。`AddCleanup` 註冊前／失敗後仍由 acquisition caller 負責收尾。
- 不刪 contracts、證據、報告或 Pi history。Run outcome、cleanup 與持久化是否成功分開表示；任何必要收尾失敗不能 exit 0，取消仍保留 130／143 並列出所有錯誤。

## 8. TUI 與最終交付

- 顯示各 Pi 的 Online/Offline/Unresponsive 文字與色彩、model、scope/step、spinner、耗時、retry 計數／上限及最新 feedback；平行 steps 各自呈現。
- TUI 只消費 engine Snapshot／Report，不派工、不直接讀 candidate。慢 consumer 不阻塞 RPC 或核心狀態。
- 外部文字須移除 terminal control sequences；feedback 可以摘要，但原文保留在 request/event，完整錯誤不可因摘要而遺失。
- TTY 支援捲動、`q`／Ctrl+C 取消並等待最終 Report。Input ownership、writer error interception、bounded presentation queue 與 join/restore 不得依賴 renderer 才登記取消。
- Workflow 以 `FinalSelection` 明確選擇 output key／可選 artifact ID；engine 以 producer attempt 建立 `FinalDelivery`，保存 `result.json.final` 並提供給 formatter。
- 不從 session 關閉時間推測最後節點。沒有合法 final 就顯示 unavailable；合法 incomplete report 不把 failed run 變成 success。
- Process 關閉後仍可取得交付，不要求 resume。人工 Pi resume/fork 是後續對話操作，不是 Controller crash resume，詳見 [README](README.md)。

## 9. 保留、隱私與非保證範圍

Repository 只留通用需求／設計／操作文件及可重現匿名 tests。真實 PR/ticket、execution reports、traces、history、認證資訊、個人路徑與執行 metadata 只留 repo 外受限位置，不以交接或驗收為理由納入 commit。

已知 numeric validator 限制不取消 JSON/schema/identity/file quota 等既有要求：極端 candidate 數值可能超出依賴可表示性，否定／條件規則可誤接受；巨大數值與錯誤樹亦可能放大資源且無法協作取消。完整 snapshot 重寫會放大 I/O，journal quota 不是總 I/O quota。具體機制見 [DESIGN](DESIGN.md)。

不承諾 supervisor、Controller abrupt death 後的全數清理、任意 detached 子孫、sandbox、同 UID 惡意防護、exactly-once、外部副作用 rollback、Controller crash resume、斷電後每個 directory entry／history 的 durability。永久堵塞 filesystem/stdout 或不遵守 context 的任意 Go callback 不保證固定 wall-clock 返回。測試與相容性宣稱必須按組別、平台和版本區分，skip／blocked 不算通過。
