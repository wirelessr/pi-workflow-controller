# Code change workflow 規劃（草案）

**狀態：規劃。** P0（第 9 節的憲法修訂與重用登記）已落地；C1 拆為 C1a 與 C1b，C1a（Controller 前置與自有 worktree）、C1b（A1、本機 receipt、gap 出口、註冊）、C2a（A2 發 PR 前審閱）與 C2b（A3 發佈、PR receipt 核對、失敗時外部物件記錄、成功時移除 worktree；目前到此為止）已落地；其餘單元依第 8 節的順序施工。`code-change` 只是暫用名稱。

Workflow 以一份已核可的 design doc 為輸入，由 Controller 派工，經實作、發 PR 前審閱、發 PR、PR 後深度審閱、自行 triage 修正、收斂 PR，到最後清理，交付一個已開出的 PR。這是第一個會寫本機 repository 並對外部系統產生副作用的 workflow；現有 workflows 都只讀或只寫自有 artifacts。

## 1. 範圍與非目標

- 不討論 scope，也不產生 design doc。輸入必須已核可；缺漏由 Agent 回報，不由 Controller 判斷內容好壞。
- Controller 不判斷任務型態（feature、bug fix、refactor）。入口只有一個，改前證據與驗收方式由 design doc 的內容與 Agent 的原則決定。
- 不含 crash resume、動態 DSL、通用 scheduler、exactly-once 或外部副作用 rollback。
- Merge 不屬於本 workflow，交付終點是「PR 已收斂或明列未收斂項目」。
- 公司內部的 skills、掃描服務、專案與主機名稱不入 repo。收斂 skill 的私有版放 repo 外，路徑於執行期由環境變數提供，不寫入 repo 內 source（第 2 節）。git 流程 skill 是一般 skill，由 Pi 正常載入。

## 2. 已確認決策

| 項目 | 決定 |
|---|---|
| 入口 | 已核可的 design doc；單一入口，不分任務型態。單行 Prompt 為 `<owner/name> <design doc 檔案路徑>`：第一個 token 是目標 repo，其餘是路徑 |
| 工作區 | Controller 建立並擁有 worktree，註冊 cleanup；Agent 只在其中做 git 操作。未成功結束時保留：cleanup callback 讀取 workflow 設定的旗標，旗標預設為保留，僅在 workflow 確定成功後才設為移除，不改 engine。workflow 返回之後的 finalization 失敗仍可能使 worktree 被移除而終態為 Failed，這是承認的殘餘窗口：成功時移除前已核對：session 嚴格關閉後重讀的 HEAD 等於 PR 上已核對的 head；未 commit 與 untracked 的檔案以及該 worktree 的 reflog 會隨移除消失，bare repository（本機 branch 與 objects）保留；脫離 process group 的 Agent 子孫不在保證內。run 目錄的報告保留 |
| Session | 預設每個 Step 都是 fresh session；只有「更正」（帶 Feedback 回到原角色）才沿用同一 handle |
| 模型 | 重 `fireworks` GLM-5p3、中 GLM-5p3-flash、輕 deepseek flash。code-review 預設模型改為 GLM-5p3／GLM-5p3-flash（第 7 節） |
| PR | 不開 draft。發 PR 前先有一個審閱 Step：基本 code review 與 design 對照 code 的一致性檢查，各由獨立 Agent 執行 |
| PR 後 review | 重用 code-review 流程，由 Controller 派工；先做重用抽取 refactor |
| 發 PR 前的審閱 | A3 只做一次更正即發 PR，不再審閱；把關交給 PR 後的三方會審 |
| 模型家族 | review 全部使用 GLM 家族（重 GLM-5p3、中 GLM-5p3-flash），接受跨家族多樣性下降 |
| 自己的 PR 審閱 | 不貼 comment，由實作端自行 triage 並修正 |
| 收斂迴圈 | Controller 跑迴圈：每輪「等待、快照、fresh Agent 一輪」，輪數上限是硬保證；Agent 不自行 loop 或等待 |
| 私有 skill | 比照 triage：環境變數提供 repo 外私有目錄，展開到 run 內並記錄 digest。收斂 skill 的私有版只做一輪，不含 loop、等待與詢問使用者；原 command 不動，兩邊不自動同步。git 流程 skill 是一般 skill，Pi 正常載入，不搬 |
| 品質掃描 | 現況靠瀏覽器 MCP，需人工登入。無登入 session 就跳過該項並明列，不當成通過 |
| PR 不關閉 | 流程失敗時保留已開出的 PR 與 branch，不自動關閉或刪除 |

模型 ID 以 source 現有宣告為準：`glm-5p3`、`glm-5p3-flash`、`deepseek-v4p1-flash`（均為 `accounts/fireworks/models/` 前綴，triage 已使用）。deepseek 已確認為輕層模型，目前提案中的 code-change 角色只有 E 使用它。

## 3. 流程

```
[Controller]  驗入口、repo 存取與 push 權限（同一次 GET）、Node TLS 驗證未被停用、建立 worktree、註冊 cleanup；均在任何副作用之前
[A1 implement]   重：讀 design doc，判斷有無實作 gap。無 gap 則依 git 流程實作並本機 commit；有 gap 則停
                 輸出：單一 contract，readiness 為 ready 或 gaps（附實作報告與 local head，或 gap 清單）
[A2 pre-PR review]  兩個獨立 fresh Agent 平行（CollectAll，兩者必要）
                 - basic：對本機 diff 做基本 code review
                 - consistency：design doc 對 code 的一致性檢查
                 輸出：各自 findings Ref
[A3 correct+publish]  沿用 A1 的 handle（Feedback 帶兩份 findings Refs）：自行 triage、修正、push、開 PR、ticket 狀態轉移；結束後關閉該 handle
                 輸出：PR receipt（URL、branch、head SHA）
[Controller]  以 gh GET 核對 receipt，核對過的 head 作為後續 exact head
[B deep review]  Controller 派 code-review 流程（Prepare、三 reviewer、Validate），針對該 head
                 輸出：validation report Ref
[C triage+fix]   fresh：讀 report Ref，自行 triage（修或不修並留理由）、修正、comment 清理、push
                 輸出：處置報告 + push receipt；Controller 核對
[D converge]     Controller 迴圈（輪數上限，初值 10；每輪在新 Child scope `round-NNN` 下）：
                 1. 等待：PR 的 checks 與 commit statuses 全部進入終態，再加有界的靜置窗口；整體等待有上限
                 2. 快照：Controller 以唯讀查詢記錄 head SHA、各 check／status 的名稱、狀態、連結，未解 thread 的 ID、位置、連結，經 Attach 成為 committed Ref
                 3. fresh Agent 一輪（私有 skill）：讀快照與上一輪報告，triage、修正、reply／resolve、push，並自行查最新狀態
                 輸出：本輪報告（逐項狀態、是否宣稱已收斂、外部寫入 receipt）；Controller 核對
                 結束：Agent 宣稱已收斂且本輪沒有任何外部寫入 receipt，或輪數用盡，或某輪執行失敗
[E final cleanup]  fresh：最後一次 comment 清理；輸出 push receipt；Controller 核對
[Controller]  最後一次快照（套用同一個有界等待），與各階段報告一併成為 final summary（見第 5 節）
```

- A1、A2 的審閱對象是本機尚未發佈的 diff，因此不能直接用 B 的流程；B 需要既存的 PR。
- A3 是更正（回到同一個實作者）加發佈，沒有再審閱的迴圈。更嚴謹的把關在 B。
- B 只在 A3 回報並經 Controller 核對的 head 上跑一次，以該 SHA 為預期值傳入（規則同下方 receipt 核對：相等或為 PR 當下 head 的祖先）。若 PR 當下 head 已較新（例如 bot 推了 commit），B 仍審 A3 的 head，Final 明示 B 未涵蓋較新的 commit。C、D、E 的改動不再被深度審閱。
- E 獨立成 Step，是為了讓 D 未收斂或某輪逾時時仍做最後清理。E 只在 D 已開始之後才執行；A3 之前的失敗、A3 的 repair 用盡、B 的 `incomplete`、C 的失敗都直接終止，不進 E（PR 保留）。進入 E 的條件：D 最後開過的 session 的 `CloseSessionReport` 已確認關閉且 Wait 完成（未確認就不進 E；D 從未開過 session 則無此條件）。Run deadline、取消、fatal，以及身分或 receipt 核對失敗都不進 E（身分已對不上的 PR 不再寫入）。E 結束後仍回傳 D 的原始結果或錯誤。

### Controller 在 D 的職責與界線

Controller 可以：依機械定義的狀態條件等待（只看是否進入終態，不看結論）；記錄外部狀態的快照；核對身分與 receipt。Controller 不判定 check 的意義（綠、flaky、是否必要），不 rerun CI，不因快照內容拒收 Agent 的「已收斂」宣稱。

- **身分核對屬第四條，不屬「不拒收」。** Controller 核對 Agent 回報的 push head 須同時滿足：等於自有 worktree 的 HEAD；確實是新 commit（不是上次已核對 head 的祖先或相同者）；為 PR 當下 head 或其祖先（以 compare 判定，容許 bot 在其間推 commit）。任一不符是具名的執行失敗，不重試，PR 保留，診斷指出欄位、收到的值與期望。這是跨來源的 Go validator，不是 prompt 規則。核對在 repair 迴圈接受最終 contract 之後執行一次，不放進 repair 的 `Validate`：只有 schema 與內容驗收會 repair，身分與 receipt 不符不會。「上次已核對的 head」只在核對通過後更新，所以 attempt 因其他欄位被打回、repair 後回報同一個 head，不會被誤判為非新 commit。沒有新 commit 的 Step（例如 C 決定都不修、E 無事可清）以明示的 `no_change` 回報，Controller 核對回報的 head 與上次已核對者相同，且仍為 PR 當下 head 或其祖先（容許 bot 推 commit）。A3 被拒 attempt 的 PR receipt 不核對，只核對最終 contract；失敗時的外部物件記錄只在 context 仍有效時做（取消、run deadline、fatal、run-scope 額度用盡時做不到，是承認的缺口）。
- **終止是機械條件。** 迴圈只在三者同時成立時終止：Agent 宣稱已收斂；本輪沒有任何外部寫入 receipt（push、CI rerun、thread reply／resolve）；Controller 在本輪結束後以 GET 取得的 PR head 等於本輪開始時快照的 head。Agent 不等待，看不到自己寫入所觸發的結果，所以有寫入的那一輪不採信為終止，下一輪的快照涵蓋該寫入；未回報的 push 由 head 比對攔下。這是依 receipt 與 head 這些機械事實做的轉移，不看快照內容的意義。輪數用盡前沒有符合條件的收斂宣稱，即未收斂。
- **「已收斂」的宣稱不涵蓋 skipped 或 not_checked 的項目**，Final 逐項列出；是否值得為它們停下，由 Agent 判斷。D 宣稱收斂之後 E 又 push 時，Final 表述為「收斂宣稱對應 head X，最終 head Y 未經收斂輪」。
- **殘餘風險：** 終止輪中 Agent 未回報的 reply、resolve 或 CI rerun，head 比對攔不到，承認為未被機械核對的範圍；CI rerun 的 run 列表只用於呈現，不判斷其意義。
- **並列顯示：** Final 同時呈現 Agent 的宣稱與最後一份快照，不一致時顯示而不拒收。快照中未解 thread 的用途是下一輪的輸入與 Final 的呈現，Controller 自己沒有用到它。
- **快照只放識別與狀態，不放 log 或留言全文；** Agent 用既有 gh skill 自取。快照在 push 後即過期，Agent 仍須自行查最新狀態。
- **每輪都派 Agent，** 即使快照看似無事：「無事」是否等於收斂是詮釋，品質掃描項也不在快照內。
- **等待逾時：** 記為快照欄位，不視為 Agent 的執行失敗，該輪照常派工並計入輪數；輪數用盡時成為未收斂。
- 已知缺口：剛 push 時 checks 可能尚未註冊；review bot 的留言不反映在 check 狀態。靜置窗口長度須 live 驗證，目前不承諾數值。
- 迴圈是普通 Go 迴圈加每輪新 Child，不用 `Retry`（其用盡錯誤是 `RetryExhausted`，語義不同）。

### Session、CWD 與角色

| Step | 角色 | Session | CWD | 模型層級（提案） |
|---|---|---|---|---|
| A1、A3 | 實作者 | 同一 handle（A3 是更正） | `PWC_PI_CWD`（部署預設），以絕對路徑操作 worktree | 重 |
| A2 basic | 審閱者 | fresh | `PWC_PI_CWD`（同 A1），以絕對路徑讀 Controller 建立的釘選 snapshot | 中 |
| A2 consistency | 審閱者 | fresh | 同 A2 basic | 重 |
| B | 見 code-review | 各自 fresh | 沿用 review（checkout 外） | 見第 7 節 |
| C | 修正者 | fresh | 同 A1 | 重 |
| D 每輪 | 收斂者 | fresh | 同 A1 | 中 |
| E | 清理者 | fresh | 同 A1 | 輕 |

層級是提案。審閱角色與寫入角色都不把 CWD 放在 worktree 或使用者的日常 checkout，而是沿用既有的部署預設 `PWC_PI_CWD`（目前 triage 使用的同一個目錄，其下即全域規則所指的相鄰 repos 所在處），不另設 `Role.CWD` 的目錄概念；C1b 把同一個值明設為 A1 的 `Role.CWD`，使檢查的值與實際使用的值相同。理由：(1) 操作者的全域規則依「CWD 位於該目錄之下」才觸發跨 repo 的相鄰探索；(2) 目標 repo 自己的 `AGENTS.md`／`CLAUDE.md`、project skills 與 extensions 不會被載入，規則只來自全域與本 workflow 的 embedded／私有 skills；(3) 若 CWD 是使用者的日常 checkout，相對路徑的編輯會改到真實 repo 而不是 worktree，所以不用。Agent 以絕對路徑讀寫 worktree，並用 `git -C` 或在命令內 `cd` 操作；其他相鄰 repo 只讀。`PWC_PI_CWD` 未設定時 fallback 是啟動 Controller 時的目錄，可能就是某個真實 checkout，所以 code-change 在任何 Pi 啟動之前檢查它已設定、為絕對路徑且為既有目錄，否則以輸入錯誤終止。它不是使用者日常 checkout 這個前提只由部署者保證，Controller 不檢查它是否位於某個 git work tree，這是承認的殘餘風險。

### 失敗與不完整的區分

- 設計文件 gap：視為資料缺失，屬合法的不完整。沿 ADDING 指南第 7 節（合法 incomplete report 可搭配非零 execution outcome）：workflow 同時回傳 final gap 報告與具名錯誤，終態為 Failed（exit 1），靠 final 區分「缺項」與「執行失敗」。不開 PR，不偽裝完成。
- 逾時是 TimedOut（exit 1），與 Failed 分開；取消另有其狀態。engine 沒有「非零成功」狀態。
- A2 任一必要審閱者失敗：執行失敗，不改標為資料不足，不發 PR。兩位都失敗時，錯誤分類取 basic 的，consistency 的失敗只保留在訊息中。
- B 的 `limited`（來源缺失等）：合法，原樣交給 C。B 的 `incomplete`（必要 reviewer 失敗）：執行失敗，PR 保留，流程終止，不進 C。
- D：輪數用盡仍無合格的收斂宣稱，即未收斂，依同一先例交付 final 並以具名錯誤結束。某輪逾時或失敗是執行失敗，不當成「已收斂」，也不由 Controller 自行重跑該輪。
- 檢查項狀態只有四種：`passed`、`failed`、`skipped`、`not_checked`。`skipped` 只用於 schema enum 預先列舉的項目 ID，且需交代前提（例如沒有登入 session），集合外的值在 schema 層即被拒。`not_checked` 表示檢查因其他原因未能執行，必須附原因。兩者都不算通過，也不併入已驗證結論。這不是通用 fallback。
- 寫入型 Step 的輸出不合 contract：沿用 triage 已有的 contract repair（同一 handle、有限預算、Controller 的具體診斷作為 Feedback），其共用機械由 R9 抽成 `engine.Scope.StepWithRepair`。Repair 只重寫本 attempt 的報告，不重做外部寫入，這條只寫在 engine 的 repair 派送（`StepSpec.RepairKeepsWork`，寫入型 Step 設定；對模型行為的效果未經 live 驗證），不在 Step requirements 重複；receipt 核對只對最終 contract 執行一次，被拒 attempt 的本機 head 不核對（本機 commit 只有一個 worktree HEAD，最終核對已涵蓋）：它攔得下回報的 head 與 worktree HEAD 不一致（謊報），攔不下 repair 中重做的寫入（新 push 產生新 commit，回報仍會一致），也攔不下重複的 thread reply 或重開的 PR，這些是承認的殘餘風險；前幾次被拒的 attempt 若有外部副作用，其 receipt 只有在最終 contract 承載時才會被核對，A3 被拒發布中的 PR receipt 是否核對，由 C2b 決定。engine 預設的 repair 派送文字要求 Agent 先執行 request.prompt 的任務，與「repair 只重寫報告」相衝突；設定 `RepairKeepsWork` 的 Step 改用不重做任務與寫入的開頭句，其模型行為尚待 live 驗證，驗證前重做寫入仍列為殘餘風險。含外部寫入的 Step（A3 起；A1 只做本機 commit，不適用）的任何失敗（repair 預算用盡、逾時、身分不符等）都是執行失敗；此時 Controller 從自有 worktree 讀出目前 branch，再以唯讀 GET 查對應的 PR，在失敗報告中交代已存在的外部物件，不自動撤銷。

## 4. 輸入、權限與授權範圍

CLI 仍是 `run <workflow> "單行 Prompt"`，沒有 flags 或外部 config（ADDING 指南的既有限制）。因此目標 repo 作為 Prompt 的一部分。

Controller 在任何網路呼叫之前依序檢查：Prompt 格式（第一段為 `owner/name`，其餘為路徑）、`PWC_PI_CWD` 已設定、為絕對路徑且指向既有目錄（可為 symlink）、Node TLS 驗證未停用、design doc 可讀；git 支援 `--no-lazy-fetch`（2.45 以上，Controller 讀 worktree 狀態的 receipt 需要，缺少時以具名錯誤在建立 worktree 之前拒絕）。Design doc 路徑為絕對路徑時照用，相對路徑依啟動 Controller 的目錄做字面解析（先 Clean 再開啟），不展開 `~`；上層目錄可以是 symlink，檔案本身必須是 regular file、不得是 symlink、至多 1 MiB，讀取期間不得變動。Repo GET 回傳的 `full_name` 與輸入不分大小寫不同即拒絕（含改名與轉移，不追隨新名稱）；`permissions.push` 缺少或不為 true 即拒絕（這是 gh 認證身分在該 repo 的角色權限，不證明 Agent 之後使用的認證也能 push）；不檢查 archived；`default_branch` 是否為合法 ref 由 fetch 前的檢查判定，所以非法值會在 bare repository 建立之後才被拒絕。以上失敗都發生在建立 worktree 之前或由 Controller 自行移除半成品。

Prompt 格式：

```
<owner/name> <design doc 檔案路徑>
```

第一個 token 是 GitHub repo，其餘整段（去頭尾空白）是路徑，讓路徑可含空白。Controller 解析這一行，格式錯誤是輸入錯誤，在任何 Pi 啟動前終止，不由模型猜測。Design doc 不需要有固定格式的欄位，內容完全由 Agent 讀取。

- Controller 讀取 design doc 後經 Attach 保存為 snapshot 的 exact Ref，下游只消費該 Ref。
- Ticket key 預期由 design doc 載明，由 A1 讀取。缺少時 A1 回報缺項並終止，因為 branch 命名與 commit 格式依賴它。Controller 不從文字猜測 ticket。
- Base branch 不由使用者提供：Controller 以 gh GET 取得該 repo 的預設 branch，並以此建立 worktree 與開 PR。同一次 GET 驗證 repo 存在、可存取，以及認證身分對該 repo 有 push 權限（依回傳的權限欄位）。因此不支援對非預設 branch（release branch、stacked PR）開工，列為非目標。
- 啟動前無法確認的權限（例如 ticket 系統）逐項註明，不假裝已確認。

授權的寫入類別與核對方式。Controller 只做唯讀核對，不新增外部系統的 client；無法核對者由 Agent 在報告中記錄：

| 類別 | 執行者 | receipt | Controller 唯讀核對 |
|---|---|---|---|
| 本機 commit | A1、A3、C、D、E | commit SHA | 能，讀自有 worktree |
| push branch（只限承載本工作的一個 branch；不推 tag、其他 branch，不改 default branch） | A3、C、D、E | 回報的 head | 部分：核對回報的 head 是 PR 當下 head 或其祖先；不核對「只推一個 branch、不推 tag 與 default branch」這些範圍限制，因為 default branch 與其他 ref 也會被其他人或自動化推送，無法機械歸因給 Agent |
| 開 PR（只含建立時的 title、body、base、head；label、reviewer、assignee 與 comment 不在授權內；不開 draft） | A3 | URL、number、branch、head | 能，GET（身分、open、非 draft、base 為 input 記錄的 default branch、head repo 與 branch 一致、回報的 head 為 PR 當下 head 或其祖先）；label、reviewer、assignee、comment 同樣不核對（repo 自動化或 CODEOWNERS 也會加，無法歸因給 Agent） |
| thread reply／resolve | C、D | thread ID 與動作 | 能，讀 thread 狀態與留言列表 |
| ticket 狀態轉移 | A3（沿 git 流程 skill） | ticket key 與目標狀態 | 不能，Controller 不新增 ticket 系統 client；記於 Agent 報告 |
| CI rerun | D | run ID | 能讀 run 列表，但不判斷其意義 |
| 品質掃描瀏覽器操作 | D | 無 | 只讀，不適用 |

這些是 Agent 操作規則，不是 sandbox。Controller 不宣稱阻擋每一次 shell 或 HTTP 副作用。

### 寫入型 worktree 政策（初值）

- Remote 名稱 `origin`，URL 為目標 repo 的 https 位址，不含憑證；bare repository 加 `blob:none` 的 promisor partial fetch，與 review 的做法一致。fetch 的對象是 default branch tip（review 只 fetch 指定 SHA，這是政策差異，列入 R7 矩陣）。
- Controller 自己的 git 呼叫停用 system／global config 與 attributes、`core.hooksPath=/dev/null`，並帶與 review 相同的傳輸與 checkout 保護（不跟隨 redirect、protocol 白名單、不遞迴 submodule、不 sparse、不轉換行尾），所以使用者的 hooks、URL 改寫與 filters 不在 Controller 建立 worktree 時執行；共用的隔離環境來自 runtime。Controller 不寫入任何 git config 的 credential、url、hooks 或 identity 設定，也不把 credential helper 寫入 config，它自己的 git 呼叫經 `runtime.Git` 逐次以 `-c` 帶入 gh 的 helper，不落地。Agent 的 git 照常使用使用者的全域 git config（含 identity），push 與開 PR 使用 Agent 既有的認證。
- Controller 設定 `origin` 的一般 tracking refspec 與 `origin/HEAD`，使 bare＋worktree 對 Agent 的 git 流程與一般 clone 相同；worktree 以 detached 方式建立於 fetch 當下的 default branch tip（記錄於 input 的 `base_sha`）。交給 Agent 之後，repository 的 config 由 Agent 寫入，Controller 之後對該 repository 的 git 只執行不讀可執行 config 的 plumbing；唯一的例外是 A2 的 snapshot：Controller 另建全新的自有 bare repository（config 由 Controller 控制、沒有 remote），以 alternates 借用 A1 repository 的 objects，再 detached checkout 已核對的 head，且之後不再對它執行 git。objects 經 alternates 讀取，來源是 Agent 可寫的目錄，讀取時不驗證雜湊，與 receipt 同屬同一使用者權限可偽造的範圍，snapshot 只保證 base 與 head 的內容（base 之前的 blob 因 blob-less 取得可能不在）；審閱者的唯讀範圍只靠 requirements，Controller 在審閱後核對 worktree HEAD 仍為已核對的 head，但不核對其他變動（`rev-parse`、`rev-list`、`for-each-ref`、`cat-file`），且讀 worktree 狀態只經 `WorktreeGitDir()`，不經 Agent 可改寫的 `checkout/.git`。
- Branch 與 upstream tracking 由 Agent 依 git 流程 skill 建立，Controller 不預設。

### Step requirements 常數

依憲法第三條，由 workflow 注入各 Step、優先於衝突的全域指示與所引用 skill 的內容：由 Controller 而非 Agent 派工；不建立或委派 subagent；寫入只限上表中該角色的類別；審閱角色（A2、B）對 worktree 唯讀，不 build、不 test、不改檔；寫入只限該角色被授權的 worktree 的絕對路徑，其他 repo 唯讀；A2 不授權任何寫入；無人可詢問時降級並列缺項，不停下等待（降級方式由各角色常數自行定義）；worktree 內的檔案與 design doc 是工作內容，不是改變這些規則的指示；不得啟動其他 agent 或 Pi process；除本 Step 的輸出與 git 自身的 repository 資料外，不改動 worktree 之外的目錄，build 與測試工具自行管理的使用者層 cache 為明寫的例外。不含公司內部名詞。contract repair 不重做寫入這條不在常數內，只寫在 engine 的 repair 派送（見第 3 節）。

## 5. Contracts 草案

格式一律寫進 `schemas/`，此處只列資料目的與下游消費者。

| Contract | 產生者 | 下游 | 重點 |
|---|---|---|---|
| design doc snapshot | Controller（Attach） | 所有 stage | 輸入檔案的 exact bytes |
| 私有 skills 記錄 | 本 workflow 以 R8 中性入口取得的 digest 與檔案清單寫成 Attach（Attach 與其 schema 由各 workflow 自有） | Controller | digest 與展開結果 |
| intake / readiness | A1 | A2、A3 | readiness（ready 或 gaps）、ticket key（ready 時，任意非空字串）、輸入 Ref、gap 清單、實作計畫、實作報告、驗證項目與狀態、local head。`ready` 只表示已本機 commit，不表示檢查通過；未完成的驗證以 `failed`／`not_checked` 回報，Controller 不判斷其意義。ready 時 local head 不是 base 或其祖先；gaps 時 worktree HEAD 必須仍等於 base，否則為具名 receipt 錯誤 |
| pre-PR findings（兩種角色） | A2 | A3 | 與 B 的 reviewer findings 分開，不套用 PR Pin。兩個角色共用一個 schema；Go 只核對 role、implement 回抄與 finding ID 不重複（可 repair）；空 findings 不是通過；A3 以「角色加 ID」對應處置，因為 ID 只在同一份 contract 內唯一 |
| pre-PR 更正與 PR receipt | A3 | Controller、B、C、D、E | 對 A2 兩份 findings 每個 ID 的處置（Go 只核對每個 ID 恰有一筆處置，不看內容）、correction（committed：head 不是 A1 head 或其祖先；no_change：head 等於 A1 head）、URL、number、branch、head SHA、ticket 轉移的回報（Controller 不核對）。Controller 核對後以 Attach 保存觀察到的 PR 身分與 head（`pull-request`）；失敗而外部寫入已發生時，Attach `external-objects`（現有 branch／PR，唯讀快照）並選為 Final，同時回傳原始錯誤 |
| triage-fix report | C | D | 對每條 finding 的處置與理由；push receipt 與其 head，Controller 核對 |
| PR 狀態快照 | Controller（Attach） | D 每輪、Final | head SHA、checks／statuses 與未解 threads 的識別、狀態、連結、等待是否逾時；不含內容全文，不含判斷 |
| converge round report | D 每輪 | 下一輪、E | 逐項狀態、是否宣稱已收斂、未收斂原因；各外部寫入的 receipt 與 head，可核對者由 Controller 核對 |
| cleanup report | E | Final | push receipt、最終 head，Controller 核對 |
| final summary | Controller（Attach） | 使用者 | 精確引用各階段最後一份報告與最後一份快照，各自標明對應的 head，與最終 head 不同時明示；逐項列出 skipped 與 not_checked；並列 Agent 宣稱與快照 |

`FinalSelection` 指向 final summary。Summary 以 exact Ref 引用各報告，並只逐字抽取固定欄位（Agent 的收斂宣稱與逐項狀態；快照的 head SHA、各 check 的名稱與狀態、未解 thread 的 ID），不改寫、不補充；完整內容讀被引用的 Ref。

## 6. 重用 map（初版，開工前需重新核對 source）

| 能力 | 現有位置 | 決定 |
|---|---|---|
| Run、Step、Parallel、Retry、Decision、CloseSession | `engine/scope.go`、`step.go`、`session.go`、`run.go` | 直接重用；A3 的更正沿用 Feedback 與同一 handle |
| Stage/Confirm/Publish、committed resolver、Decode/ReadContract | `engine/`、`contract/` | 直接重用 |
| Cleanup 註冊 | `Run.AddCleanup` | 直接重用；worktree 為自有資源 |
| Shared-discovery preflight、strict close | `runtime/` | 沿用，不另寫 |
| Controller 產出資料的 committed Ref | `Scope.Attach`（`engine/attach.go`） | 直接重用：design doc、快照、final summary；每次計入 total attempts，key 與 Step 共用命名空間 |
| 本 workflow 專屬 embedded skills | `reportresource.ExtractFresh` | 直接重用 |
| 私有 skill 目錄展開 | `internal/contract/skilldir`（R8；triage 經 `PrepareSkills` 呼叫） | R8 抽出中性的讀取與展開入口（角色集合、環境變數、Attach 與 schema 由各 workflow 宣告）。上游比對與缺口紀錄留在 triage；code-change 依第 2 節只展開並記錄 digest，不做上游比對（私有版由原 command 衍生、兩邊不自動同步），日後若要偵測過期再於 C4 評估 |
| gh／git 命令執行 | `runtime.GitHubGET`、`runtime.Git`（R7 已抽出） | 直接重用；PR 狀態 adapter 的 REST 查詢走 `GitHubGET`，GraphQL 於 C4 新增固定入口 |
| bare repository 保留、init／remote／pinned fetch、SameFile cleanup | `internal/gitrepo`（R7 已抽出） | 直接重用；可寫 worktree 的 default branch fetch 已於 C1a 以 `Repo.FetchBranch` 加入（review 不呼叫） |
| GitHub PR 狀態讀取（check runs、commit statuses、review threads） | 無 | 缺口：新的唯讀 adapter，底層 gh 執行共用 R7 |
| 可寫 worktree | `internal/workflows/codechange`（`createWorktree`） | C1a 已實作：run 內 bare repository 加 `FetchBranch`、detached worktree；註冊前失敗自行移除，註冊後預設保留，只有 workflow 設定移除旗標時才清理（C2b 起設定）；憑證由 Agent 既有 gh／git 認證處理，Controller 不寫入憑證相關 config |
| 測試 host transport | `testutil/protocol`；gh／git 的 PATH wrapper 為 `testutil/pathexec`（runtime、review、code-change 共用） | 直接重用 |
| Contract repair 迴圈 | `engine.Scope.StepWithRepair`（`engine/step.go`，R9 已抽出；triage 經 `TaskSession.Run` 呼叫，`repairBudget = 2` 留在 triage） | 已抽成 `engine.Scope.StepWithRepair`（R9），triage 已遷移；本 workflow 的寫入型 Step 直接使用，不複製。Feedback 的 Refs 在 repair 時保留 |
| Capacity handoff | triage package | 不需要：D 每輪 fresh session，context 以輪切分 |

## 7. 重用抽取：code-review 成為可組合單元

R6 之前，`review.execute` 是單一函式，且包含以下綁定，直接呼叫會違反憲法第七條，也無法放進較大的 Run。下表「現況」欄為 R6 前的狀態，已實作的接線見 [WORKFLOW-REUSE](WORKFLOW-REUSE.md) R6：

| 面向 | 現況 | 抽取後須明列的變化 |
|---|---|---|
| 輸入 | `in.Prompt` 即 PR URL，由 `ParsePRURL` 解析 | 參數 `prURL`；`code-review` 入口傳入 `in.Prompt`，單元開頭依序檢查前綴、URL、TLS，巢狀呼叫者傳錯也在副作用前被拒 |
| 目錄與資源名稱 | 固定 `r.Dir()/review`、`review-work`、skills 目錄 `review-skills`、cleanup 名稱 `review-checkout`；重複會 Mkdir 失敗或被拒為重名資源 | 以呼叫者提供的前綴命名，使其能與外層資源共存；前綴以保守字元集在任何副作用前檢查，不得逃出 run 目錄，也不得以 `-skills`、`-work` 結尾（避免推導名稱在兩次呼叫之間互撞） |
| Scope 名稱 | Step key `prepare`、`validate`、Parallel `reviewers`、Decision `context-accepted`／`report-accepted` 在 `r.Root()`；`review` 在各 reviewer 的 branch scope。Decision、Child、Parallel、Retry 共用一組名稱；Step 與 Attach 共用另一組 | 單元放在呼叫者傳入的專用 scope 下（`code-review` 傳 `r.Root()`，scope path 不變），避免與外層兩組名稱衝突 |
| 預期 head | acquire 每次自行取得當下 PR head，checkout、diff、Pin、ContextID 與每階段 `Verify` 都綁在這個值上 | 預期 head X 的參數與核對（相等或為 PR 當下 head Y 的祖先，新增一次唯讀 compare 查詢；X 不等於 Y 時 checkout、diff、Pin 與 ContextID 釘 X，metadata snapshot 保留 Y，兩者不同成為 review 的明列限制）屬 code-change 才需要的政策分支，在 C3 加入，R6 不預造；C3 會連帶修改單元簽名與 `code-review` 的一行呼叫 |
| 限額 | Definition 內 Run policy：3 live、5 sessions、5 attempts、120 分鐘 | 組合後由外層 workflow 的 policy 承擔，須重新核算 |
| 前置檢查 | `NODE_TLS_REJECT_UNAUTHORIZED` 檢查在 `execute` 開頭，任何副作用之前，錯誤字串寫死 `code-review blocked` | 抽成 `runtime.CheckNodeTLS`（中性檢查函式，不自動套用到每次 Pi 啟動），review 單元在原位置呼叫（`ParsePRURL` 之後、任何副作用之前）；code-change 在 A1 之前呼叫同一函式，不得等到 B 才檢查（那時 PR 已開出）。這樣 code-change 不 import review 的 helper。錯誤訊息改為不綁 workflow 名稱，屬明列的字串變更 |
| 結果 | 回傳 `Outputs`、`Final`，並可同時回傳 branch error | 單元需把「report 與 branch error 並存」的語義明確交給呼叫者 |
| 模型 | 內部 `model(role)` 寫死 | 維持單元內常數，由 M-model 單元變更；呼叫者不傳入 |
| 取消與 cleanup | `AddCleanup` 註冊 checkout；validate session 從未由 workflow 關閉 | checkout 所有權不變。單元在成功與 incomplete 的返回路徑關閉 validate session，使巢狀呼叫者不被留下一個 live slot；對 `code-review` 是明列的行為變更：關閉失敗由「Succeeded、交付 Final 加 cleanup error」變為 workflow error（Failed，不交付 Final，report Ref 仍保留在 Outputs；exit code 同為 1）。incomplete 路徑關閉失敗時，原本交付的 incomplete report 也不再交付，失敗分類維持 reviewer 的原始錯誤。Final 為 nil 時單元可能留下最多 3 個 live session，呼叫者須結束它的 Run |
| path／symlink、錯誤分類、穩定性、生命週期 | root 驗證、保留路徑與 SameFile cleanup 在 `internal/gitrepo`；checkout 與 snapshot 的 rooted no-follow 讀取和錯誤分類在 `check.go`、`safety.go`、`acquire.go` | 呼叫者提供的前綴不得放寬既有路徑檢查；錯誤碼、phase 與 cleanup 時點逐項對照，不變 |

抽取是行為保持的 refactor：code-review 自己必須遷移到新單元，既有 workflow 與測試的契約不變，舊副本移除。這裡的「可重入」指單元能放在外層 scope 下與外層名稱共存；同一 Run 內也可以不同前綴、各自專用的 scope 多次呼叫，同一前綴不得重複或並行呼叫。改變錯誤時點、會計或拒絕條件之處（例如預期 head 的核對）須明列並驗收。

**為什麼不做 engine 層的 sub-workflow 原語。** 現在 workflow 的 `Execute` 就是普通的 Go 函式，組合已經可行：問題不在引擎不能呼叫，而在 review 沒有被設計成可被呼叫的單元，簽名綁死 `Input` 全文、`Root()` 與固定名稱。若加一個引擎層的「呼叫已註冊 Definition」原語，仍然要處理同樣的輸入參數化、命名空間、限額會計與結果映射，所以參數化抽取是無論如何都要做的前置。抽取完成後，組合就是一次普通的 Go 函式呼叫，沒有新增框架概念。是否再把這個慣例提升為引擎原語，等出現第三個組合者再依憲法第七條評估。

**模型變更是獨立單元。** 把 review 預設模型改為 GLM-5p3 與 GLM-5p3-flash，是產品行為變更，不是零行為 refactor。須列出：

- 目前三位 reviewer 跨三個模型家族；改配置會讓這個差異消失。
- 需更新 `docs/CODE-REVIEW.md` 的模型表。目前沒有測試釘住 review 的模型 binding，所以要新增這類測試；模型品質與 schema 遵循的 live 驗證另列。
- 已確認：Prepare 用 GLM-5p3-flash、Code 用 GLM-5p3、Scale/Failure 用 GLM-5p3、Simplicity 用 GLM-5p3-flash、Validation 用 GLM-5p3。review 因此全部落在 GLM 家族，三位 reviewer 不再跨模型家族，獨立性只靠各自的審閱視角與 session 隔離。這是對先前「跨家族多樣性」的取捨，已接受。thinking 層級沿現有各角色設定，變更前須確認該模型支援；品質與 schema 遵循須 live 驗證，未驗證前不宣稱與舊配置等價。

## 8. 施工單元與相依

順序依相依排列，每個單元可獨立 review、驗收，不合併成一次大重構。

| 單元 | 內容 | 相依 |
|---|---|---|
| P0 | 憲法修訂與 `WORKFLOW-REUSE.md` 登記更新（只改文件，文字待確認） | 無 |
| R7 | gh／git 命令執行與 bare repository 機械的共用：核對 `acquire.go`，列差異矩陣；process 執行放 runtime，bare repository 與 worktree 的 ownership 放另一個中性 package；review 遷移；consumers 包含 review、PR 狀態 adapter 與新 worktree | P0 |
| R6 | 抽出 review 流程為可組合單元，遷移 code-review 自身，完成差異矩陣回歸 | P0、R7（兩者都改 `acquire.go`，R7 先） |
| R9 | contract repair 迴圈的中性化：由 `triage.TaskSession.Run` 抽出不綁 triage 角色名與 recovery 型別的核心，triage 遷移 | P0 |
| R8 | 私有 skill 目錄展開中性化：由 `triage.PrepareSkills` 抽出不綁角色名的入口，triage 遷移 | P0 |
| M-model | 更換 review 預設模型與文件、測試 | R6 |
| C1a | `code-change` 骨架的 Controller 前置：Prompt 解析、`PWC_PI_CWD` 與 TLS 檢查、design doc snapshot、repo GET（identity 與 push 權限）、`gitrepo.FetchBranch`、可寫 worktree 與預設保留的 cleanup、`input` Attach；不啟動 Pi、不註冊，暫時以 `input` 為 Final | P0、R7 |
| C1b | A1、receipt 核對（含只經 `WorktreeGitDir()` 讀 worktree HEAD）、gap 出口、repair 派送、registry／CLI 註冊（含 `Definitions` 的 `piCWD` 接線，A1 的 `Role.CWD` 明設為同一個值，並於任何副作用前確認它是目錄） | C1a、R9 |
| C2a | A2：Controller 建立釘選在 A1 head 的唯讀 snapshot（全新的自有 bare repository，以 alternates 借用 A1 repository 的 objects，再 detached checkout；不在 Agent 改過 config 的 repository 上 checkout，以免執行其 filter），兩位 fresh 審閱者平行（CollectAll）讀 snapshot，共用一份 findings schema；兩位都 strict close 之後移除 snapshot；終點仍是 `Final: implement`，不 push | C1b |
| C2b | A3 沿用 A1 的 handle（Feedback 帶兩份 findings Refs、`RepairKeepsWork`）：更正、push、開 PR、ticket 轉移；PR receipt 核對與失敗時的外部物件快照；關閉 handle 後重讀 HEAD 並確認等於已 push 的 head，才在最終成功返回處設定 worktree 移除旗標；code-change 的 `Policy.Runtime.CleanupTimeout` 設為 2 分鐘（待 live 修正）。須明定：「上次已核對 head」只在核對通過時更新；A3 失敗時（外部寫入已發生）Controller 以唯讀快照交代已存在的 branch／PR | C2a |
| C3 | 接 B，並加上 C 的 triage 與修正。含 B 的預期 head 參數與核對（第 7 節，連帶修改 `review.Run` 簽名與 `code-review` 的呼叫）；呼叫 `review.Run` 時外層不得有 live session，且須把單元回傳的 Outputs 以自己的名稱重映射再併入；Final 為 nil 時結束 Run | C2b、R6 |
| C4 | D 的 Controller 迴圈、PR 狀態 adapter、每輪私有 skill、E、品質掃描 skipped 語義 | C3、R7、R8 |
| C5 | 實際整合驗收（需一次性測試 repo 與憑證，留在 repo 外；ticket 轉移見第 11 節） | C4、M-model |

共用單元（R6 至 R9）只交付政策無關的核心與原 consumer 的遷移；新 consumer 才需要的政策分支（可寫 worktree 的 fetch、GraphQL 查詢、寫入型 Step 的 receipt 核對）在第一個使用它的 C 單元落地並於該處驗收，不在共用單元預造。

每個單元開工前補完整的 reuse map（既有 API、實際 callers、新業務資料、剩餘缺口），並檢查其他 workflows 與 engine/runtime/contract/testutil，不只看當前 package。

## 9. 憲法修訂案（待確認）

現行憲法預設調查與唯讀，對「會寫入並產生外部副作用」的 workflow 沒有明文。實際修訂文字見 `AGENTS.md` 的 diff，涵蓋：

1. 第一條：自有工作區的所有權與 cleanup；寫入型 workflow 沿用同樣分工，授權範圍逐類明列。
2. 第二條：Controller 可只讀取得外部狀態的快照，並可依機械定義的狀態條件等待，但不判定其意義；對意義的宣稱不拒收，身分與 receipt 不符依第四條處理。
3. 第三條：寫入型 workflow 明列允許的寫入類別；Step 可引用 repo 外既有 skill／command，常數優先於衝突步驟。
4. 第四條：不可回滾副作用的 receipt 成為 committed Ref，可核對者須核對；不默認重試安全，失敗時保留外部物件。
5. 第五條：合法的 skipped 檢查項。
6. 第六條：寫入型 workflow 的 live 驗收只用一次性測試目標。
7. 第七條：workflow 組合另一 workflow 時的界線，不新增引擎層原語。

## 10. 風險與目前不承諾的事

- **收斂迴圈的等待。** checks／statuses 終態判定與 bot 靜置窗口都是經驗值，須 live 驗證；剛 push 時 checks 可能尚未註冊，bot 留言不在 check 狀態內。只涵蓋 check runs 與 commit statuses，其他 CI 回報來源列為已知限制。
- **跨輪記憶。** 每輪 fresh，上一輪處理過什麼只靠上一輪報告傳遞；私有 skill 須有「修同類全部、不只修被點名者」之類的原則來避免來回。
- **Controller 範圍增加。** Controller 新增 GitHub 唯讀 adapter，含分頁、rate limit 與認證處理；它只讀不判斷，但仍是新增的維護面。
- **Run 限額與期限。** sessions 至多約 20（A 1、A2 2、B 5、C 1、D 至多 10、E 1）。每個 Step 至多 3 個 attempts（首次加 2 次 contract repair，同一 handle，不增加 session）；provider auto retry 在同一 attempt 內，不消耗額度。A2 期間 A1 handle 存活加兩個審閱者共 3 個 live；A3 結束後關閉 A1 handle，B 與 D 期間不超過 3 個。
  - **額度約束：** Steps（約 21，每個至多 3 個 attempts）加 Attach（design doc 與 skills 記錄各 1、快照至多 D 輪數加 1、final summary 1）的最壞情況必須 ≤ `MaxTotalAttempts`；`MaxTotalSessions` 同理。run 範圍的 `LimitExceeded` 是 fatal，fatal 不解析 Final，所以額度要為 E、最後快照與 final summary 預留，即使 D 跑滿輪數。
  - **時間約束：以 deadline 感知的准入，只保證 D 開始之後。** 每輪開始前由 workflow 以 `Run.Snapshot` 的 `CreatedAt` 加 `Policy.RunTimeout` 算出 deadline 與剩餘時間（`DisableRunTimeout` 為真則不准入）。剩餘時間小於「單輪上界加尾段預留」時不再開新一輪，D 以未收斂結束。單輪上界 = 等待上限 + Controller 操作上限（快照查詢與 Attach 包在同一個期限 context 內，初值 10 分鐘）+ 一個 Step 上限。尾段預留 = E 的 Step 上限 + 最後快照的等待上限與靜置窗口 + final summary 的 Attach（10 分鐘）+ 餘量 20 分鐘。初值：單輪上界 70 分鐘，尾段預留 83 分鐘，剩餘不足 153 分鐘即不開新一輪。
  - **為何不乘 repair 倍數：** 沿用 triage 的取捨，為每個可能的 repair 預留完整 Step 上限會讓輪次在 deadline 前數小時就停止。代價是 repair 疊加時 E 與 final 仍可能被截斷，因此保證是有條件的，不是絕對的。
  - **範圍：** D 之前的階段沒有准入；deadline 若在 D 之前觸發，終態為 TimedOut，不進 E 也不產生 final summary。
- **A3 一次更正。** A2 找到問題後只做一次修正，沒有再審閱，已確認由 B 把關。
- **多樣性下降。** 見第 7 節。
- **本機 receipt 的範圍。** 它防的是錯誤與謊報，不防擁有同樣使用者權限的 Agent 偽造（registration 目錄、shallow、grafts 皆可被改寫）；「新 commit」只表示 HEAD 不是 base 或其祖先，不證明是 Agent 所建，也不證明是 base 的後代。
- **未經 live 驗證的 Agent 行為。** repair 派送的新開頭句、A1 常數與 schema 的遵循度、`--no-lazy-fetch` 對 lazy fetch 的實際效果。
- **只模擬或單元測試無法證明的事：** 模型能否遵循 schema、私有 skill 的單輪版能否在無人狀態下完成、品質掃描 skip 的實際行為、bot 靜置窗口、Agent 在 CWD 之外操作 worktree 時，各工具（git、build、test、專案內的相對路徑設定）的實際行為。皆列為 live 缺口。
- **公開 repo。** Repo 內的 requirements 與文件只用通用描述；私有 skill 路徑與內容留在 repo 外。
- **Prepare 的耗時與磁碟。** Controller 前置（GET、fetch、worktree add）各有 20 分鐘 deadline，最壞約一小時以上，不在准入範圍。每個 run 在 run 目錄永久保留含 default branch tip 全部 blob 的 bare repository，非成功路徑還保留整個 worktree，沒有大小上限也沒有磁碟 preflight。成功路徑移除大型 worktree 與 session 關閉共用 15 秒 cleanup 預算，逾時會被記為 cleanup failure；設定移除旗標的單元（C2）須決定處理方式，不默默改 engine 的 timeout。移除旗標只在 workflow 確定成功的最終返回處設定；之後 engine 仍可能因取消或 run deadline 判為 Cancelled／TimedOut 並移除 worktree，這是承認的殘餘窗口。 2 分鐘的 `CleanupTimeout` 是 deadline 而不是移除的上限：`RemoveAll` 無法中斷，超時或權限錯誤時 checkout 可能部分或全部已刪、registration 殘留，結果是 Succeeded 但 exit 1。A3 若被 run deadline 截斷，已發生的外部寫入不會有記錄。A1 與 A3 共用 handle，A3 的 context 容量未經 live 驗證。

## 11. 初值與待核對

下列初值由規劃者暫定，之後遇到問題再調整，不視為最終：

| 項目 | 初值 | 備註 |
|---|---|---|
| D 輪數上限 | 10 | 沿用現有收斂 command 的預算 |
| D 每輪等待上限／靜置窗口 | 30 分鐘／3 分鐘 | 靜置窗口是占位值，須 live 驗證 |
| D 每輪 Step 上限 | 30 分鐘 | |
| A1／A2（每位）／A3 | 90／30／45 分鐘 | |
| C／E | 45／20 分鐘 | |
| B | 沿用 review 現有：Prepare 20、各 reviewer 30、Validate 30 分鐘 | |
| RunTimeout | 8 小時 | 預設 4 小時不足以容納跑滿輪數的常見情況；由准入規則保證 E 與 final |
| MaxLiveSessions／MaxTotalSessions／MaxTotalAttempts | 3／22／80 | 最壞：可 repair 的 16 個 Step ×3，加 B 的 5 個 Step ×1（review 無 repair），加 Attach 16（含 A3 失敗時 `pull-request` 與 `external-objects` 可能並存），共 69；80 為保守值 |
| thinking | code-change 自己的角色沿 triage 慣例：GLM 用 high，deepseek flash 用 off（B 的角色維持各自現有設定，見第 7 節） | 變更前須確認該模型支援 |
| D 未收斂的終態 | 比照 gap：交付 final 並以具名錯誤結束（Failed、exit 1） | |
| 等待逾時 | 記為快照欄位，照常派該輪並計入輪數 | |

待核對、尚未決定：

1. **CWD 上層鏈與工具行為的核對。** 已查：`PWC_PI_CWD` 目前使用的目錄及其上層（到 `/Users`）沒有 `AGENTS.md`、`CLAUDE.md`、`AGENTS.override.md` 或 `.agents`，只有家目錄的 `.pi`（全域 agent 目錄）。C1 前再核對一次部署實際值，並實測 Agent 在 CWD 之外操作 worktree 時，git、build、test 與依 CWD 找設定檔的工具是否順暢，以及全域的相鄰探索規則確實觸發。以上依文件與檔案系統現況推論，未經 live 驗證。
2. **live 驗收中的 ticket 轉移。** 納入，但需一張專用測試 ticket 指派給操作者，在其上依序轉換狀態；C5 前建立，不使用真實 ticket。
