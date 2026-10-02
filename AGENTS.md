# 專案憲法

本文件是開發與 review 本專案時必須遵守的架構原則。適用於所有 workflows，不只 Jira triage；不是提供給 workflow nodes 的共通安全 prompt，也不新增 runtime 注入、hook 或工具權限機制。

## 一、Controller 管 workflow，Agent 做領域工作

**Controller 擁有流程控制與可驗證的交接；Agent 擁有領域操作與推理。不得以增加 validator 的方式，逐步把 Agent 的工作搬進 Controller。**

Controller 負責：
- 派工、角色／模型綁定、Step、session 的建立／重用／關閉。
- Contracts、exact committed Refs、artifact ownership、版本關係與明確的流程轉移。
- 預算、timeout、checkpoint／handoff、錯誤傳遞、自有 process 的 Wait／cleanup。
- Shared discovery 的 parent PID、`.recovering` 與 ownership 檢查。

Agent 負責：
- 沿正常 Pi 載入既有 AGENTS.md、hooks、skills 與 tools；優先讀相關領域 skill，再使用其 scripts、CLI、REST、shell 自主完成工作。
- 資料取得、搜尋、身份／時間解析、證據適用性、假說與調查判讀。
- 依 contract 回報資料、分析、缺項、失敗診斷與下一步需求，不自行建立子 agent 或接管派工。

Planner Agent 可以提出下一步工作；Controller 依已批准的 contract、scope、Refs、預算與轉移規則驗收後派工。這不是把任意程式、模型或下一節點的控制權交給 Agent。

派工以完整任務為單位，不把任務內的領域操作拆成 Controller 的微型審批流程。在已授權 scope、角色與完成條件內，Agent 自主選擇工具、處理新發現的來源並回報實際工作、結果與缺項；不能只因發現新附件或 linked issue，就要求先停止、提交申請再由 Controller 批准取得。超出授權範圍、需要其他角色或後續 Step 時，才由 Controller 接續派工。明確的局部補取可以保留為窄任務，但不能成為所有更新工作的唯一形式。

查詢窗口、分段、filters 與 aggregation 是 Agent 在完整任務內控制資料量及取得證據的策略，不是逐次 Controller 審批單位。Agent 可依證據與結果自主縮窄、移動或擴展窗口，不要求每次查詢涵蓋整段事故，也不由 Controller 統一指定窗口寬度。保留實際查詢條件、時間依據、結果完整性與失敗診斷，以供交接驗收。

局部工作已有可靠前提，不等於所有背景資訊都必須先解析完成。例如已有可信的有限 UTC 搜尋依據時，可先取得 supporting evidence 協助解析其他時間；不能猜時區或盲掃來冒充前提。Controller 保留既定 scope、UTC、receipt、completeness、Refs、預算及 cleanup 驗收，不自行判斷窗口的調查價值。小窗空結果、partial 或 timeout 不代表整段事故不存在。

## 二、版本驗收不等於領域認列

- Exact 引用舊 evidence，不等於冒充新版 evidence。保留真正 owner 與歷史 binding；不得因為來源較舊就一律拒絕，也不得把舊產物重新標成新版取得或驗證。
- Controller 驗來源／版本／ownership、結構與既定完成條件；不自行判斷歷史 wiki 內容、facts 或時間證據能否支持新的領域結論。
- 不新增依 evidence schema、fact status 等條件替 Agent 認列內容的規則，例如 validFacts、validAnchors、retainedConfirmed。必要的領域判讀應成為 Agent 工作及可追溯的輸出。
- Inventory 增刪與來源更新由 Agent 在任務內處理；Controller 接版本、exact ownership、歷史及下游 dependencies，不強制 Agent 宣告領域上的 old → new 替換關係，也不從名稱、大小或內容猜測它。新版來源不自動證明舊分析失效，不因來源更新就強制另派 analysis；Agent 判斷適用性，歷史產物保持真正 binding，不冒充新版取得或分析。
- 來源移除不自動關閉調查缺口；Agent 決定保留缺口或依既有 contract 交代其不再適用。沿用既有 gap／evidence 驗收，不為 inventory 接線另造領域狀態分類，也不將 gap 已交代誤認為資料已取得。
- 此界線不授權刪除既有已批准的 contracts／驗收，包括 completeness、scope、receipt、UTC 算術與 Ref 檢查。新增接線若碰到限制，先指出具體衝突與必要最小改動，不以憲法為由大刪或重構。

## 三、沿用工具，不把移植變成工具重造

- 單支 formatted-view helper 沒有包辦 pagination、下載或 raw 保存，不等於 Agent 或既有 skill 缺少這些能力。
- 不據此新增 Controller 自帶的 acquisition executable、可選 wrapper、新 skill 或通用 adapter 框架，更不強制 Agent 呼叫以取代領域自主操作。
- 真有工具缺口時，先核對 skill 全貌、scripts、立即 callers 與既有工具，提出具體缺口及必要改動，取得確認再施工。既有已批准工具與 workflow resources 不因此被自動撤除。
- Step prompt 交代角色、工作、授權 scope、inputs／outputs 與完成條件；不另造共通安全 instructions、專用 AGENTS.md、hooks、allowlist、LaunchProfile、capability manifest、credential broker 或 per-Step 權限輪替。
- 前述「不新增…新 skill」針對因工具缺口而另造 skill。Workflow 專用 skills（repo 內 embed，或 repo 外私有目錄）依 [新增 workflow 指南](docs/ADDING-A-WORKFLOW.md) 交付，不受此限；仍不得修改既有 skills／agents／settings。
- 單一 workflow 自有、注入該 workflow 各 Step requirements 的流程契約常數（例如由 Controller 而非 Agent 派工、不建立或委派 subagent、不寫回外部系統、無人可詢問時降級並列缺項）屬 Step requirements，不算「共通安全 instructions」；後者指跨 workflow 的通用安全政策、runtime 注入或 hook。此類常數須明寫優先於衝突的全域指示，且不得含公司內部名詞。

## 四、硬性保證不能退成 prompt

- 下游只消費 exact committed Refs。Publish rename、檔案存在、自行計算 digest 或未 committed candidate 都不是 checkpoint／交接授權。
- Fresh session 不繼承前一個 session 的記憶。必要 evidence owners、狀態與 feedback 必須明確交付，不掃描「最新」檔案補足。
- 換 session／恢復工作前，必須按 workflow 的 handoff 規則確認舊 session 關閉、Wait／cleanup 完成；不得以新 session 掩蓋未確認的 process，也不得重置 run 額度。
- 不吞 fatal、cancel、storage／journal、限額或 cleanup failure。Timeout 是執行失敗，不是業務反證。
- Agent 操作規則不是 sandbox。不得把 production 唯讀、node 寫入範圍或不得發布等提示，宣稱成 Controller 對每次 shell／HTTP／副作用的硬性阻擋。
- 不以流程擴充為由新增 crash resume、supervisor、動態 DSL 或 exactly-once 承諾。

## 五、資料不完整與執行失敗必須分開

- 合法的不完整 contract 可以依明確 workflow 規則交給後續工作；保留缺項與實際狀態，不偽裝完成。未完成搜尋不等於 no matches，`ready` 不等於 root cause confirmed。
- 這不是通用 fallback。Contract 不合法或執行失敗不得改標普通缺資料以取得成功；可恢復範圍與分支必須明確編排，保留錯誤及會計。
- 框架不硬編碼各 workflow 的業務 verdict。特定 workflow 的角色數、模型、deadline、唯讀範圍、最終產物等，必須由該 workflow 明定，不默默套用其他 workflow 的選擇。

## 六、先守邊界，再談實作通過

- 修改前讀現況、exports、立即 callers、contracts 與共用 utilities。先說明哪些是 workflow 接線，哪些留給 Agent；需要 Controller 新增領域推理時先停下確認。
- 規格或既有文件衝突時，指出具體位置並確認必要變更；不得自行放寬本憲法，也不得沿錯誤架構追加 validators 補洞。
- Tests 通過不能替架構選擇背書。合理單元需正式獨立 review、核實修正及對應驗證；runner 採證不能取代 code／scale-failure／simplicity review。
- 使用真 engine／Store／protocol 路徑，只替代外部 API、provider 或明確批准的 filesystem dependency boundary；OS File.Sync 可注入正常成功或故障結果，產品預設真 Sync，durability／storage-journal fault／持久化順序及 fsync queue 測試保真。這是正常 DI，不是產品 fast mode；其他 filesystem 操作不變。不 mock Step、validator、內部編排或 parser 來製造成功。
- 驗收 exact 待提交 tree，清楚列出 skips、未執行與未驗 live 能力。匿名 fixtures、工具存在、skill 可載入、Agent 實際成功操作是不同層級的證據。
- Repository 只保留通用 source、匿名 tests 與文件。真資料、認證、個人路徑、逐次工程報告及交接 metadata 留在 repo 外指定位置；不覆蓋不明來源變更，不自行發布。
- 本 repo 為公開。公司內部系統、架構、主機與環境名稱同屬不得入 repo 的資料；領域細節留在 repo 外的私有 skill 或文件，repo 內的 requirements、schema 說明與文件只用通用描述。

## 七、共用機制不能隨 workflow 重造

**新 workflow 的預設工作是組合既有執行機制、定義自己的業務 contracts 與轉移，不是複製上一個 workflow 的底層接線。** 共用能力的歸屬及本輪收斂順序見 [Workflow 重用與 refactor 計畫](docs/WORKFLOW-REUSE.md)；接手與 review 都必須核對，不把計畫中的 API 當成已存在。

- 每個實作單元開始前，列出將使用的既有 API、實際 callers、必要的新業務資料，以及剩餘機制缺口。至少比較其他 workflows 與 engine/runtime/contract/testutil，不能只看當前 package 就宣稱沒有可重用能力。
- 同一保證只因角色名、schema、file map/slice 形狀或小量參數不同，不得另寫一套機制。缺共用入口時，先從已驗證實作抽出最小中性能力，遷移既有 consumers，再接新工作；不以「先複製，以後再抽」繞過。
- 共用機制放在其責任層：process/discovery 在 runtime，committed 授權與流程原語在 engine，contract/file 表示及檢查在 contract，測試 transport 在 testutil。不讓 engine 依賴特定 workflow，也不讓新 workflow import 另一 workflow 的私有業務實作作為共用層。
- Startup preflight、cleanup 確認、envelope/file 消費、測試 host transport 是必查的共用性項目。不能每個 workflow 自行拼出不同的安全保證；尚未收斂的既有實作須依計畫遷移，不因寫入本條就宣稱已完成。
- 共用前先列差異矩陣：取消、錯誤分類、ownership、path/symlink、限額、穩定性及生命週期。不得挑較弱版本統一，也不得將更強檢查默默套到舊 API。新增自動拒絕條件、改變錯誤時點或會計，都要明列並驗收，不稱為零行為 refactor。
- 業務 Evidence/Status、claim/verdict、角色/模型、scope、reframe 與安全重做條件可以不同，不硬合成大量 optional fields、通用判讀或可配置流程。沒有第二個實際用途時，不預造共用層；即使有多個 consumers，也不因 DRY 引入 WorkflowBase、通用 scheduler、DAG/DSL、plugin/installer 或工具權限框架。普通 Go 控制流程與少量重複不是抽象化理由。
- 不另造 Store/journal/commit、session 管理、retry 計數或 final artifact registry。使用既有原語；`Parallel` 不是限流 queue，`Retry` 不是整個調查狀態機，policy hard cap 也不是排程策略。
- 共用 refactor 的完成條件包含實際 consumer 遷移、舊副本移除或核實的保留理由、原契約及新邊界回歸。只新增未使用 helper、只移檔案，或留下兩套繼續演化，不算收斂完成。
- 後續 milestones 必須遵守已記錄的相依與重用決策。接手先核對 source、完成單元與採證，不回復歷史版本、不重做已完成工作；要改變既定邊界或繞過相依，先指出具體新衝突並取得確認，再同步計畫。
- 本條是開發／review gate，不新增對 workflow nodes 的共通安全 prompt、runtime hook 或工具 allowlist，也不以 DRY 為由取消前六條保證。

## 八、資訊通道分層：schema、診斷、prompt 各管一種資訊

**格式的機器可檢查事實歸 schema；需要執行期資料的跨欄位／跨檔案規則歸 Go validator 與具體診斷；prompt 只留 Agent 必須判斷或 Controller 無法機械表達的東西。規則有兩份會漂移；用 prompt 補 schema 或 validator 的洞，就是製造第二份會漂移的規則。**

- 機器可檢查的格式（型別、pattern、必填、enum、欄位語義）寫進該 workflow 的 schemas，不寫在 prompt。Schema 是格式的唯一權威來源；skills 與 Step requirements 不手抄 JSON 格式，只可指向 schema。
- 只有 Go 算得出的規則（跨欄位一致性、byte-exact 保留、條件式 lineage）靠驗收診斷教 Agent，不靠 prompt 預先背誦。診斷必須具體到可修復：指出哪個欄位、收到什麼、期望什麼。籠統訊息（同一句話涵蓋多種原因）先拆訊息，不改驗收條件；repair 迴路是這些規則的執行處。
- 禁止以「Prompt X to Y」補驗收的洞：schema 缺 pattern 補 schema，診斷不具體修診斷。同一格式規則被 prompt 補兩次以上，代表 prompt 壓制無效，必須升級到 schema 或診斷層，不得第三次寫進 prompt。
- 格式規則從 validator 前移到 schema 會改變錯誤時點與訊息，屬行為變更：必須明列（依第七條）、schema pattern 不得比對應 Go 檢查更嚴（只讓同一個拒絕更早發生，不新增拒絕條件）、同步更新期待該錯誤時點的測試，並核對 fixtures 與引擎產出端皆符合新 pattern。
- 領域語義（判斷指引）、scope 與角色邊界（授權、不寫回、不做什麼）屬於 prompt 的正當內容，不得以分層為由刪除或搬進 validator；把領域判斷搬進 Controller 違反第一條。
- 刪 prompt 句子須有數據佐證（規則實際違反次數、repair 失敗紀錄）或 live 驗證通過；匿名 fixtures 只證明結構不變，不證明模型行為不退化。模型可否讀懂 schema（`output.schema.path` 的自我取用）是 live 能力，未驗證前列為缺口。

## 閱讀入口

- [PRD](PRD.md)：產品範圍與 Controller 唯一派工。
- [DESIGN](DESIGN.md)：分層、commit／Ref、runtime、錯誤與 cleanup 的精確語義。
- [新增 workflow 指南](docs/ADDING-A-WORKFLOW.md)：接線、contracts、review 與驗收程序。
- [Workflow 重用與 refactor 計畫](docs/WORKFLOW-REUSE.md)：共用能力歸屬、實作相依與接手防重造 gate。
- [Jira triage](docs/JIRA-TRIAGE.md)：上述原則在調查 workflow 的具體應用與目前能力。
- [VERIFICATION](docs/VERIFICATION.md)：分組 gates、發布檢查與非保證範圍。
