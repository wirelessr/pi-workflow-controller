# Validation 方法

Validate 是驗證與彙整 stage，不是第四位 reviewer，不補跑失敗 reviewer，也不新開 review role。

1. 從 request.prompt 讀 Controller 列出的 expected reviewers。必須恰好 code、scale、simplicity 各一 row，所有提供的 role、status、完整 ref（包含 null）、detail 精確複製到 reviewers，不能根據磁碟檔案、候選檔或自己的分析猜 succeeded，也不能把 failed 改 missing。允許 status 只有 succeeded、failed、missing；ref nullable 依 Controller 指示原樣保留，不自己造 Ref。若 Controller 清單缺列、重複或與 inputs 矛盾，停止並回報阻擋，不猜缺漏 row。
2. 只讀 Controller 指定的 Prepared 與成功 reviewer 的 published inputs；failed/missing 即使有旁邊 candidate 也不消費。核對 Pin、context Ref、role、coverage、requirements ID、findings ID 前綴及 head locations。必要 input 不可讀時回報 Controller，不能無視錯誤或自行改 Controller row。Prepared 的 open questions、source 缺失／衝突與各 reviewer limitations 都要保留在最終限制。
3. 為每個成功 reviewer 輸入的 finding 恰好建立一項 disposition，不能只處理最後想報告的問題。重新讀 pinned head 的實際 code、caller/callee 與需求來源，不只信 agent 引用。每項 action 僅 confirmed、merged、excluded、unconfirmed，reason 必須具體說明驗證依據或缺失：
   - confirmed：獨立驗證成立，放入 final findings，保留原 reviewer 的穩定 finding ID；target_id 指向這個 final finding ID。
   - merged：確認是同一缺陷及根因／觸發／影響才合併，選一個穩定 surviving reviewer finding ID 作 final ID，target_id 指向該已確認 finding，保留所有來源 ID 與合併理由。不能只因同檔案／同標題就合併，不能指向另一個 merged／excluded ID。
   - excluded：head code 或規格已證明該主張不成立／不適用，說清楚反證，target_id 必須空字串。
   - unconfirmed：必要證據缺失、source 衝突、依賴版本／load 不明，不能確認也不能排除；target_id 必須空字串，不列 final findings，原因加入 limitations。不得默默刪除，或假稱「已修好」。
4. final findings 只能包含 independently confirmed 的缺陷或證據充分的 simplicity 問題，location/evidence 全部指 pinned head code。不能新增不屬於三位 reviewer 輸入的第四角色 finding；驗證時看到另一個疑點，列限制，不擴張成新 review pass。合併保留清楚的根因、影響與證據，不提高 severity 以掩蓋不確定性。
5. 對 Prepared 每個 requirement、constraint、decision ID 恰好給一項最終 assessment。原始 statement 不可縮小或改寫成較容易成立的子命題：證明 fallback 存在，不等於證明所有被丟棄 rows 均可 fallback；證明 diff 沒改 writer，不等於核實外部 writer fix 已修復根因。遇到「所有／不變／已修復」等量化或外部事實，缺少涵蓋原命題的證據即保持 unconfirmed。若將 Code 的 unconfirmed 改為 satisfied，reason 必須逐一消除 Code 原本的未釐清前提，不能一面承認相同前提未確認、一面標 satisfied。讀來源 statement 與 pinned head，參考 code reviewer 矩陣及其他角色補充，但獨立核對；不同 reviewer 判斷衝突要解釋，不能多數決。status 僅 satisfied、not_satisfied、unconfirmed。前兩者提供 head code evidence；unconfirmed 說明缺什麼並加入 limitations。找不到 code 本身不是 not_satisfied 的充分證據。不能省略 unknown 或重新編 ID。
6. 計算 completeness，採最嚴重的狀態：
   - 任一必要 reviewer failed 或 missing，一律 incomplete。必要輸入／角色 coverage 缺漏亦不能 complete；無法形成可追溯的必要矩陣時回報阻擋或 incomplete，不能偽造完整結果。
   - 三位皆成功，但有 missing/conflicting source、open question、claim-only 規格限制、requirement unconfirmed、reviewer limitation、unconfirmed finding、未覆蓋 dependency／檔案或其他證據限制，至少 limited。不能因自己認為不重要就清空原始限制。
   - 只有三位皆成功、需求皆可判定、每項 finding 已有可解釋處置且上述限制全無，才可 complete。complete 僅表示本固定三角色任務完整，不代表程式整體正確。
7. 計算 conclusion：有任何 confirmed final finding 就是 findings（即使 incomplete 仍須明示限制）；沒有 confirmed finding 且 completeness=complete 才是 no_confirmed_findings；沒有 confirmed finding 且 limited 或 incomplete 一律 undetermined，不能把不完整的 review 說成沒有問題或建議無條件 approve。
8. 先依 registry 寫完整 Validated candidate，精確保留所有原始 Ref、需求判定、findings、dispositions 與限制。report_file 先填 `review-report`，但不要手寫 Markdown，也不要預先建立 `artifacts/review-report.md` 或在 files 登記此 ID/path；其餘 evidence/files 原樣保留。完成 JSON 後，在同一次 Step 明確執行一次 `python3 <absoluteScript> <absoluteRequest> <absoluteCandidate>`。absoluteScript 是本 SKILL.md 所在目錄下 `scripts/render_report.py` 的絕對路徑（本 reference 所在目錄的 `../scripts/render_report.py`），另兩個參數是 Controller 指定、同一 attempt 內的 request.json 與 candidate.json 絕對路徑，不依賴 cwd。腳本僅讀 request 指定的 exact published inputs，核對 digest／identity，確定性生成繁中分節報告、完整原始值及結構化追溯附錄，新增 report artifact files 條目並更新 candidate。這是 Controller 自有的 renderer，不是執行被審專案的 scripts，也不代替本 stage 的內容驗證。
9. 腳本成功後不再修改 candidate 或報告，簡短回報產物位置並交還 Controller，完成同一 Step；失敗就回報阻擋，不重試、不手寫替代報告、不開另一 stage。報告明示固定三 reviewer 唯讀分析，沒有執行被審專案 tests/build/compile/deploy、OCR 或 fallback，不宣稱 local correctness coverage，不把 no_confirmed_findings 翻成「程式正確／測試通過」。不提交評論、不 approve、不寫 wiki。
