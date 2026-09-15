---
name: pwc-review-prepare
description: 只在 Controller 的 review prepare attempt 使用；從 pinned snapshots 與 linked 規格整理可追溯 context 和穩定需求，不執行 reviewer 或調度。
---

# PWC Review prepare

本 skill 僅執行 Controller 指派的 prepare attempt，不是完整 PR workflow。

開始任何工作前，完整讀取本 SKILL.md，再完整讀取下列兩份 reference；若工具輸出截斷，繼續讀到檔案結尾。reference 的相對路徑以本 SKILL.md 所在目錄解析，不是 cwd。缺少任一必要 reference 就停止，不用舊全域 workflow 代替。

1. [當次 attempt 契約與安全邊界](../common/contract.md)
2. [prepare 方法與完成條件](references/method.md)

輸出格式只以 request.output 的 registry schema 為權威。完成本角色後交還 Controller，不自行 spawn、補角色或建立下一 stage。
