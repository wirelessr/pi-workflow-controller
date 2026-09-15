---
name: pwc-review-simplicity
description: 只在 Controller 的 simplicity reviewer attempt 使用；驗證可刪除或重用的複雜度，保護安全與規格保證，只產生有 pinned evidence 的分析。
---

# PWC Review simplicity

本 skill 僅執行 Controller 指派的 simplicity attempt，不是完整 PR workflow。

開始任何工作前，完整讀取本 SKILL.md，再完整讀取下列兩份 reference；若工具輸出截斷，繼續讀到檔案結尾。reference 的相對路徑以本 SKILL.md 所在目錄解析，不是 cwd。缺少任一必要 reference 就停止，不用舊全域 workflow 代替。

1. [當次 attempt 契約與安全邊界](../common/contract.md)
2. [simplicity 方法與完成條件](references/method.md)

輸出格式只以 request.output 的 registry schema 為權威。完成本角色後交還 Controller，不自行 spawn、補角色或建立下一 stage。
