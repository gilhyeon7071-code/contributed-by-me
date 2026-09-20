# UI Numeric Display Rules (SSOT)

## 1) Data Type
- Sort/filter target columns must remain numeric (`int/float`) until render.
- Do not convert numeric columns to string in source DataFrame.
- Text columns (name, status, reason) remain string.

## 2) Missing Values
- Numeric missing values stay as `NaN` in data.
- Render-time only: show missing as `-` (`na_rep="-"`).
- Do not inject `"-"`/`None` into numeric columns before render.

## 3) Format
- Money/price/pnl/qty: `{:,.0f}`
- Percent: `{:+.2f}%`
- Stock code: digits only, 6-digit zero-pad.

## 4) Sorting
- Sorting must use numeric dtype, not display string.
- If formatting is needed, use render layer (`Styler.format` or `column_config`).

## 5) Scope
- Applies to stock dashboard (`8502`) and integrated validation dashboard (`8501`).
- New tables must follow the same render rule set.

## 6) Validation Checklist
- Comma rendering check (`93500 -> 93,500`)
- Percent rendering check (`3.96 -> +3.96%`)
- Missing value rendering check (`NaN -> -`)
- Asc/desc sort check on numeric columns
