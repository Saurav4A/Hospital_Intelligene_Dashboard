## MOD report PDF fonts

Morning MOD PDF export can render Hindi/Devanagari remarks only when the server
has a Devanagari-capable TrueType/OpenType font available.

Supported options:

- Install `Nirmala UI` or `Mangal` on Windows Server.
- Install `Noto Sans Devanagari` on Linux.
- Place `NotoSansDevanagari-Regular.ttf` and optionally
  `NotoSansDevanagari-Bold.ttf` in this folder.
- Or set `MOD_REPORT_DEVANAGARI_FONT_PATH` to a font file path on the server.
