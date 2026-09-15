#!/usr/bin/env bash
# Renders docs/report.md (and docs/proposal.md) to PDF: Markdown -> HTML (python-markdown) -> PDF (LibreOffice).
# The converter lives in a throwaway venv under tools/, and LibreOffice runs with a private profile
# so it never interferes with a LibreOffice window that is already open.
set -euo pipefail
DOCS="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$DOCS")"
VENV="$ROOT/tools/docs-venv"
PROFILE="$ROOT/tools/lo-profile"

[[ -x "$VENV/bin/python" ]] || { python3 -m venv "$VENV" && "$VENV/bin/pip" install -q markdown; }

for name in "${@:-report proposal}"; do
  for doc in $name; do
    "$VENV/bin/python" - "$DOCS/$doc.md" "$DOCS/$doc.html" <<'EOF'
import re, sys, markdown
src, dst = sys.argv[1], sys.argv[2]
body = markdown.markdown(open(src, encoding="utf-8").read(), extensions=["tables", "fenced_code", "sane_lists"])
# LibreOffice's HTML import honours attributes more reliably than CSS for tables and images.
body = body.replace("<table>", '<table border="1" cellspacing="0" cellpadding="4" width="100%">')
import os, struct
def sized(m):
    alt, path = m.group(1), m.group(2)
    with open(os.path.join(os.path.dirname(src), path), "rb") as f:
        w, h = struct.unpack(">II", f.read(24)[16:24])          # PNG IHDR width/height
    # Full width for the wide architecture diagram; charts are legible at about two thirds of the page.
    width_cm = {"architecture.png": 16.5, "category_flows.png": 10.0}.get(os.path.basename(path), 11.5)
    return f'<img alt="{alt}" src="{path}" style="width:{width_cm}cm;height:{width_cm * h / w:.2f}cm"'
body = re.sub(r'<img alt="([^"]*)" src="([^"]+)"', sized, body)
html = f"""<!doctype html><html><head><meta charset="utf-8"><title>{src.rsplit('/', 1)[-1]}</title>
<style>
 body {{ font-family: 'Liberation Sans', Arial, sans-serif; font-size: 10pt; line-height: 1.3; }}
 h1 {{ font-size: 20pt; margin-bottom: 4pt; }}  h2 {{ font-size: 13.5pt; margin-top: 10pt; margin-bottom: 4pt; }}
 h3 {{ font-size: 11.5pt; margin-top: 8pt; margin-bottom: 3pt; }}  table {{ border-collapse: collapse; font-size: 9pt; }}
 th {{ background: #e3eefb; }}  td, th {{ margin: 0; padding: 2pt 4pt; }}  code {{ font-family: 'Liberation Mono', monospace; font-size: 9pt; }}
 p em {{ color: #52514e; }}  img {{ display: block; margin: 0 auto; }}  blockquote {{ color: #52514e; border-left: 3px solid #c3c2b7; padding-left: 8pt; }}
</style></head><body>{body}</body></html>"""
open(dst, "w", encoding="utf-8").write(html)
EOF
    soffice -env:UserInstallation="file://$PROFILE" --headless --infilter="HTML (StarWriter)" --convert-to pdf:writer_pdf_Export \
      --outdir "$DOCS" "$DOCS/$doc.html" >/dev/null 2>&1
    rm -f "$DOCS/$doc.html"
    echo "  wrote docs/$doc.pdf ($(du -h "$DOCS/$doc.pdf" | cut -f1))"
  done
done
