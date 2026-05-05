import fitz
import json

doc = fitz.open("/Ubuntu/home/devcontainers/uGithub/DDocs/documents/2026/miami-grand-prix/decision-car-14-alleged-yellow-flag-infringement.pdf")
font_info = []

for i in range(len(doc)):
    page = doc[i]
    fonts = page.get_fonts()
    for f in fonts:
        font_info.append({
            "page": i + 1,
            "font_name": f[3],
            "font_type": f[2]
        })

print(json.dumps(font_info, indent=2))
