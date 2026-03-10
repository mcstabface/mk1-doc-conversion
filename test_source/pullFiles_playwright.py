#!/usr/bin/env python3
import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URLS = [
    "https://www.justice.gov/epstein/doj-disclosures/data-set-11-files?page=0",
    "https://www.justice.gov/epstein/doj-disclosures/data-set-11-files?page=1",
]

PDF_NAME_RE = re.compile(r"\bEFTA\d{5,}\.pdf\b", re.IGNORECASE)

def extract_pdf_names(html: str) -> list[str]:
    return sorted(set(m.group(0) for m in PDF_NAME_RE.finditer(html)))

def try_click_yes(page) -> bool:
    # Strategy A: text-based locator (covers links/divs/spans/buttons)
    candidates = [
        'text="Yes"',
        'text=/^Yes$/i',
        'text=/\\bYes\\b/i',
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=4000)
            loc.click(timeout=4000)
            return True
        except Exception:
            pass

    # Strategy B: role-based (broader)
    try:
        page.get_by_role("link", name=re.compile(r"\bYes\b", re.I)).first.click(timeout=4000)
        return True
    except Exception:
        pass
    try:
        page.get_by_role("button", name=re.compile(r"\bYes\b", re.I)).first.click(timeout=4000)
        return True
    except Exception:
        pass

    # Strategy C: common inputs
    for css in [
        'input[value="Yes"]',
        'button:has-text("Yes")',
        'a:has-text("Yes")',
    ]:
        try:
            loc = page.locator(css).first
            loc.wait_for(state="visible", timeout=4000)
            loc.click(timeout=4000)
            return True
        except Exception:
            pass

    return False

def set_age_cookie(context):
    # Best-effort: some DOJ pages use a cookie like "age_verified" or "doj_age_verified".
    # We set a few likely names; harmless if ignored.
    cookies = [
        {"name": "age_verified", "value": "1", "domain": "www.justice.gov", "path": "/"},
        {"name": "doj_age_verified", "value": "1", "domain": "www.justice.gov", "path": "/"},
        {"name": "AgeGate", "value": "1", "domain": "www.justice.gov", "path": "/"},
    ]
    context.add_cookies(cookies)

def main():
    all_pdfs = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            locale="en-US",
        )
        page = context.new_page()

        # Go to page 0
        page.goto(URLS[0], wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1000)

        # Attempt to pass gate
        clicked = try_click_yes(page)
        if not clicked:
            # fallback: set cookie(s) and reload
            set_age_cookie(context)
            page.reload(wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(1000)

        # Now scrape page 0
        html0 = page.content()
        pdfs0 = extract_pdf_names(html0)
        print(f"{URLS[0]} -> {len(pdfs0)} pdfs")
        all_pdfs.update(pdfs0)

        # Visit page 1
        page.goto(URLS[1], wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1000)

        html1 = page.content()
        pdfs1 = extract_pdf_names(html1)
        print(f"{URLS[1]} -> {len(pdfs1)} pdfs")
        all_pdfs.update(pdfs1)

        # Debug if still zero
        if len(all_pdfs) == 0:
            print("DEBUG: title:", page.title())
            snippet = re.sub(r"\s+", " ", html1)[:300]
            print("DEBUG: html snippet:", snippet)

        browser.close()

    all_sorted = sorted(all_pdfs)
    with open("doj_all_pdfs_playwright.txt", "w", encoding="utf-8") as f:
        for name in all_sorted:
            f.write(name + "\n")

    print(f"Total unique PDFs collected (playwright): {len(all_sorted)}")
    print("Wrote: doj_all_pdfs_playwright.txt")

if __name__ == "__main__":
    main()
