import json
import re

import PyPDF2

def extract_text_from_file(uploaded_file) -> str:
    """Safely extracts text from uploaded PDF or TXT files."""
    if uploaded_file.name.endswith('.pdf'):
        pdf_reader = PyPDF2.PdfReader(uploaded_file)
        text = ""
        for i, page in enumerate(pdf_reader.pages):
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
                
        if not text.strip():
            raise ValueError(f"No extractable text found in '{uploaded_file.name}'. Is it a scanned document? Standard text-based PDFs are required.")
            
        return text
    else:
        return uploaded_file.getvalue().decode("utf-8")


def normalize_whitespace(text: str) -> str:
    """Normalizes whitespace without destroying paragraph boundaries."""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def optimize_policy_text(policy_text: str, max_chars: int = 12000, max_paragraphs: int = 18) -> str:
    """
    Reduces long policy documents into the most rule-dense paragraphs.

    This keeps the first paragraph for context, then ranks remaining paragraphs
    using policy-specific heuristics so we preserve obligations, thresholds, and
    exception language instead of sending the whole PDF verbatim.
    """
    normalized = normalize_whitespace(policy_text)
    if len(normalized) <= max_chars:
        return normalized

    paragraphs = [p.strip() for p in normalized.split("\n\n") if p.strip()]
    if not paragraphs:
        return normalized[:max_chars]

    keywords = {
        "must", "shall", "required", "prohibited", "violation", "flag",
        "review", "threshold", "limit", "within", "days", "hours",
        "amount", "account", "transaction", "customer", "gdpr", "aml",
        "sanction", "retention", "sla", "report", "suspicious"
    }

    scored = []
    for index, paragraph in enumerate(paragraphs):
        lower = paragraph.lower()
        score = 0
        score += sum(2 for keyword in keywords if keyword in lower)
        if re.search(r"\b\d+(?:,\d{3})*(?:\.\d+)?\b", paragraph):
            score += 3
        if re.search(r"[$€£₹%]", paragraph):
            score += 2
        if re.search(r"\b(rule|section|article|clause|control)\b", lower):
            score += 2
        if 80 <= len(paragraph) <= 1200:
            score += 1
        scored.append((score, index, paragraph))

    selected = []
    used_indexes = set()

    # Keep the opening context paragraph when possible.
    selected.append((0, paragraphs[0]))
    used_indexes.add(0)
    total_chars = len(paragraphs[0])

    ranked = sorted(scored, key=lambda item: (-item[0], item[1]))
    for _, index, paragraph in ranked:
        if index in used_indexes:
            continue
        projected = total_chars + len(paragraph) + 2
        if projected > max_chars:
            continue
        selected.append((index, paragraph))
        used_indexes.add(index)
        total_chars = projected
        if len(selected) >= max_paragraphs:
            break

    selected.sort(key=lambda item: item[0])
    optimized = "\n\n".join(paragraph for _, paragraph in selected).strip()
    return optimized if optimized else normalized[:max_chars]


def build_schema_context(df, max_examples: int = 3, max_columns: int = 40) -> str:
    """Creates a compact schema profile for prompting Agent 2."""
    profiles = []
    for column in list(df.columns)[:max_columns]:
        series = df[column]
        examples = series.dropna().astype(str).unique()[:max_examples].tolist()
        profiles.append({
            "name": column,
            "dtype": str(series.dtype),
            "examples": examples,
        })

    return json.dumps({"columns": profiles}, separators=(",", ":"), default=str)
