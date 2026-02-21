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
