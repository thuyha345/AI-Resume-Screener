from src.database.database import Database

if __name__ == "__main__":
    db = Database("storage/app.db")

    resume_id = db.insert_resume(
        filename="cv_demo.pdf",
        language="vi",
        file_path="data/cv_demo.pdf",
        raw_text="Demo CV: Python, SQL...",
        ocr_used=False
    )

    db.insert_extraction(resume_id, {"name": "Demo User", "skills": ["Python", "SQL"]})
    db.insert_screening_result(resume_id, "Data Analyst Intern", 0.85, "Có Python/SQL phù hợp.")

    print("Resumes:", db.list_resumes())
    print("OK! resume_id =", resume_id)
