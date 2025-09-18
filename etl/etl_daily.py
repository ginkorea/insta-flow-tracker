from db.models import SessionLocal

def run_daily():
    session = SessionLocal()
    print("[ETL] Running daily ingestion jobs…")
    # TODO: add actual API fetch calls
    session.close()

if __name__ == "__main__":
    run_daily()
