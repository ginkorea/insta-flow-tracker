"""Daily ETL job that pulls data from public data sources."""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import sys
from pathlib import Path
from typing import Iterable, Optional
from xml.etree import ElementTree

import requests
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from db.models import (
    Award,
    Company,
    ETFTrade,
    Fund,
    Holding,
    Patent,
    SessionLocal,
)


# A descriptive user agent keeps us in good standing with the public APIs.
# The email can be overridden by setting PUBLIC_DATA_CONTACT_EMAIL in the
# environment so API providers have a way to reach us if needed.
DEFAULT_CONTACT = "ops@instaflow.local"
CONTACT_EMAIL = os.getenv("PUBLIC_DATA_CONTACT_EMAIL", DEFAULT_CONTACT)
USER_AGENT = f"InstaFlowTracker/0.1 (contact: {CONTACT_EMAIL})"


def _get_session_headers() -> dict[str, str]:
    return {"User-Agent": USER_AGENT}


def get_or_create_company(
    session, name: str, ticker: Optional[str] = None, cik: Optional[str] = None
) -> Company:
    company = session.query(Company).filter(Company.name == name).one_or_none()
    if company is None:
        company = Company(name=name, ticker=ticker, cik=cik)
        session.add(company)
        session.flush()
    else:
        # Backfill metadata when we learn more about the company.
        if ticker and not company.ticker:
            company.ticker = ticker
        if cik and not company.cik:
            company.cik = cik
    return company


def get_or_create_fund(session, name: str, fund_type: str) -> Fund:
    fund = session.query(Fund).filter(Fund.name == name).one_or_none()
    if fund is None:
        fund = Fund(name=name, type=fund_type)
        session.add(fund)
        session.flush()
    elif fund_type and not fund.type:
        fund.type = fund_type
    return fund


def fetch_latest_13f_holdings(session, cik: str = "0001067983") -> None:
    """Fetch the latest 13F holdings for a given CIK from the SEC."""

    cik_padded = cik.zfill(10)
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    try:
        submissions_resp = requests.get(
            submissions_url, headers=_get_session_headers(), timeout=30
        )
        submissions_resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ETL][13F] Unable to pull submissions feed for {cik}: {exc}")
        return

    submissions = submissions_resp.json()
    recent = submissions.get("filings", {}).get("recent", {})
    forms: Iterable[str] = recent.get("form", [])
    accession_numbers: Iterable[str] = recent.get("accessionNumber", [])
    filing_dates: Iterable[str] = recent.get("filingDate", [])

    target_index = None
    for idx, form in enumerate(forms):
        if form == "13F-HR":
            target_index = idx
            break

    if target_index is None:
        print(f"[ETL][13F] No 13F-HR filings found for CIK {cik}.")
        return

    accession = accession_numbers[target_index]
    filing_date_str = filing_dates[target_index]
    try:
        filing_date = dt.date.fromisoformat(filing_date_str)
    except ValueError:
        filing_date = dt.date.today()

    accession_compact = accession.replace("-", "")
    cik_trimmed = cik.lstrip("0") or cik
    base_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_trimmed}/{accession_compact}/"
    )
    possible_paths = ["form13fInfoTable.xml", "infotable.xml", "primary_doc.xml"]
    info_resp = None
    for path in possible_paths:
        try:
            candidate = requests.get(
                base_url + path, headers=_get_session_headers(), timeout=30
            )
            if candidate.ok:
                info_resp = candidate
                break
        except requests.RequestException:
            continue

    if info_resp is None:
        print(
            f"[ETL][13F] Unable to locate holdings table for accession {accession}."
        )
        return

    try:
        document = ElementTree.fromstring(info_resp.content)
    except ElementTree.ParseError as exc:
        print(f"[ETL][13F] Unable to parse holdings XML: {exc}")
        return

    namespace = ""
    if document.tag.startswith("{"):
        namespace = document.tag.split("}", 1)[0].strip("{")
    ns = {"n": namespace} if namespace else {}

    def _find(element, tag: str) -> Optional[str]:
        if namespace:
            found = element.find(f"n:{tag}", ns)
        else:
            found = element.find(tag)
        return found.text.strip() if found is not None and found.text else None

    fund_name = submissions.get("name") or f"CIK {cik_trimmed}"
    fund = get_or_create_fund(session, fund_name, "13F filer")

    created = 0
    for table in document.findall(".//n:infoTable" if namespace else ".//infoTable", ns):
        issuer = _find(table, "nameOfIssuer")
        cusip = _find(table, "cusip")
        value_str = _find(table, "value")
        if not issuer or not value_str:
            continue

        try:
            position_usd = float(value_str.replace(",", "")) * 1000.0
        except ValueError:
            continue

        company = get_or_create_company(session, issuer, ticker=cusip)

        existing = (
            session.query(Holding)
            .filter(
                Holding.date == filing_date,
                Holding.fund_id == fund.id,
                Holding.company_id == company.id,
                Holding.source == "SEC 13F",
            )
            .one_or_none()
        )
        if existing is None:
            holding = Holding(
                date=filing_date,
                fund_id=fund.id,
                company_id=company.id,
                pos_usd=position_usd,
                source="SEC 13F",
            )
            session.add(holding)
            created += 1

    if created:
        try:
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            print(f"[ETL][13F] Failed to commit holdings: {exc}")
            return

    print(f"[ETL][13F] Upserted {created} holdings for {fund_name} ({filing_date}).")


def ingest_usaspending_awards(session, limit: int = 20) -> None:
    """Pull the most recent federal contract awards from USAspending."""

    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    today = dt.date.today()
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [
                {
                    "date_type": "action_date",
                    "start_date": (today - dt.timedelta(days=30)).isoformat(),
                    "end_date": today.isoformat(),
                }
            ],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Awarding Agency",
            "Action Date",
            "Last Modified Date",
        ],
        "page": 1,
        "limit": limit,
        "sort": "Last Modified Date",
        "order": "desc",
    }

    try:
        response = requests.post(
            url, json=payload, headers=_get_session_headers(), timeout=30
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ETL][Awards] Unable to fetch USAspending awards: {exc}")
        return

    data = response.json()
    results: Iterable[dict] = data.get("results", [])

    inserted = 0
    for award in results:
        date_signed = (
            award.get("Action Date")
            or award.get("Last Modified Date")
            or award.get("Period of Performance Start Date")
        )
        if not date_signed:
            continue
        try:
            award_date = dt.date.fromisoformat(date_signed)
        except ValueError:
            continue

        agency_name = award.get("Awarding Agency")
        recipient = award.get("Recipient Name")
        amount = award.get("Award Amount")
        program = award.get("Category") or award.get("Award ID")

        if not recipient or amount in (None, ""):
            continue

        try:
            amount_value = float(str(amount).replace(",", ""))
        except ValueError:
            continue

        company = get_or_create_company(session, recipient)

        existing = (
            session.query(Award)
            .filter(
                Award.date == award_date,
                Award.company_id == company.id,
                Award.amount_usd == amount_value,
                Award.source == "USAspending",
            )
            .one_or_none()
        )
        if existing:
            continue

        record = Award(
            date=award_date,
            agency=agency_name or "Unknown Agency",
            recipient=recipient,
            company_id=company.id,
            amount_usd=amount_value,
            program=program,
            source="USAspending",
        )
        session.add(record)
        inserted += 1

    if inserted:
        try:
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            print(f"[ETL][Awards] Failed to commit awards: {exc}")
            return

    print(f"[ETL][Awards] Upserted {inserted} award records from USAspending.")


def ingest_patents(session, per_page: int = 25) -> None:
    """Pull recent innovation disclosures using the OpenAlex works API."""

    window_start = (dt.date.today() - dt.timedelta(days=90)).isoformat()
    params = {
        "search": "patent",  # bias toward IP-related research
        "filter": f"publication_date:>{window_start}",
        "sort": "publication_date:desc",
        "per-page": per_page,
    }
    url = "https://api.openalex.org/works"

    try:
        response = requests.get(
            url, params=params, headers=_get_session_headers(), timeout=30
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ETL][Patents] Unable to query OpenAlex: {exc}")
        return

    payload = response.json()
    works = payload.get("results", [])

    def _reconstruct_abstract(abstract_index: Optional[dict]) -> Optional[str]:
        if not abstract_index:
            return None
        tokens: list[tuple[int, str]] = []
        for word, positions in abstract_index.items():
            for pos in positions:
                tokens.append((pos, word))
        tokens.sort(key=lambda item: item[0])
        return " ".join(word for _, word in tokens)

    inserted = 0
    for work in works:
        title = work.get("display_name")
        date_str = work.get("publication_date")
        if not (title and date_str):
            continue
        try:
            pub_date = dt.date.fromisoformat(date_str)
        except ValueError:
            continue

        institutions = []
        for authorship in work.get("authorships", []) or []:
            for institution in authorship.get("institutions", []) or []:
                name = institution.get("display_name")
                if name:
                    institutions.append(name)
        assignee_name = institutions[0] if institutions else None

        abstract = _reconstruct_abstract(work.get("abstract_inverted_index"))
        identifier = work.get("id")

        company = get_or_create_company(session, assignee_name or title.split()[0])

        existing = (
            session.query(Patent)
            .filter(
                Patent.pub_date == pub_date,
                Patent.company_id == company.id,
                Patent.title == title,
            )
            .one_or_none()
        )
        if existing:
            continue

        record = Patent(
            pub_date=pub_date,
            company_id=company.id,
            assignee=assignee_name,
            title=title,
            keywords=abstract,
            url=identifier,
        )
        session.add(record)
        inserted += 1

    if inserted:
        try:
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            print(f"[ETL][Patents] Failed to commit patents: {exc}")
            return

    print(f"[ETL][Patents] Upserted {inserted} innovation records from OpenAlex.")


def ingest_ark_trades(session, etf: str = "ARKK") -> None:
    """Pull daily pricing data for ARK ETFs from Stooq as a proxy for trades."""

    symbol = f"{etf.lower()}.us"
    csv_url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        response = requests.get(csv_url, headers=_get_session_headers(), timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ETL][ETF] Unable to download {etf} prices: {exc}")
        return

    decoded = response.content.decode("utf-8", errors="ignore")
    buffer = io.StringIO(decoded)
    reader = csv.DictReader(buffer)

    inserted = 0
    for row in reader:
        trade_date = row.get("Date")
        close_value = row.get("Close")
        if not trade_date or close_value in (None, ""):
            continue
        try:
            parsed_date = dt.date.fromisoformat(trade_date)
            trade_value = float(close_value)
        except ValueError:
            continue

        existing = (
            session.query(ETFTrade)
            .filter(
                ETFTrade.date == parsed_date,
                ETFTrade.etf == etf,
                ETFTrade.ticker == etf,
                ETFTrade.direction == "close",
            )
            .one_or_none()
        )
        if existing:
            continue

        trade = ETFTrade(
            date=parsed_date,
            etf=etf,
            ticker=etf,
            direction="close",
            value_usd=trade_value,
            source="Stooq",
        )
        session.add(trade)
        inserted += 1

    if inserted:
        try:
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            print(f"[ETL][ETF] Failed to commit ETF prices: {exc}")
            return

    print(f"[ETL][ETF] Upserted {inserted} daily prices for {etf} from Stooq.")


def run_daily():
    session = SessionLocal()
    print("[ETL] Running daily ingestion jobs…")
    try:
        fetch_latest_13f_holdings(session)
        ingest_usaspending_awards(session)
        ingest_patents(session)
        ingest_ark_trades(session)
    finally:
        session.close()


if __name__ == "__main__":
    run_daily()
