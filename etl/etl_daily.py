"""Daily ETL job that pulls data from public data sources."""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
from typing import Iterable, Optional
from xml.etree import ElementTree

import requests
from sqlalchemy.exc import SQLAlchemyError

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
USER_AGENT = "InstaFlowTracker/0.1 (github.com/openai)"


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

    url = "https://api.usaspending.gov/api/v2/awards/"
    params = {"page": 1, "limit": limit, "sort": "-date_signed"}
    try:
        response = requests.get(
            url, params=params, headers=_get_session_headers(), timeout=30
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ETL][Awards] Unable to fetch USAspending awards: {exc}")
        return

    data = response.json()
    results: Iterable[dict] = data.get("results", [])

    inserted = 0
    for award in results:
        date_signed = award.get("date_signed")
        if not date_signed:
            continue
        try:
            award_date = dt.date.fromisoformat(date_signed)
        except ValueError:
            continue

        agency_name = award.get("awarding_agency", {}).get("toptier_name") or award.get(
            "awarding_agency_name"
        )
        recipient = award.get("recipient", {}).get("recipient_name") or award.get(
            "recipient_name"
        )
        amount = award.get("total_obligation")
        program = award.get("type_description") or award.get("naics_description")

        if not recipient or amount is None:
            continue

        company = get_or_create_company(session, recipient)

        existing = (
            session.query(Award)
            .filter(
                Award.date == award_date,
                Award.company_id == company.id,
                Award.amount_usd == amount,
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
            amount_usd=float(amount),
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
    """Pull recent patents from the PatentsView API."""

    query = {
        "_gte": {"patent_date": (dt.date.today() - dt.timedelta(days=90)).isoformat()}
    }
    fields = [
        "patent_number",
        "patent_title",
        "patent_date",
        "assignees",
        "patent_abstract",
    ]
    options = {"page": 1, "per_page": per_page}

    url = "https://api.patentsview.org/patents/query"
    params = {
        "q": json.dumps(query),
        "f": json.dumps(fields),
        "o": json.dumps(options),
    }

    try:
        response = requests.get(
            url, params=params, headers=_get_session_headers(), timeout=30
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ETL][Patents] Unable to query PatentsView: {exc}")
        return

    payload = response.json()
    patents = payload.get("patents", [])

    inserted = 0
    for patent in patents:
        number = patent.get("patent_number")
        title = patent.get("patent_title")
        date_str = patent.get("patent_date")
        if not (number and title and date_str):
            continue
        try:
            pub_date = dt.date.fromisoformat(date_str)
        except ValueError:
            continue

        assignees = patent.get("assignees", [])
        assignee_name = None
        if assignees:
            assignee_name = assignees[0].get("assignee_organization")
        abstract = patent.get("patent_abstract")

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
            url=f"https://patents.google.com/patent/US{number}",
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

    print(f"[ETL][Patents] Upserted {inserted} patents from PatentsView.")


def ingest_ark_trades(session, etf: str = "ARKK") -> None:
    """Pull the latest disclosed trades from ARK's public CSV feed."""

    csv_url = f"https://ark-funds.com/wp-content/uploads/funds-etf-csv/{etf}_Trades.csv"
    try:
        response = requests.get(csv_url, headers=_get_session_headers(), timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ETL][ETF] Unable to download ARK trades ({etf}): {exc}")
        return

    decoded = response.content.decode("utf-8", errors="ignore")
    buffer = io.StringIO(decoded)
    reader = csv.DictReader(buffer)

    inserted = 0
    for row in reader:
        trade_date = row.get("date") or row.get("Date")
        if not trade_date:
            continue
        try:
            parsed_date = dt.date.fromisoformat(trade_date)
        except ValueError:
            continue

        ticker = row.get("ticker") or row.get("Ticker")
        direction = row.get("direction") or row.get("Direction")
        value = row.get("value") or row.get("Value")
        company_name = row.get("company") or row.get("Company")

        if value is None or not ticker:
            continue

        try:
            trade_value = float(value.replace(",", ""))
        except ValueError:
            continue

        company = get_or_create_company(session, company_name or ticker, ticker=ticker)

        existing = (
            session.query(ETFTrade)
            .filter(
                ETFTrade.date == parsed_date,
                ETFTrade.etf == etf,
                ETFTrade.ticker == ticker,
                ETFTrade.direction == direction,
            )
            .one_or_none()
        )
        if existing:
            continue

        trade = ETFTrade(
            date=parsed_date,
            etf=etf,
            ticker=ticker,
            direction=direction,
            value_usd=trade_value,
            source="ARK",
        )
        session.add(trade)
        inserted += 1

    if inserted:
        try:
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            print(f"[ETL][ETF] Failed to commit ETF trades: {exc}")
            return

    print(f"[ETL][ETF] Upserted {inserted} {etf} trades from ARK disclosures.")


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