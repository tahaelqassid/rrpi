"""ingestion/pipeline.py — orchestrates scraping and raw data storage"""
from datetime import datetime
from typing import List, Dict
from config.settings import CITIES, MAX_PAGES_PER_CITY
from database.models import SessionLocal, RawListing, ScrapeLog
from utils.helpers import make_hash
from utils.logger import log


def save_raw(listings: List[Dict], session) -> int:
    inserted = 0
    for l in listings:
        h = make_hash(l.get("url",""), l.get("price"), l.get("title",""))
        try:
            if session.query(RawListing).filter_by(listing_hash=h).first():
                continue
            session.add(RawListing(
                source       = l.get("source","unknown"),
                url          = l.get("url"),
                listing_hash = h,
                title        = l.get("title"),
                city         = l.get("city"),
                neighborhood = l.get("neighborhood"),
                raw_price    = l.get("raw_price"),
                raw_surface  = l.get("raw_surface"),
                raw_rooms    = l.get("raw_rooms"),
                scraped_at   = datetime.utcnow(),
            ))
            session.flush()
            inserted += 1
        except Exception:
            session.rollback()
    session.commit()
    return inserted


def run_ingestion(cities: List[str] = None, max_pages: int = None):
    from ingestion.scrapers.mubawab_scraper import MubawabScraper

    cities    = cities    or list(CITIES.keys())
    max_pages = max_pages or MAX_PAGES_PER_CITY
    session   = SessionLocal()
    total     = 0
    start     = datetime.utcnow()

    log.info(f"╔══════════════════════════════════════════╗")
    log.info(f"║  RPPI Ingestion — {len(cities)} cities             ║")
    log.info(f"║  Max pages: {max_pages} · Source: Mubawab.ma    ║")
    log.info(f"╚══════════════════════════════════════════╝")

    for city in cities:
        log_row = ScrapeLog(source="mubawab", city=city,
                            started_at=datetime.utcnow(), status="running")
        session.add(log_row)
        session.commit()
        try:
            scraper  = MubawabScraper(city=city, max_pages=max_pages)
            listings = scraper.run()
            for l in listings:
                l["source"] = "mubawab"
                l["city"]   = city
            inserted = save_raw(listings, session)
            total   += inserted
            log_row.finished_at      = datetime.utcnow()
            log_row.records_found    = len(listings)
            log_row.records_inserted = inserted
            log_row.status           = "success"
            session.commit()
            log.success(f"[{city}] {len(listings)} found → {inserted} new")
        except Exception as e:
            log.error(f"[{city}] error: {e}")
            log_row.status = "failed"
            log_row.finished_at = datetime.utcnow()
            session.commit()

    elapsed = (datetime.utcnow() - start).seconds
    log.info(f"╔══════════════════════════════════════════╗")
    log.info(f"║  INGESTION COMPLETE                      ║")
    log.info(f"║  Total inserted : {total:<6}                ║")
    log.info(f"║  Time elapsed   : {elapsed}s                  ║")
    log.info(f"╚══════════════════════════════════════════╝")
    session.close()
    return total
