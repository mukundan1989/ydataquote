#!/usr/bin/env python3
"""Colab: %run /content/decodo_batched_prices_fixed_colab.py
Cookie-independent multi-symbol Spark test. 1000 symbols / 20 = 50 planned calls.
Each price is labelled as timestamped quote metadata or latest one-minute close.
API access is external and must pass the first batch before the rest run.
"""
import csv
import gzip
import importlib.util
import json
import math
import os
import subprocess
import sys
import time
import zipfile
import zlib
from datetime import datetime, timezone
from getpass import getpass
from html.parser import HTMLParser
from pathlib import Path

BATCH_SIZE = 20
GAP_SECONDS = 1.0
MAX_DECODO_CALLS = 50
TIMEOUT_SECONDS = 150
AUTO_DOWNLOAD_IN_COLAB = True
SYMBOLS_FILE = ''
DECODO_ENDPOINT = 'https://data.decodo.com/v1/scrape'
YAHOO_ENDPOINT = 'https://query1.finance.yahoo.com/v7/finance/spark'
PROXY_POOL = 'premium'
GEO = 'United States'
SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'GOOG', 'META', 'NVDA', 'TSLA', 'AVGO', 'ORCL', 'ADBE', 'CRM', 'AMD', 'INTC', 'QCOM', 'TXN', 'AMAT', 'MU', 'LRCX', 'KLAC', 'IBM', 'CSCO', 'NOW', 'INTU', 'PANW', 'CRWD', 'FTNT', 'SNPS', 'CDNS', 'ADI', 'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'BLK', 'SCHW', 'AXP', 'V', 'MA', 'PYPL', 'COF', 'USB', 'PNC', 'BK', 'TROW', 'SPGI', 'MCO', 'CME', 'UNH', 'JNJ', 'LLY', 'MRK', 'ABBV', 'PFE', 'TMO', 'ABT', 'DHR', 'AMGN', 'GILD', 'BMY', 'CVS', 'CI', 'HUM', 'MDT', 'SYK', 'BSX', 'REGN', 'VRTX', 'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'OXY', 'PSX', 'MPC', 'VLO', 'KMI', 'WMT', 'COST', 'HD', 'LOW', 'TGT', 'PG', 'KO', 'PEP', 'MDLZ', 'CL', 'CAT', 'DE', 'HON', 'GE', 'RTX', 'LMT', 'NOC', 'UPS', 'FDX', 'UNP', 'BUSE', 'VCTR', 'MESO', 'DGII', 'LPTH', 'PAVS', 'WMG', 'BLSM', 'GURE', 'RGC', 'IDN', 'SCPQ', 'BTOC', 'ATOS', 'MOMO', 'SAIH', 'SPTX', 'SCNX', 'MVBF', 'BNTC', 'PFIS', 'CMTL', 'ITRI', 'FORM', 'MGRT', 'NWSA', 'KC', 'ALLO', 'ECPG', 'BWAY', 'FRGT', 'MMSI', 'RYET', 'OCFC', 'RNXT', 'BRCB', 'OIG', 'FRVO', 'BHRB', 'CAAS', 'GRNQ', 'AIAI', 'ALRM', 'NAKA', 'SPPL', 'NATR', 'IONR', 'TRAW', 'NXH', 'BCTX', 'CYN', 'HTFL', 'IDCC', 'FSUN', 'SGML', 'ESEA', 'GBLI', 'PHOS', 'GRCE', 'DAVI', 'JRSH', 'INLF', 'OXBR', 'BOKF', 'LGIH', 'DMII', 'BBIO', 'ZJYL', 'IZM', 'BWEN', 'ALCO', 'NTRB', 'CHEF', 'SYRE', 'EHGO', 'HUMA', 'NVNO', 'INV', 'XRX', 'AEHL', 'NMRA', 'RTSN', 'INCR', 'RSSS', 'WAFDP', 'STOK', 'NXL', 'CTXR', 'HIVE', 'RNAC', 'PWRL', 'MBVI', 'WNEB', 'FLNT', 'JSPR', 'CRDO', 'TECH', 'LIDR', 'BXBL', 'CNVS', 'TWST', 'PRTA', 'RYOJ', 'RUSHB', 'TTMI', 'YICC', 'LGO', 'LE', 'FOXF', 'ASTL', 'DRK', 'OPEN', 'VIAV', 'BCBP', 'DFTX', 'CTW', 'HWC', 'EBAY', 'IPX', 'ZBAO', 'KGEI', 'PUSA', 'SNT', 'OPBK', 'RGP', 'BRKR', 'VREX', 'CURR', 'SOLS', 'BEEM', 'APYX', 'TIPT', 'FLXS', 'OLOX', 'FATN', 'XLAB', 'PCB', 'MVST', 'HEPS', 'ENLT', 'CAST', 'AIIR', 'MTRX', 'RAIL', 'FCFS', 'LEE', 'HRZN', 'AIFA', 'DOYU', 'ULCC', 'COFS', 'CGTX', 'LNSR', 'UONEK', 'IRON', 'ZS', 'DDI', 'CETX', 'NKTR', 'KMB', 'IDYA', 'BRUN', 'IMMR', 'LZMH', 'ABSI', 'GNTA', 'CDTG', 'SENEB', 'KYNB', 'CBUS', 'CRVS', 'CHGA', 'CGEN', 'GLPI', 'KFFB', 'BETR', 'LXEO', 'AHG', 'HAPN', 'LFVN', 'LASR', 'CHRD', 'EGBN', 'EXFY', 'HCM', 'NNOX', 'EDIT', 'RDI', 'CEPO', 'HOPE', 'GIFT', 'AIBZ', 'VAI', 'PERI', 'ACCL', 'ENLV', 'LTRX', 'ABVX', 'KLRA', 'MWH', 'USIO', 'BGMS', 'RUM', 'ICON', 'OPNW', 'ROST', 'SANM', 'GRSD', 'WOK', 'APXC', 'MITK', 'KRYS', 'PMAX', 'TELA', 'MDXG', 'ONEG', 'HCKT', 'PDSB', 'ZG', 'LOOP', 'ABOS', 'DYNC', 'RUSHA', 'CFFN', 'EGAN', 'ARHS', 'ADEA', 'JRVR', 'ARKO', 'WRAP', 'NTLA', 'LQDT', 'PGEN', 'SPT', 'ARCB', 'ALOY', 'QSI', 'TRDA', 'MGIH', 'DDOG', 'GLBE', 'PLXS', 'OCS', 'ENTG', 'VSNT', 'ORLY', 'JBLU', 'HIFS', 'TH', 'CELZ', 'BATRK', 'TKNO', 'LKFN', 'AZR', 'AMPH', 'PAYP', 'ALPS', 'CVKD', 'RCKT', 'WULF', 'LCNB', 'IQ', 'GDS', 'RWAY', 'AGPU', 'CULP', 'TTWO', 'VOD', 'BLRX', 'GLIBA', 'LFS', 'PHAT', 'RADX', 'NXDR', 'CGC', 'LEXX', 'ZENA', 'MAR', 'LEDS', 'LKQ', 'LSAK', 'TTGT', 'HOFT', 'ROOT', 'INMB', 'PICS', 'FINW', 'INVA', 'PFX', 'FMST', 'CV', 'PTN', 'HQY', 'TFSL', 'SKYW', 'CCCC', 'PROV', 'TRT', 'CYTK', 'PRTS', 'VIVS', 'BJDX', 'IMNM', 'POOL', 'INEO', 'AYTU', 'RENX', 'GMAB', 'KRMD', 'SSP', 'RDAG', 'CHRN', 'NICM', 'CDZIP', 'DUKR', 'NSPR', 'CGEM', 'ACRS', 'INNV', 'OPXS', 'LTBR', 'MDB', 'GSUN', 'KLTR', 'LYEL', 'NNBR', 'RPAY', 'BSY', 'FTLF', 'BVC', 'TTRX', 'CYRX', 'MKZR', 'PSNL', 'GRAN', 'TARS', 'FIVE', 'EBMT', 'LCU', 'CCG', 'CAES', 'CXII', 'APLD', 'ADSE', 'PAMT', 'NTCT', 'PNTG', 'CBLL', 'LAKE', 'IMRN', 'VERA', 'PACB', 'DCGO', 'MBAI', 'XCUR', 'VIR', 'DORM', 'BDMD', 'OCC', 'GLOO', 'GO', 'CAPS', 'RKLB', 'MORN', 'RAVE', 'AVAT', 'QTEX', 'LINC', 'OVLY', 'NVMI', 'EQIX', 'WSFS', 'SBRA', 'DARE', 'SOTK', 'HIT', 'TRBG', 'LFST', 'MCAH', 'SNGX', 'ELTK', 'CARL', 'DLHC', 'PRHI', 'CCOI', 'MNDO', 'ZYBT', 'FGI', 'ECBK', 'ZLAB', 'SOGP', 'AVIR', 'BLLN', 'GEOS', 'GFUZ', 'QTRX', 'DAKT', 'SCYX', 'KXIN', 'BMRN', 'WEST', 'NTAP', 'IRTC', 'SPSC', 'WEYS', 'EML', 'VALU', 'ACTU', 'BLMN', 'FRNM', 'ANTX', 'LBRX', 'FIZZ', 'HROW', 'ARTNA', 'MSBI', 'KRNY', 'FCUV', 'AMPG', 'UVSP', 'ADV', 'STRO', 'CBIO', 'AEC', 'SKYA', 'NHTC', 'FBIO', 'EDBL', 'DGICA', 'TCOM', 'RGLD', 'HITI', 'LBTYA', 'NSIT', 'JFU', 'GOGO', 'BVFL', 'VANI', 'ARWR', 'NODK', 'ANGO', 'MRTN', 'RRBI', 'HTOO', 'LILA', 'TTEC', 'ZTG', 'BFRG', 'PBFS', 'TITN', 'OPI', 'PBK', 'STRR', 'ECHO', 'RNA', 'AZI', 'YHNA', 'REYN', 'VEON', 'CIGL', 'BDAI', 'UPBD', 'BL', 'CWST', 'REAL', 'HWBK', 'WEN', 'CODA', 'DSWL', 'XE', 'PPC', 'ERAS', 'BTCS', 'AUID', 'MTC', 'BULL', 'LITE', 'CSAI', 'APPN', 'PALI', 'BRZE', 'UMBF', 'VLOS', 'LFTO', 'MFIN', 'RICK', 'SAIA', 'VISN', 'AIIO', 'MNPR', 'AIOT', 'SAMG', 'ABNB', 'MNOV', 'TRSG', 'RKTO', 'GLND', 'BIOA', 'DAIC', 'IQST', 'WRD', 'CLRB', 'MLKN', 'MRNO', 'PRME', 'CIIT', 'GIW', 'HUT', 'TVRD', 'FLEX', 'ARCL', 'XENE', 'NGEN', 'CENTA', 'ADSK', 'HBIO', 'BGLC', 'AAME', 'AVR', 'SWBI', 'ZYME', 'OBAI', 'VCRE', 'LX', 'CRMD', 'ALNY', 'MBBC', 'CALC', 'ABCL', 'WTFC', 'BZ', 'RDCM', 'MBIO', 'CHSN', 'MAAS', 'VOGX', 'ALXO', 'OIO', 'TRXB', 'NTRP', 'ZION', 'BCG', 'CPOP', 'MIDD', 'TGTX', 'MDXH', 'QTI', 'BATRA', 'DFNS', 'YDDL', 'BCRX', 'LLYVK', 'HLIT', 'MSS', 'FENC', 'STIM', 'FCEL', 'COSM', 'RMR', 'SIGA', 'PARA', 'HPAI', 'SMMT', 'IVDA', 'MCHB', 'AIRS', 'BSVN', 'IMXI', 'NCNA', 'FNUC', 'IPGP', 'ALEC', 'TRMD', 'SNEX', 'BOXL', 'MWC', 'HCHL', 'NXST', 'HFBL', 'CEPS', 'KARD', 'DSGR', 'CHDN', 'TIGO', 'AKAN', 'BTTC', 'WTF', 'INTJ', 'LICN', 'FBDT', 'LSBK', 'COLM', 'EJH', 'NEO', 'LNTH', 'GBFH', 'GDC', 'TBCV', 'FRME', 'VERI', 'FULT', 'ECOR', 'OMEX', 'HYFT', 'ALVO', 'EFSI', 'MKSI', 'SUGP', 'RYM', 'TRVI', 'NWGL', 'ONDS', 'JAGX', 'BZFD', 'CNTX', 'PMN', 'WLFC', 'PWP', 'WBTN', 'PLRX', 'LYCA', 'EQ', 'MTEX', 'FDMT', 'APAC', 'WYNN', 'ANNA', 'NKLR', 'NWPX', 'DTSS', 'FSBC', 'PENN', 'PRQR', 'INBK', 'ASYS', 'GCL', 'ALKT', 'PAYS', 'TASK', 'PDD', 'IMOS', 'SHOT', 'BBNX', 'KEQU', 'ZJK', 'FLUX', 'ZBIO', 'XBIO', 'ISPC', 'AQB', 'EZRA', 'AIFU', 'MGNI', 'CAN', 'WBUY', 'OCG', 'PASW', 'BWMN', 'AUPH', 'BTBD', 'REFR', 'PTRN', 'PVLA', 'ODTX', 'LHSW', 'ISTR', 'RDNW', 'SRRK', 'LCID', 'PFG', 'DOGZ', 'TW', 'VOXR', 'YORW', 'TJGC', 'BSET', 'LKFT', 'DRIO', 'ABEO', 'PCVX', 'TNMG', 'ELWT', 'ADAC', 'AMPL', 'AUR', 'OTTR', 'DYN', 'KYTX', 'SLNH', 'NXPL', 'IRDM', 'LZ', 'GRWG', 'XELB', 'GHRS', 'CGCF', 'AENT', 'CPBI', 'INHD', 'SFHG', 'PEBO', 'VNOM', 'TBLA', 'FRPH', 'SRTS', 'QXL', 'RMSG', 'CBSH', 'VOR', 'GENK', 'NDSN', 'BOLD', 'MGYR', 'LABT', 'ORMP', 'BBOT', 'KZIA', 'NWL', 'IVA', 'INVZ', 'ASTH', 'PYXS', 'INTA', 'HCSG', 'NPCE', 'GLNG', 'TRS', 'NVCT', 'YOUL', 'AXSM', 'GDYN', 'SRTM', 'AMSS', 'KRNT', 'BMEA', 'OPRX', 'SDGR', 'SRPT', 'ATCX', 'CELH', 'EDUC', 'NFE', 'FLNA', 'SIDU', 'SPCB', 'FOFO', 'CLIR', 'VVOS', 'CLNE', 'HGBL', 'AIXI', 'TULP', 'PRTH', 'AVPT', 'STI', 'IRD', 'IMTE', 'PPIH', 'CYCU', 'BSIN', 'SSRM', 'XGN', 'INKT', 'BSRR', 'DEFT', 'PLUT', 'KRKR', 'CELU', 'AKTS', 'FISN', 'MEGL', 'PMVP', 'JONE', 'KTOS', 'OSBC', 'WLDN', 'RYTM', 'CLLS', 'AQMS', 'LIND', 'INDV', 'GMEX', 'LOCO', 'PDEX', 'CIFR', 'ALMR', 'JAKK', 'GUTS', 'SWVL', 'DNMX', 'MFP', 'BGDE', 'GLIBK', 'GTIM', 'CADL', 'ARQQ', 'PATK', 'TRUP', 'AUC', 'LIEN', 'GWRS', 'APA', 'PDC', 'GTX', 'SEER', 'SRZN', 'KOPN', 'BCAX', 'CIVB', 'ALDF', 'SWKS', 'VHCP', 'CLMB', 'LMB', 'RLMD', 'TBCH', 'XERS', 'BIAF', 'PCSA', 'DCX', 'TAYD', 'IQMX', 'AMRN', 'ELPW', 'CRDF', 'FRMM', 'CTMX', 'ERNA', 'AFRM', 'SOPH', 'FCAP', 'PLYX', 'ZEO', 'EBC', 'EFSC', 'SEGG', 'FNLC', 'CBC', 'SGHT', 'ASPI', 'DRTS', 'SBC', 'QTTB', 'AIRJ', 'RDNT', 'CDIO', 'FXNC', 'LPCN', 'ZCMD', 'BGM', 'BOLT', 'GLBS', 'RPID', 'MRX', 'NNE', 'IDXX', 'ATEX', 'SVCC', 'NAAS', 'LUCY', 'BMM', 'YSXT', 'ATXG', 'RMCF', 'BYRN', 'DYOR', 'FRTT', 'ZSTK', 'SURG', 'LAUR', 'SAFT', 'SLM', 'BNR', 'OGI', 'SWMR', 'AVAV', 'STAK', 'FJDI', 'NBIX', 'CRBU', 'ALMU', 'JL', 'BFC', 'IVF', 'YXT', 'CSPI', 'UXIN', 'UPXI', 'CRVL', 'FTDR', 'ASTS', 'DCTH', 'GTLB', 'BVS', 'FFIV', 'MRBK', 'PC', 'URGN', 'ACT', 'BZUN']
SYMBOL_SOURCE = '100 familiar test symbols + 900 Nasdaq-listed stock/ADR symbols; directory downloaded 2026-10-29 from https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'

class StopRun(Exception):
    pass


def utc(stamp=None):
    return datetime.fromtimestamp(time.time() if stamp is None else stamp, timezone.utc).isoformat()


def finite_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def valid_pair(price, stamp, now):
    return finite_number(price) and price > 0 and finite_number(stamp) and 0 < stamp <= now + 300


def load_symbols():
    raw = Path(SYMBOLS_FILE).read_text().splitlines() if SYMBOLS_FILE else SYMBOLS
    result = list(dict.fromkeys(s.strip().upper() for s in raw if s.strip() and not s.lstrip().startswith('#')))
    if not result or any(',' in s or any(c.isspace() for c in s) for s in result):
        raise ValueError('Supply one valid Yahoo symbol per line')
    if not 1 <= BATCH_SIZE <= 20:
        raise ValueError('BATCH_SIZE must be 1 through 20')
    return result


def write_csv(path, rows):
    fields = list(dict.fromkeys(k for r in rows for k in r)) or ['status']
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)


def missing_row(symbol, error='Run stopped before this symbol was fetched'):
    return dict(symbol=symbol, status='not_fetched', price='', price_time_utc='',
        price_age_seconds='', price_source='', market_session='', currency='',
        exchange_delay_minutes='', fetched_at_utc='', error=error)


def price_from_response(response, now):
    meta = response.get('meta') or {}
    candidates = []
    for session in ('regular', 'pre', 'post'):
        price, stamp = meta.get(session+'MarketPrice'), meta.get(session+'MarketTime')
        if valid_pair(price, stamp, now):
            candidates.append((stamp, 1, price, session+'_market_quote', session))
    timestamps = response.get('timestamp') or []
    quotes = (response.get('indicators') or {}).get('quote') or []
    closes = quotes[0].get('close', []) if quotes and isinstance(quotes[0], dict) else []
    for stamp, price in zip(timestamps, closes):
        if valid_pair(price, stamp, now):
            candidates.append((stamp, 0, price, 'one_minute_close', 'unknown'))
    if not candidates:
        return None
    stamp, _, price, source, session = max(candidates)
    return dict(price=price, price_time_utc=utc(stamp), price_age_seconds=round(now-stamp),
        price_source=source, market_session=session, currency=meta.get('currency', ''),
        exchange_delay_minutes=meta.get('exchangeDataDelayedBy', ''))


def parse_spark(payload, requested, now=None):
    now = time.time() if now is None else now
    spark = payload.get('spark') if isinstance(payload, dict) else None
    if not isinstance(spark, dict):
        raise StopRun('Yahoo response is not a Spark JSON response')
    if spark.get('error'):
        raise StopRun('Yahoo Spark returned an application error')
    items = spark.get('result')
    if not isinstance(items, list):
        raise StopRun('Yahoo Spark result list is missing')
    by_symbol = {str(x.get('symbol', '')).upper(): x for x in items if isinstance(x, dict)}
    rows = []
    for symbol in requested:
        row = missing_row(symbol, 'Symbol omitted from batch response')
        row.update(status='not_returned', fetched_at_utc=utc(now))
        item = by_symbol.get(symbol)
        if item:
            responses = item.get('response') or []
            choices = []
            for response in responses:
                if not isinstance(response, dict):
                    continue
                returned_symbol = str((response.get('meta') or {}).get('symbol', symbol)).upper()
                if returned_symbol != symbol:
                    continue
                value = price_from_response(response, now)
                if value:
                    choices.append(value)
            if choices:
                value = max(choices, key=lambda q: q['price_time_utc'])
                row.update(value, status='ok', error='')
            else:
                row.update(status='invalid_price', error='No valid matching timestamp/price pair')
        rows.append(row)
    return rows


def content_json(content):
    if isinstance(content, dict):
        return content
    if not isinstance(content, str):
        raise StopRun('Decodo target content is missing or has an unsupported type')
    try:
        return json.loads(content)
    except (ValueError, TypeError):
        pass
    class PreParser(HTMLParser):
        def __init__(self):
            super().__init__(); self.depth = 0; self.parts = []
        def handle_starttag(self, tag, attrs):
            if tag == 'pre': self.depth += 1
        def handle_endtag(self, tag):
            if tag == 'pre': self.depth = max(0, self.depth-1)
        def handle_data(self, text):
            if self.depth: self.parts.append(text)
    parser = PreParser(); parser.feed(content)
    try:
        return json.loads(''.join(parser.parts))
    except (ValueError, TypeError):
        raise StopRun('Yahoo returned HTML/non-JSON content; inspect response diagnostics') from None


class Client:
    def __init__(self, session, token, out):
        self.session, self.token, self.out = session, token, out
        self.attempts = []
        self.previous_end = None

    def get_batch(self, batch, batch_id):
        import requests
        if len(self.attempts) >= MAX_DECODO_CALLS:
            raise StopRun('Client call budget reached; no further requests sent')
        if self.previous_end is not None:
            time.sleep(max(0, self.previous_end + GAP_SECONDS - time.monotonic()))
        params = dict(symbols=','.join(batch), range='1d', interval='1m',
            includeTimestamps='true', includePrePost='true', indicators='close')
        url = requests.Request('GET', YAHOO_ENDPOINT, params=params).prepare().url
        payload = dict(url=url, proxy_pool=PROXY_POOL, geo=GEO, markdown=False)
        record = dict(call_number=len(self.attempts)+1, batch_id=batch_id, symbols=len(batch),
            decodo_status='', yahoo_status='', status='failed', error='', seconds=0,
            encoded_response_bytes=0, decoded_response_bytes=0, target_content_bytes=0,
            top_level_keys='', target_keys='', target_content_type='', usage_metadata='')
        self.attempts.append(record)
        write_csv(self.out/'request_report.csv', self.attempts)
        started = time.monotonic(); response = None
        try:
            # Exactly one client POST per batch. Requests does not retry automatically.
            response = self.session.post(DECODO_ENDPOINT, json=payload,
                headers={'Authorization': 'Bearer '+self.token, 'Content-Type': 'application/json',
                         'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate'},
                timeout=(10, TIMEOUT_SECONDS), stream=True, allow_redirects=False)
            record['decodo_status'] = response.status_code
            response.raw.decode_content = False
            data = bytearray()
            while True:
                part = response.raw.read(65536, decode_content=False)
                if not part: break
                data.extend(part); record['encoded_response_bytes'] = len(data)
                if len(data) > 10_000_000: raise StopRun('Encoded response exceeded 10 MB cap')
            encoding = response.headers.get('Content-Encoding', '').lower().strip()
            if encoding in ('', 'identity'): decoded = bytes(data)
            elif encoding == 'gzip': decoded = gzip.decompress(data)
            elif encoding == 'deflate':
                try: decoded = zlib.decompress(data)
                except zlib.error: decoded = zlib.decompress(data, -zlib.MAX_WBITS)
            else: raise StopRun('Unsupported response encoding')
            if len(decoded) > 25_000_000: raise StopRun('Decoded response exceeded 25 MB cap')
            record['decoded_response_bytes'] = len(decoded)
            if response.status_code != 200:
                raise StopRun('Decodo HTTP '+str(response.status_code)+'; inspect key, plan, credits or request settings')
            envelope = json.loads(decoded)
            if not isinstance(envelope, dict): raise StopRun('Unexpected Decodo response structure')
            record['top_level_keys'] = ','.join(sorted(envelope))
            usage = {k:v for k,v in envelope.items() if any(w in k.lower() for w in ('credit','cost','usage'))
                     and finite_number(v)}
            record['usage_metadata'] = json.dumps(usage) if usage else ''
            results = envelope.get('results')
            if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
                raise StopRun('Decodo results must contain exactly one target response')
            target = results[0]
            record['target_keys'] = ','.join(sorted(target))
            try: status = int(target.get('status_code'))
            except (TypeError, ValueError): raise StopRun('Target HTTP status missing') from None
            record['yahoo_status'] = status
            if status != 200:
                raise StopRun('Yahoo HTTP '+str(status)+'; stopped without retries or single-symbol fallback')
            content = target.get('content')
            record['target_content_type'] = type(content).__name__
            text = json.dumps(content) if isinstance(content, (dict, list)) else content
            record['target_content_bytes'] = len(text.encode()) if isinstance(text, str) else 0
            rows = parse_spark(content_json(content), batch)
            record['valid_symbols'] = sum(r['status'] == 'ok' for r in rows)
            record['status'] = 'validated' if record['valid_symbols'] == len(batch) else 'partial'
            return rows
        except StopRun as exc:
            record['error'] = str(exc); raise
        except Exception as exc:
            # No raw exception URLs, authentication headers, cookies or API keys saved.
            record['error'] = type(exc).__name__
            raise StopRun('Request or response failed: '+type(exc).__name__) from None
        finally:
            if response is not None: response.close()
            record['seconds'] = round(time.monotonic()-started, 3)
            self.previous_end = time.monotonic()
            write_csv(self.out/'request_report.csv', self.attempts)


def perform(client, symbols, out):
    results = {}
    batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    for i, batch in enumerate(batches, 1):
        print(f'Batch {i}/{len(batches)}: {len(batch)} symbols; one Decodo POST', flush=True)
        rows = client.get_batch(batch, i)
        results.update({r['symbol']:r for r in rows})
        write_csv(out/'prices.csv', [results.get(s, missing_row(s)) for s in symbols])
        valid = sum(r['status']=='ok' for r in rows)
        print(f'  Valid prices: {valid}/{len(batch)}; calls used: {len(client.attempts)}', flush=True)
        if i == 1 and valid != len(batch):
            raise StopRun('First batch did not fully validate; remaining paid calls cancelled')
        if valid == 0:
            raise StopRun('Batch returned no valid prices; remaining paid calls cancelled')
    return results


def main():
    # Create outputs first, so runtime/install/key-entry failures also leave a report.
    out = Path('decodo_fixed_batch_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f'))
    out.mkdir()
    results, client, error, symbols = {}, None, '', list(SYMBOLS)
    started = time.monotonic()
    try:
        if importlib.util.find_spec('requests') is None:
            subprocess.check_call([sys.executable,'-m','pip','install','-q','requests>=2.32,<3'])
        import requests
        symbols = load_symbols()
        needed = math.ceil(len(symbols)/BATCH_SIZE)
        if needed > MAX_DECODO_CALLS:
            raise StopRun(f'{needed} calls required, but budget is {MAX_DECODO_CALLS}; no request sent')
        (out/'symbols.txt').write_text('\n'.join(symbols))
        (out/'config.json').write_text(json.dumps(dict(symbol_count=len(symbols),
            batch_size=BATCH_SIZE, planned_calls=needed, hard_call_budget=MAX_DECODO_CALLS,
            cookie_setup_calls=0, automatic_retries=0, single_symbol_fallback=False,
            price_endpoint=YAHOO_ENDPOINT, range='1d', interval='1m', include_prepost=True,
            decodo_endpoint=DECODO_ENDPOINT, proxy_pool=PROXY_POOL, geo=GEO),indent=2))
        token = (os.environ.get('DECODO_API_KEY') or os.environ.get('DECODO_API_TOKEN')
                 or getpass('Decodo Web Data API key (hidden): ')).strip()
        if token.lower().startswith('bearer '): token = token[7:].strip()
        if not token: raise StopRun('API key is empty')
        print(f'{len(symbols)} symbols; {needed} planned calls; zero cookie/crumb calls.',flush=True)
        print('First batch validates access before the remaining calls. No automatic retries.',flush=True)
        with requests.Session() as session:
            session.trust_env = False
            client = Client(session, token, out)
            try:
                results = perform(client, symbols, out)
            finally:
                if (out/'prices.csv').exists():
                    with (out/'prices.csv').open() as f:
                        results = {r['symbol']:r for r in csv.DictReader(f)}
    except (Exception, KeyboardInterrupt) as exc:
        error = str(exc) if isinstance(exc, StopRun) else type(exc).__name__
    finally:
        rows = [results.get(s, missing_row(s)) for s in symbols]
        write_csv(out/'prices.csv', rows)
        attempts = client.attempts if client else []
        write_csv(out/'request_report.csv', attempts)
        (out/'failed_symbols.txt').write_text('\n'.join(r['symbol'] for r in rows if r['status']!='ok'))
        valid = sum(r['status']=='ok' for r in rows)
        report = f'''DECODO / COOKIE-INDEPENDENT BATCHED PRICE TEST
Completed UTC: {utc()}
Requested symbols: {len(symbols)}
Symbols per batch: {BATCH_SIZE}
Planned calls: {math.ceil(len(symbols)/BATCH_SIZE)}
Hard client-call budget: {MAX_DECODO_CALLS}
ACTUAL Decodo API calls: {len(attempts)}
Cookie / crumb setup calls: 0
Valid timestamped prices: {valid}/{len(symbols)}
Stopped/error: {error or 'none'}
Elapsed seconds: {time.monotonic()-started:.2f}
Encoded Decodo response body bytes: {sum(r['encoded_response_bytes'] for r in attempts):,}
Decoded Decodo response body bytes: {sum(r['decoded_response_bytes'] for r in attempts):,}
Target content UTF-8 bytes: {sum(r['target_content_bytes'] for r in attempts):,}

20 symbols share ONE Yahoo Spark URL and ONE Decodo POST. For 1000 symbols,
50 planned calls replace 1000 individual-symbol calls: 95% fewer client POSTs.
The first 20-symbol batch doubles as the access/validation check, not an extra call.
The first batch must fully validate; otherwise the remaining calls are cancelled.
Later missing symbols are recorded, not retried individually. A zero-valid batch,
HTTP failure or malformed response stops the rest. No retries or rendering fallback.

PRICE MEANING
This uses Spark close data, not the cookie-dependent v7 quote endpoint.
price_source=one_minute_close: latest valid one-minute candle close returned.
price_source=regular/pre/post_market_quote: timestamped metadata price if newer.
A candle timestamp identifies the candle interval, not necessarily the exact trade time.
price_age_seconds uses that timestamp. Prices may be delayed or from a previous
session. Session is unknown for candle-derived prices. No current timestamp is
substituted for an old price. Bid/ask, volume and full OHLCV are not returned.
Separate batches do not form an atomic simultaneous snapshot.

USAGE
Client POST counts do not establish billable credits: Decodo accounting, request
complexity and internal retries may differ. Compare your dashboard before/after.
No API key, cookies or crumb values are written to files. No login or proxy
credentials required. This still depends on Yahoo access through your Decodo plan;
it cannot guarantee upstream availability. The diagnostic report records envelope
field names and HTTP statuses, not secret-bearing response headers or content.

OUTPUTS
prices.csv: one row per requested symbol, with source, timestamp and status.
request_report.csv: actual calls, statuses, timings, sizes and response field names.
failed_symbols.txt: missing, invalid or unattempted symbols.
'''
        (out/'FINAL_REPORT.txt').write_text(report)
        archive = out.with_suffix('.zip')
        with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
            for p in sorted(out.iterdir()):
                if p.is_file(): z.write(p,p.name)
        print('\n'+report)
        print('RESULT ZIP:', archive.resolve(), flush=True)
        if AUTO_DOWNLOAD_IN_COLAB:
            try:
                from google.colab import files
                files.download(str(archive))
            except ImportError: pass
            except Exception: print('Download the ZIP from the Colab Files panel.')
        return archive


if __name__ == '__main__':
    main()
