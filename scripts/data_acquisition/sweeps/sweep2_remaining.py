"""
Global Jurisdiction Sweep Part 2: 30+ New Countries
=====================================================
Tests every remaining open property data source NOT yet tried.
"""
import requests, json, time, os
HEADERS = {"User-Agent": "Mozilla/5.0 Properlytic/5.0 (Global Research)"}
RESULTS = {}

def get(url, timeout=20, **kw):
    try:
        h = kw.pop("headers", HEADERS)
        return requests.get(url, timeout=timeout, headers=h, allow_redirects=True, **kw)
    except: return None

def post(url, timeout=20, **kw):
    try:
        h = kw.pop("headers", HEADERS)
        return requests.post(url, timeout=timeout, headers=h, **kw)
    except: return None

def log(country, source, status, n, detail):
    RESULTS[f"{country}|{source}"] = {"country":country,"source":source,"status":status,"n_rows":n,"detail":detail}
    print(f"  [{'PASS' if status=='pass' else 'FAIL'}] {country}/{source}: {n} rows — {detail}")

# ═══════════ EUROPEAN ═══════════

def try_portugal():
    print("\n>>> PORTUGAL INE")
    # INE API
    r = get("https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0011281&lang=EN", timeout=20)
    if r and r.status_code == 200 and len(r.content) > 100:
        try:
            data = r.json()
            n = len(str(data))
            log("Portugal","INE","pass",1 if n>200 else 0,f"API returned {n} chars")
            return
        except: pass
    r2 = get("https://www.ine.pt/xportal/xmain?xpid=INE&xpgid=ine_indicadores&contecto=pi&indOcorrCod=0011281&selTab=tab0", timeout=20)
    log("Portugal","INE","pass" if r2 and r2.status_code==200 else "fail", 0,
        "Portal accessible" if r2 and r2.status_code==200 else "Timeout")

def try_belgium():
    print("\n>>> BELGIUM StatBel")
    r = get("https://statbel.fgov.be/en/open-data/house-price-index", timeout=20)
    if r and r.status_code == 200:
        log("Belgium","StatBel HPI","pass",0,"Page accessible")
    else:
        log("Belgium","StatBel","fail",0,"Timeout")
    # Statbel open data API
    r2 = get("https://statbel.fgov.be/sites/default/files/files/opendata/bouwvergunningen/TF_BUILDING_PERMITS.zip", timeout=20)
    if r2 and r2.status_code == 200:
        log("Belgium","StatBel Permits","pass",1,f"ZIP download available ({len(r2.content)//1024}KB)")

def try_austria():
    print("\n>>> AUSTRIA Statistik")
    r = get("https://data.statistik.gv.at/web/meta.jsp?dataset=OGD_gpi_ext_GPIMonat_1", timeout=20)
    if r and r.status_code == 200:
        log("Austria","Statistik GPI","pass",0,"Metadata accessible")
    # Try actual data download
    r2 = get("https://data.statistik.gv.at/opendata/OGD_gpi_ext_GPIMonat_1.csv", timeout=20)
    if r2 and r2.status_code == 200 and len(r2.content) > 200:
        lines = r2.text.strip().split("\n")
        log("Austria","Statistik CSV","pass",len(lines)-1,f"{len(lines)-1} rows of property price index")
    else:
        log("Austria","Statistik CSV","fail",0,"Not accessible" if not r2 else f"HTTP {r2.status_code}")

def try_greece():
    print("\n>>> GREECE Bank of Greece")
    r = get("https://www.bankofgreece.gr/en/statistics/real-estate-market", timeout=20)
    log("Greece","Bank of Greece","pass" if r and r.status_code==200 else "fail",0,
        "Real estate stats page" if r and r.status_code==200 else "Timeout")

def try_croatia():
    print("\n>>> CROATIA DZS")
    r = get("https://web.dzs.hr/default_e.htm", timeout=20)
    log("Croatia","DZS","pass" if r and r.status_code==200 else "fail",0,
        "Portal accessible" if r and r.status_code==200 else "Timeout")

def try_romania():
    print("\n>>> ROMANIA INS")
    r = get("https://insse.ro/cms/en/content/statistical-data", timeout=20)
    log("Romania","INS","pass" if r and r.status_code==200 else "fail",0,
        "Portal accessible" if r and r.status_code==200 else "Timeout")

def try_hungary():
    print("\n>>> HUNGARY KSH")
    r = get("https://www.ksh.hu/stadat_files/lak/en/lak0020.html", timeout=20)
    log("Hungary","KSH","pass" if r and r.status_code==200 else "fail",0,
        "Housing prices page" if r and r.status_code==200 else "Timeout")

def try_lithuania():
    print("\n>>> LITHUANIA Registru Centras")
    r = get("https://www.registrucentras.lt/ntr/stat/", timeout=20)
    log("Lithuania","Registru Centras","pass" if r and r.status_code==200 else "fail",0,
        "Property stats portal" if r and r.status_code==200 else "Timeout")
    # Open data
    r2 = get("https://osp.stat.gov.lt/web/guest/statistiniu-rodikliu-analize?hash=d1b1fb5f-d16b-4cd4-a5d0-6f95ccf93e98", timeout=20)
    if r2 and r2.status_code == 200:
        log("Lithuania","OSP Stats","pass",0,"Statistics portal accessible")

def try_latvia():
    print("\n>>> LATVIA CSB")
    r = get("https://data.stat.gov.lv/pxweb/en/OSP_PUB/OSP_PUB__nekust__cenas/NCG020.px/", timeout=20)
    if r and r.status_code == 200:
        log("Latvia","CSB PxWeb","pass",0,"Property price table exists")
        # Try to get data
        r2 = post("https://data.stat.gov.lv/api/v1/en/OSP_PUB/nekust/cenas/NCG020.px",
                  json={"query":[],"response":{"format":"json-stat2"}}, timeout=15)
        if r2 and r2.status_code == 200:
            try:
                vals = r2.json().get("value",[])
                log("Latvia","CSB Data","pass",len([v for v in vals if v]),f"{len([v for v in vals if v])} price values")
            except: pass
    else:
        log("Latvia","CSB","fail",0,"Timeout")

def try_slovenia():
    print("\n>>> SLOVENIA GURS")
    r = get("https://www.e-prostor.gov.si/zbirke-prostorskih-podatkov/nepremicnine/evidenca-trga-nepremicnin/", timeout=20)
    log("Slovenia","GURS ETN","pass" if r and r.status_code==200 else "fail",0,
        "Property market registry" if r and r.status_code==200 else "Timeout")

def try_luxembourg():
    print("\n>>> LUXEMBOURG STATEC")
    r = get("https://statistiques.public.lu/en/themes/logement.html", timeout=20)
    log("Luxembourg","STATEC","pass" if r and r.status_code==200 else "fail",0,
        "Housing stats page" if r and r.status_code==200 else "Timeout")

def try_iceland():
    print("\n>>> ICELAND Statistics")
    r = get("https://px.hagstofa.is/pxen/pxweb/en/Efnahagur/Efnahagur__visitolur__1_vnv__1_ibudarhusnaeidi/VIS01100.px/", timeout=20)
    if r and r.status_code == 200:
        log("Iceland","Hagstofa PxWeb","pass",0,"Property price index table")
    else:
        log("Iceland","Hagstofa","fail",0,"Timeout")

def try_turkey():
    print("\n>>> TURKEY TURKSTAT")
    r = get("https://data.tuik.gov.tr/Bulten/Index?p=House-Sales-Statistics-December-2024-53791", timeout=20)
    log("Turkey","TURKSTAT","pass" if r and r.status_code==200 else "fail",0,
        "House sales bulletin" if r and r.status_code==200 else "Timeout")

# ═══════════ MIDDLE EAST / AFRICA ═══════════

def try_israel():
    print("\n>>> ISRAEL CBS")
    r = get("https://www.cbs.gov.il/en/subjects/Pages/Dwellings-and-Construction.aspx", timeout=20)
    log("Israel","CBS","pass" if r and r.status_code==200 else "fail",0,
        "Dwellings page" if r and r.status_code==200 else "Timeout")

def try_saudi():
    print("\n>>> SAUDI ARABIA GASTAT")
    r = get("https://www.stats.gov.sa/en/814", timeout=20)
    log("Saudi Arabia","GASTAT","pass" if r and r.status_code==200 else "fail",0,
        "Real estate index page" if r and r.status_code==200 else "Timeout")

def try_kenya():
    print("\n>>> KENYA KNBS")
    r = get("https://www.knbs.or.ke/", timeout=20)
    log("Kenya","KNBS","pass" if r and r.status_code==200 else "fail",0,
        "Portal" if r and r.status_code==200 else "Timeout")

def try_nigeria():
    print("\n>>> NIGERIA NBS")
    r = get("https://nigerianstat.gov.ng/", timeout=20)
    log("Nigeria","NBS","pass" if r and r.status_code==200 else "fail",0,
        "Portal" if r and r.status_code==200 else "Timeout")

def try_egypt():
    print("\n>>> EGYPT CAPMAS")
    r = get("https://www.capmas.gov.eg/", timeout=20)
    log("Egypt","CAPMAS","pass" if r and r.status_code==200 else "fail",0,
        "Portal" if r and r.status_code==200 else "Timeout")

def try_morocco():
    print("\n>>> MOROCCO HCP")
    r = get("https://www.hcp.ma/", timeout=20)
    log("Morocco","HCP","pass" if r and r.status_code==200 else "fail",0,
        "Portal" if r and r.status_code==200 else "Timeout")

# ═══════════ ASIA-PACIFIC ═══════════

def try_thailand():
    print("\n>>> THAILAND Bank of Thailand")
    r = get("https://www.bot.or.th/en/statistics/economic-and-financial/real-estate.html", timeout=20)
    log("Thailand","BOT","pass" if r and r.status_code==200 else "fail",0,
        "Real estate stats" if r and r.status_code==200 else "Timeout")

def try_malaysia():
    print("\n>>> MALAYSIA NAPIC")
    r = get("https://napic.jpph.gov.my/portal/web/guest/main", timeout=20)
    log("Malaysia","NAPIC","pass" if r and r.status_code==200 else "fail",0,
        "Portal" if r and r.status_code==200 else "Timeout")

def try_indonesia():
    print("\n>>> INDONESIA BPS")
    r = get("https://www.bps.go.id/en/statistics-table?subject=517", timeout=20)
    log("Indonesia","BPS","pass" if r and r.status_code==200 else "fail",0,
        "Housing stats" if r and r.status_code==200 else "Timeout")

def try_philippines():
    print("\n>>> PHILIPPINES PSA")
    r = get("https://psa.gov.ph/construction-real-estate", timeout=20)
    log("Philippines","PSA","pass" if r and r.status_code==200 else "fail",0,
        "Construction/RE page" if r and r.status_code==200 else "Timeout")

# ═══════════ LATIN AMERICA ═══════════

def try_argentina():
    print("\n>>> ARGENTINA INDEC")
    r = get("https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-3-30", timeout=20)
    log("Argentina","INDEC","pass" if r and r.status_code==200 else "fail",0,
        "Construction stats" if r and r.status_code==200 else "Timeout")

def try_chile():
    print("\n>>> CHILE INE")
    r = get("https://www.ine.gob.cl/estadisticas/economia/indice-de-precios/indice-de-precios-de-viviendas", timeout=20)
    log("Chile","INE HPI","pass" if r and r.status_code==200 else "fail",0,
        "Housing price index page" if r and r.status_code==200 else "Timeout")

def try_peru():
    print("\n>>> PERU BCRP")
    r = get("https://estadisticas.bcrp.gob.pe/estadisticas/series/mensuales/resultados/PN38927PM/html", timeout=20)
    log("Peru","BCRP","pass" if r and r.status_code==200 else "fail",0,
        "Housing price series" if r and r.status_code==200 else "Timeout")

def try_brazil_fipe():
    print("\n>>> BRAZIL FIPE ZAP")
    r = get("https://www.fipe.org.br/en/indices/fipezap/", timeout=20)
    log("Brazil","FIPE ZAP","pass" if r and r.status_code==200 else "fail",0,
        "Property price index" if r and r.status_code==200 else "Timeout")

def try_uruguay():
    print("\n>>> URUGUAY INE")
    r = get("https://www.gub.uy/instituto-nacional-estadistica/datos-y-estadisticas/estadisticas", timeout=20)
    log("Uruguay","INE","pass" if r and r.status_code==200 else "fail",0,
        "Stats portal" if r and r.status_code==200 else "Timeout")

# ═══════════ EUROSTAT (covers all EU at once) ═══════════

def try_eurostat():
    print("\n>>> EUROSTAT (All EU)")
    # House Price Index for all EU countries
    url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hpi_q?format=TSV&startPeriod=2020-Q1"
    r = get(url, timeout=25)
    if r and r.status_code == 200 and len(r.content) > 500:
        lines = r.text.strip().split("\n")
        log("Eurostat","HPI (all EU)","pass",len(lines)-1,f"{len(lines)-1} country-quarter HPI observations")
        return r.text
    else:
        log("Eurostat","HPI","fail",0,"Timeout" if not r else f"HTTP {r.status_code}")
    return None

# ═══════════ MAIN ═══════════

def main():
    t0 = time.time()
    print("="*70)
    print("GLOBAL SWEEP PART 2: 30+ NEW JURISDICTIONS")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # European
    try_eurostat()
    try_portugal()
    try_belgium()
    try_austria()
    try_greece()
    try_croatia()
    try_romania()
    try_hungary()
    try_lithuania()
    try_latvia()
    try_slovenia()
    try_luxembourg()
    try_iceland()
    try_turkey()

    # Middle East / Africa
    try_israel()
    try_saudi()
    try_kenya()
    try_nigeria()
    try_egypt()
    try_morocco()

    # Asia-Pacific
    try_thailand()
    try_malaysia()
    try_indonesia()
    try_philippines()

    # Latin America
    try_argentina()
    try_chile()
    try_peru()
    try_brazil_fipe()
    try_uruguay()

    # Summary
    print("\n" + "="*70)
    print("SWEEP 2 RESULTS")
    print("="*70)
    passes = sum(1 for r in RESULTS.values() if r["status"]=="pass")
    fails = sum(1 for r in RESULTS.values() if r["status"]=="fail")
    total_data = sum(r["n_rows"] for r in RESULTS.values())
    print(f"  Sources tested: {len(RESULTS)}")
    print(f"  PASS: {passes}  |  FAIL: {fails}")
    print(f"  Rows with data: {total_data}")
    print("\n  PASSED:")
    for k,r in sorted(RESULTS.items()):
        if r["status"]=="pass":
            print(f"    {r['country']:20s} {r['source']:25s} {r['n_rows']:5d} rows  {r['detail']}")
    print("\n  FAILED:")
    for k,r in sorted(RESULTS.items()):
        if r["status"]=="fail":
            print(f"    {r['country']:20s} {r['source']:25s} {r['detail']}")

    out = os.path.join(os.path.dirname(__file__), "sweep2_results.json")
    with open(out, "w") as f:
        json.dump({"elapsed":round(time.time()-t0,1),"n_sources":len(RESULTS),
                   "n_pass":passes,"n_fail":fails,"total_rows":total_data,
                   "results":RESULTS}, f, indent=2)
    print(f"\n  Results: {out}")
    print(f"  Completed in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
