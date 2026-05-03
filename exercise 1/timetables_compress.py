import xml.etree.ElementTree as ET
from pathlib import Path
from lxml import etree
import time

# access XSLT-file
XSLT_PATH = Path("compress.xsl")

# load and parse XSLT-file
def load_xslt():
    if not XSLT_PATH.exists():
        raise FileNotFoundError(f"XSLT file not found: {XSLT_PATH}")
    return etree.XSLT(etree.parse(str(XSLT_PATH)))

# transform XML-files based on the XSLT-script
def apply_xslt(input_path, output_path, xslt):
    dom = etree.parse(str(input_path))
    newdom = xslt(dom)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
        f.write(etree.tostring(newdom, encoding="UTF-8", pretty_print=False))

# start compression and define output (folder)
def compress_all(base="timetables", out="timetables_compressed"):
    # load files to compress
    xslt = load_xslt()

    files = list(Path(base).rglob("*timetable.xml"))
    print("Compressing", len(files), "files...")

    # compress single file
    for f in files:
        rel = f.relative_to(base)
        out_file = Path(out) / rel
        apply_xslt(f, out_file, xslt)

    print("Done compressing.")

if __name__ == "__main__":
    # start timer
    start = time.perf_counter()

    # start of "timetables"-compression
    compress_all("timetables", "timetables_compressed")

    print("success")

    # end timer
    end= time.perf_counter()
    print("Laufzeit:", round(end - start, 2), "Sekunden")