import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# DocGen primary database. Kept separate from HID's databases so existing DocGen
# records and attendance processes continue to use the same storage.
SQL_SERVER = r"192.168.20.100"
SQL_DATABASE = "DocGenDB"
SQL_TRUSTED = False
SQL_USERNAME = "sa"
SQL_PASSWORD = "Prodoc_23"

# HID-owned DocGen assets.
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "hr_docgen", "output")
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "word_templates")
