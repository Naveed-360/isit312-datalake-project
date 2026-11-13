"""M3 (Fawaz): generate dummy medical files and metadata.csv.

Makes synthetic files for the first 200 patients in Afrith's patients.csv:
  200 scan images (X-ray / MRI, PNG), 100 lab reports (PDF), 50 patient documents (PDF)
plus metadata.csv linking every file to its patient, branch, date and type.
All content is fictional and labelled "SYNTHETIC DATA - NOT FOR CLINICAL USE".

Run on your laptop from the project folder (needs: pip install pillow reportlab):
  python jobs/m3/generate_medical_files.py
Reads  data/m1/patients.csv
Writes data/m3/images/, data/m3/lab_reports/, data/m3/documents/, data/m3/metadata.csv
Then upload them with jobs/m3/file_upload.py (see the README).

Optional: put a logo at data/m3/assets/alpha_logo.png to print it on the PDFs.
"""
import csv
import random
from pathlib import Path

from PIL import Image, ImageDraw
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.colors import black, white, HexColor

project_folder = Path(__file__).resolve().parents[2]   # jobs/m3 -> project folder

csv_file = project_folder / "data" / "m1" / "patients.csv"
output_folder = project_folder / "data" / "m3"

images_folder = output_folder / "images"
reports_folder = output_folder / "lab_reports"
documents_folder = output_folder / "documents"

for folder in [images_folder, reports_folder, documents_folder]:
    folder.mkdir(parents=True, exist_ok=True)

with open(csv_file, "r", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)
    patients = list(reader)

print("Total patients:", len(patients))

if len(patients) < 200:
    raise ValueError("At least 200 patients are required.")

logo_path = output_folder / "assets" / "alpha_logo.png"
logo_ready = output_folder / "assets" / "logo_ready.jpg"

if logo_path.exists():
    with Image.open(logo_path) as img:
        img = img.convert("RGBA")
        img.thumbnail((400, 400))

        background = Image.new("RGB", img.size, "white")
        background.paste(img, (0, 0), img)

        background.save(logo_ready, "JPEG", quality=90)

    print("Company logo prepared successfully.")
else:
    print("Company logo not found (optional).")
    logo_ready = None

random.seed(42)

# ---------- 200 dummy scan images ----------
for i in range(200):
    patient = patients[i]
    patient_id = patient["patient_id"]

    image = Image.new("RGB", (400, 400), "black")
    draw = ImageDraw.Draw(image)

    for j in range(8):
        x = random.randint(50, 300)
        y = random.randint(50, 300)
        size = random.randint(20, 80)

        draw.ellipse(
            (x, y, x + size, y + size),
            outline="gray",
            width=3
        )

    draw.text(
        (20, 20),
        "Dummy Scan - %s" % patient_id,
        fill="white"
    )

    filename = "scan_%s.png" % patient_id
    image.save(images_folder / filename)

print("200 synthetic medical images generated!")

# ---------- 100 lab reports ----------
for i in range(100):
    patient = patients[i]
    patient_id = patient["patient_id"]

    filename = "lab_report_%s.pdf" % patient_id
    file_path = reports_folder / filename

    pdf = canvas.Canvas(str(file_path), pagesize=A4)
    width, height = A4

    pdf.setTitle("Laboratory Report")
    pdf.setAuthor("Alpha Health Care - Synthetic Data")

    pdf.setFillColor(white)
    pdf.rect(0, 0, width, height, fill=1, stroke=0)

    if logo_ready is not None:
        pdf.drawImage(
            str(logo_ready),
            48,
            height - 145,
            width=145,
            height=115,
            preserveAspectRatio=True
        )

    pdf.setStrokeColor(black)
    pdf.setLineWidth(1)
    pdf.line(210, height - 145, 210, height - 35)

    pdf.setFillColor(black)
    pdf.setFont("Helvetica-Bold", 21)
    pdf.drawString(230, height - 95, "LABORATORY REPORT")

    pdf.setLineWidth(1.5)
    pdf.line(40, height - 170, width - 40, height - 170)

    pdf.setFont("Helvetica-Bold", 15)
    pdf.drawString(45, height - 210, "Patient Information")

    pdf.setLineWidth(0.8)
    pdf.roundRect(
        45, height - 305,
        width - 90, 75,
        6, fill=0, stroke=1
    )

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(60, height - 255, "Patient ID:")
    pdf.drawString(60, height - 282, "Branch ID:")
    pdf.drawString(330, height - 255, "Date:")
    pdf.drawString(330, height - 282, "Test:")

    pdf.setFont("Helvetica", 11)
    pdf.drawString(130, height - 255, patient_id)
    pdf.drawString(130, height - 282, str(patient["branch_id"]))
    pdf.drawString(365, height - 255, "2026-11-03")
    pdf.drawString(365, height - 282, "Blood Panel")

    pdf.setFont("Helvetica-Bold", 15)
    pdf.drawString(
        45, height - 355,
        "Laboratory Test Results"
    )

    table_x = 45
    table_top = height - 390
    table_width = width - 90
    row_height = 40

    col1 = 220
    col2 = 130

    pdf.setFillColor(HexColor("#E8E8E8"))
    pdf.rect(
        table_x,
        table_top - row_height,
        table_width,
        row_height,
        fill=1,
        stroke=0
    )

    pdf.setStrokeColor(black)
    pdf.setFillColor(black)
    pdf.setLineWidth(0.8)

    pdf.rect(
        table_x,
        table_top - row_height * 4,
        table_width,
        row_height * 4,
        fill=0,
        stroke=1
    )

    pdf.line(
        table_x + col1,
        table_top,
        table_x + col1,
        table_top - row_height * 4
    )

    pdf.line(
        table_x + col1 + col2,
        table_top,
        table_x + col1 + col2,
        table_top - row_height * 4
    )

    for row in range(1, 4):
        y = table_top - row * row_height
        pdf.line(table_x, y, table_x + table_width, y)

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(65, table_top - 26, "Test")
    pdf.drawString(285, table_top - 26, "Result")
    pdf.drawString(415, table_top - 26, "Unit")

    results = [
        ("Hemoglobin", "13.5", "g/dL"),
        ("White Blood Cells", "7.2", "x10^9/L"),
        ("Platelets", "250", "x10^9/L")
    ]

    pdf.setFont("Helvetica", 11)

    for index, (test, result, unit) in enumerate(results):
        y = table_top - 66 - index * row_height

        pdf.drawString(65, y, test)
        pdf.drawString(285, y, result)
        pdf.drawString(415, y, unit)

    pdf.setFont("Helvetica-Oblique", 9)
    pdf.drawString(
        45,
        105,
        "All results are fictional and generated for academic testing."
    )

    pdf.setLineWidth(1)
    pdf.line(40, 85, width - 40, 85)

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawCentredString(
        width / 2,
        60,
        "SYNTHETIC DATA - NOT FOR CLINICAL USE"
    )

    pdf.save()

print("100 professional laboratory reports generated!")

# ---------- 50 patient documents ----------
for i in range(50):
    patient = patients[i]
    patient_id = patient["patient_id"]

    filename = "patient_document_%s.pdf" % patient_id
    file_path = documents_folder / filename

    pdf = canvas.Canvas(str(file_path), pagesize=A4)
    width, height = A4

    if logo_ready is not None:
        pdf.drawImage(
            str(logo_ready),
            45,
            height - 145,
            width=145,
            height=115,
            preserveAspectRatio=True
        )

    pdf.setFillColor(black)
    pdf.setFont("Helvetica-Bold", 18)
    pdf.drawString(220, height - 90, "PATIENT DOCUMENT")

    pdf.setLineWidth(1)
    pdf.line(40, height - 170, width - 40, height - 170)

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(45, height - 215, "Patient Information")

    pdf.setFont("Helvetica", 11)
    pdf.drawString(45, height - 255, "Patient ID: %s" % patient_id)
    pdf.drawString(
        45, height - 280,
        "Branch ID: %s" % patient["branch_id"]
    )
    pdf.drawString(45, height - 305, "Date: 2026-11-03")
    pdf.drawString(
        45, height - 330,
        "Document Type: Medical Visit Summary"
    )

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(45, height - 390, "Visit Summary")

    pdf.setFont("Helvetica", 11)
    pdf.drawString(
        45, height - 425,
        "Routine patient check-up completed."
    )
    pdf.drawString(
        45, height - 450,
        "No additional follow-up recorded."
    )

    pdf.setFont("Helvetica-Oblique", 9)
    pdf.drawString(
        45, 105,
        "This document contains fictional academic test data."
    )

    pdf.line(40, 85, width - 40, 85)

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawCentredString(
        width / 2,
        60,
        "SYNTHETIC DATA - NOT FOR CLINICAL USE"
    )

    pdf.save()

print("50 synthetic patient documents generated!")

# ---------- metadata.csv: one row per file ----------
metadata = []
file_counter = 1
report_date = "2026-11-03"

for i in range(200):
    patient = patients[i]
    patient_id = patient["patient_id"]

    file_type = "X-ray" if i % 2 == 0 else "MRI"
    filename = "scan_%s.png" % patient_id

    metadata.append({
        "file_id": "F%05d" % file_counter,
        "patient_id": patient_id,
        "branch_id": patient["branch_id"],
        "date": report_date,
        "file_type": file_type,
        "file_path": "images/%s" % filename
    })

    file_counter += 1

for i in range(100):
    patient = patients[i]
    patient_id = patient["patient_id"]

    metadata.append({
        "file_id": "F%05d" % file_counter,
        "patient_id": patient_id,
        "branch_id": patient["branch_id"],
        "date": report_date,
        "file_type": "Lab Report",
        "file_path": "lab_reports/lab_report_%s.pdf" % patient_id
    })

    file_counter += 1

for i in range(50):
    patient = patients[i]
    patient_id = patient["patient_id"]

    metadata.append({
        "file_id": "F%05d" % file_counter,
        "patient_id": patient_id,
        "branch_id": patient["branch_id"],
        "date": report_date,
        "file_type": "Medical Document",
        "file_path": "documents/patient_document_%s.pdf" % patient_id
    })

    file_counter += 1

metadata_file = output_folder / "metadata.csv"

with open(metadata_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=[
            "file_id",
            "patient_id",
            "branch_id",
            "date",
            "file_type",
            "file_path"
        ]
    )

    writer.writeheader()
    writer.writerows(metadata)

print("Metadata CSV created successfully!")
print("Total metadata records:", len(metadata))

missing_files = [
    row["file_path"]
    for row in metadata
    if not (output_folder / row["file_path"]).is_file()
]

print("Missing files:", len(missing_files))

if missing_files:
    print("Examples:", missing_files[:5])
