from celery import Celery
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
import redis
import os
import io
import json
import uuid
import mysql.connector
from pymongo import MongoClient
from openpyxl import Workbook
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
from reportlab.pdfgen import canvas

from pydantic import BaseModel

apps = FastAPI()

celerys = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')


class RequestData(BaseModel):
    user_id: int
    transaction_id: str
    
load_dotenv()


def fetch_user_data(user_id):
    connection = mysql.connector.connect(
        host=os.getenv("mysql_host"),
        user=os.getenv("mysql_user"),
        password=os.getenv("mysql_password"),
        database=os.getenv("mysql_database"),
        port=os.getenv("mysql_port"),
    )
    cursor = connection.cursor()
    query = (
        "SELECT user_name, user_email, user_address FROM userdata WHERE user_id = %s"
    )
    cursor.execute(query, (user_id,))
    user_data = cursor.fetchone()
    connection.close()
    return user_data


def fetch_transaction_data(transaction_id):
    mongo_url = os.getenv("mongo_url")
    client = MongoClient(mongo_url) 
    db = client["task_invoice"]
    collection = db["Transaction"]
    transaction_data = collection.find_one({"transaction_id": transaction_id})
    return transaction_data


redis_host = os.getenv("redis_host")
redis_port = os.getenv("redis_port")
    
redis_client = redis.Redis(host=redis_host, port=redis_port)


def store_request_info(request_id, request_data):
    """
    Store request information in Redis.
    """
    redis_key = f"request:{request_id}"
    redis_client.set(redis_key, json.dumps(request_data))


def fetch_request_info(request_id):
    """
    Fetch request information from Redis using request ID.
    """
    redis_key = f"request:{request_id}"
    request_data_json = redis_client.get(redis_key)
    if request_data_json:
        return json.loads(request_data_json)
    else:
        return None
    

def retrieve_request_info(request, request_id):
    request_data = fetch_request_info(request_id)
    if request_data:
        user_id = request_data["user_id"]
        transaction_id = request_data["transaction_id"]
        return (request_data)
    else:
        return HTTPException({"error": "Request ID not found"}, status=404)


def create_excel_file(user_id, transaction_id):
    wb = Workbook()
    ws = wb.active
    user_data = fetch_user_data(user_id)
    transaction_data = fetch_transaction_data(transaction_id)
    print("TRANSACTION", transaction_id)

    ws.append(
        [
            "Invoice",
            "User Name",
            "User Email",
            "User Address",
            "Item Bought",
            "Quantity",
            "Price",
            "Description",
            "Date",
            "Order Number",
        ]
    )
    for item in transaction_data["items"]:
        ws.append(
            [
                "Invoice",
                user_data[0],
                user_data[1],
                user_data[2],
                item["item_name"],
                item["quantity"],
                item["price"],
                item["description"],
                transaction_data["date"],
                transaction_data["order_number"],
            ]
        )

    wb.save("invoice.xlsx")

def generate_invoice_pdf(user_id, transaction_id):
    try:
        
        user_data = fetch_user_data(user_id)
        transaction_data = fetch_transaction_data(transaction_id)
        print(user_id)
        print(transaction_id)

        buffer = io.BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)

        p.setFont("Helvetica-Bold", 18)
        p.setFillColorRGB(0, 0, 0)
        p.drawCentredString(297.5, 750, "Invoice")

        p.setFont("Helvetica-Bold", 12)
        p.setFillColorRGB(0, 0, 0)
        p.drawString(100, 700, "Name:")
        p.setFont("Helvetica", 12)
        p.drawString(150, 700, f"{user_data[0]}")
        p.setFont("Helvetica-Bold", 12)
        p.drawString(100, 680, "Email:")
        p.setFont("Helvetica", 12)
        p.drawString(150, 680, f"{user_data[1]}")
        p.setFont("Helvetica-Bold", 12)
        p.drawString(100, 640, "Sold By:")

        p.setFont("Helvetica", 12)
        if transaction_data:
            # Ensure transaction_data contains the "items" field
            if "items" in transaction_data:
                sold_by_address = transaction_data["items"][0]["sold_by"]    
                address_words = sold_by_address.split()
        line_y = 620
        line_width = 380
        line = []

        for word in address_words:
            if p.stringWidth(" ".join(line + [word]), "Helvetica", 12) > line_width:
                p.drawString(100, line_y, " ".join(line))
                line_y -= 20
                line = [word]
            else:
                line.append(word)

        p.drawString(100, line_y, " ".join(line))

        p.setFont("Helvetica-Bold", 12)
        p.drawString(100, 580, "Shipping Address:")
        p.setFont("Helvetica", 12)
        p.drawString(100, 560, f"{user_data[2]}")

        p.setFont("Helvetica-Bold", 12)
        p.drawString(400, 700, f"Date:")
        p.setFont("Helvetica", 12)
        p.drawString(430, 700, f"{transaction_data['date']}")
        p.setFont("Helvetica-Bold", 12)
        p.drawString(400, 680, f"Payment Mode:")
        p.setFont("Helvetica", 12)
        p.drawString(500, 680, f"{transaction_data['payment_mode']}")
        p.setFont("Helvetica-Bold", 12)
        p.drawString(100, 520, f"Order Number:")
        p.setFont("Helvetica", 12)
        p.drawString(200, 520, f"{transaction_data['order_number']}")

        data = [
            [
                "Item Bought",
                "Item Description",
                "Quantity",
                "Price",
                "Tax",
                "Total Amount Payable",
            ]
        ]
        total_amount_payable = 0

        for item in transaction_data["items"]:
            amount = item["quantity"] * item["price"]
            tax = amount * 0.18
            total_amount = amount + tax
            total_amount_payable += total_amount

            data.append(
                [
                    item["item_name"],
                    item["description"],
                    item["quantity"],
                    f"rs. {item['price']:.2f}",
                    "18%",
                    f"{total_amount:.2f}",
                ]
            )

        table = Table(data)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 12),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                ]
            )
        )

        table_y = 400
        table.wrapOn(p, 500, 600)
        table.drawOn(p, 100, table_y)

        p.setFont("Helvetica-Bold", 12)
        p.drawString(330, 380, "Total Amount Payable: rs. ")
        p.setFont("Helvetica", 12)
        p.drawString(480, 380, f"{total_amount_payable:.2f}")
        
        file_name = f"invoice_{user_id}_{transaction_id}.pdf"  
        file_path = os.path.join("C:\\Users\\AnaabRaut\\Desktop\\reyrent", file_name)  

        p.save()

        with open(file_path, "wb") as f:
            f.write(buffer.getvalue())

        buffer.close()

        return file_name

        # p.showPage()
        # p.save()

        # buffer.seek(0)
        # return FileResponse(buffer, as_attachment=True, filename="invoice.pdf")
    #file location from .env
    #return file name
        # buffer.seek(0)
        # pdf_data = buffer.getvalue()
        # return pdf_data
        # return {
        #     'pdf_data': pdf_data,
        #     'user_id': user_id,
        #     'transaction_id': transaction_id,
        # }    

    
    except Exception as e:
        print(f"PDF generation failed: {e}") 
        return None


@celerys.task
def process(user_id,transaction_id):
    print("PROCESS STARTED")
    create_excel_file(user_id, transaction_id)
    generate_invoice_pdf(user_id, transaction_id)
    # return  generate_invoice_pdf(user_id, transaction_id)

    
@apps.post("/generate-invoice/")
def get_invoice(rd: RequestData):
    request_id = str(uuid.uuid4())  
    
    request_data = {
        "request_id": request_id,
        "user_id": rd.user_id,
        "transaction_id": rd.transaction_id,
        "current_state": "pending"  
    }
    
    try:
        store_request_info(request_id, request_data)
        print("Request data:", request_data)
        result = process.delay(rd.user_id, rd.transaction_id)
        print(result.backend)
        return {"message": "File processing has been started"}    
    except Exception as e:
        request_data["current_state"] = "failed"
        print(f"PDF generation failed: {e}")    
        raise HTTPException(detail = "PDF generation failed", status_code=500)

    
        