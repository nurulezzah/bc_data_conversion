from flask import Flask, render_template, request, send_file
import os
import re
import PyPDF2
import pdfplumber
import pandas as pd
import logging
from datetime import datetime
# Suppress verbose logs from pdfminer (used by pdfplumber)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')

app = Flask(__name__)

# @app.route('/')
# def home():
#     if 'username' in session:
#         return render_template('index.html', username=session['username'])
#     return redirect(url_for('login'))

# # Route for login page
# @app.route('/login', methods=['GET', 'POST'])
# def login():
#     if request.method == 'POST':
#         username = request.form['username']
#         password = request.form['password']
#         if check_login_credentials(username, password):
#             session['username'] = username
#             return redirect(url_for('home'))
#         else:
#             return 'Invalid username or password'
#     return render_template('login.html')

# Set the output folders for Excel and PDF processing to the new path
ERP_OUTPUT_FOLDER = '/home/bookcapital/output/erp'
OMS_OUTPUT_FOLDER = '/home/bookcapital/output/oms'
AWB_OUTPUT_FOLDER = '/home/bookcapital/output/awb'
POSLAJU_FOLDER = os.path.join(AWB_OUTPUT_FOLDER, 'poslaju')
DHL_FOLDER = os.path.join(AWB_OUTPUT_FOLDER, 'dhl')
NINJA_FOLDER = os.path.join(AWB_OUTPUT_FOLDER, 'ninja')
OTHERS_FOLDER = os.path.join(AWB_OUTPUT_FOLDER, 'others')
GDEX_FOLDER = os.path.join(AWB_OUTPUT_FOLDER, 'gdex')

# Ensure all necessary folders exist
os.makedirs(ERP_OUTPUT_FOLDER, exist_ok=True)
os.makedirs(OMS_OUTPUT_FOLDER, exist_ok=True)
os.makedirs(POSLAJU_FOLDER, exist_ok=True)
os.makedirs(DHL_FOLDER, exist_ok=True)
os.makedirs(NINJA_FOLDER, exist_ok=True)
os.makedirs(OTHERS_FOLDER, exist_ok=True)
os.makedirs(GDEX_FOLDER, exist_ok=True)



# Define allowed extensions for Excel files
ALLOWED_EXTENSIONS = {'xlsx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Excel processing functions (BC to ERP)
def extract_shop_name(file_path):
    base_name = os.path.basename(file_path)
    shop_name = base_name.split('_')[0]
    return shop_name.strip()

# Read and process the Excel file
# Read and process the Excel file
def read_and_process_excel(file_stream):
    # Read the Excel file from the file stream
    df = pd.read_excel(file_stream, usecols=['Order Number', 'SKU', 'Book Title', 'Airwaybill', 'Customer Phone'])
    
    # Process the DataFrame
    df["Customer Phone"] = df["Customer Phone"].astype(str)
    df['Customer Phone'] = df['Customer Phone'].where(df['Customer Phone'].str.startswith('60'), '60' + df['Customer Phone'])
    df.rename(columns={
        'Order Number': '*Online order id', 
        'SKU': '*Commodity code', 
        'Book Title': 'Item Title', 
        'Airwaybill': 'Remarks', 
        'Customer Phone': "Recipient's mobile phone"
    }, inplace=True)
    
    # Define courier assignment based on Airwaybill
    def assign_courier(airwaybill):
        if pd.isna(airwaybill):  # Handle NaN values
            return 'UNKNOWN'
        airwaybill = str(airwaybill)
        if airwaybill.startswith('7'):
            return 'DHL'
        elif airwaybill.startswith('ERD'):
            return 'POSLAJU'
        elif airwaybill.startswith('D3LYV1'):
            return 'NINJA_VAN'
        elif airwaybill.startswith('MY'):
            return 'SHIPDOC'
        else:
            return 'UNKNOWN'
    
    # Apply courier assignment to each row based on the 'Remarks' (Airwaybill) column
    df['Courier'] = df['Remarks'].apply(assign_courier)
    
    # Define the columns that are required in the final DataFrame
    required_columns = {
        '*Online order id': None,
        '*Shop': None,
        'Payment time': None,
        'Buyer ID': 'ABU',
        "*Recipient's name": 'ABU',
        "Recipient's mobile phone": None,
        'Recipient Email': '',
        '*Recipient Address 1': 'NO 12 JALAN INDUSTRI KIDAMAI 2/1',
        'Recipient Address 2': 'TAMAN INDUSTRI KIDAMAI 2',
        '*Recipient District/Country': 'KAJANG',
        'Recipient City': 'KAJANG',
        '*Recipient Province/State': 'SELANGOR',
        "*Recipient's country": 'Malaysia',
        '*Recipient Zip Code': '43000',
        '*Freight': '',
        '*Commodity code': None,
        'Item Title': None,
        'Commodity Color': '',
        'Commodity Size': '',
        '*Quantity of goods': 1,
        '*Commodity price': 0,
        'Commodity tax': '',
        'Payment Method': 'Online payment',
        'Payment Collection Amount': '',
        'Currency': 'MYR',
        'Message': '',
        'Remarks': None,
        'Courier': None  # The courier column added here
    }
    
    # Ensure all required columns exist in the DataFrame
    for col, default in required_columns.items():
        if col not in df.columns:
            df[col] = default
    
    # Reindex the DataFrame to match the expected column order
    column_order = list(required_columns.keys())
    df = df.reindex(columns=column_order)
    
    return df

# Save the processed DataFrame to Excel
def save_to_excel(df, filename, output_directory):
    timestamp = datetime.now().strftime("%Y%m%d")
    folder_path = os.path.join(output_directory, timestamp)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{filename}.xlsx")
    df.to_excel(file_path, index=False)
    return file_path

# Extract the timestamp from the filename
def extract_timestamp_from_filename(filename):
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return match.group(1).replace('-', '')
    return datetime.now().strftime("%Y%m%d")

# Process the Excel file for BC to ERP
def process_bc_to_erp(file):
    shop_name = extract_shop_name(file.filename)
    date_part = extract_timestamp_from_filename(file.filename)
    current_time = datetime.now().strftime("%H%M%S")
    
    try:
        df = read_and_process_excel(file.stream)
    except Exception as e:
        return f"Error processing the Excel file: {str(e)}"

    if df is None or df.empty:
        return "The uploaded file could not be processed or is empty."

    if 'IMAN' in shop_name.upper():
        df['*Shop'] = 'IMAN OFFLINE'
    elif 'FIXI' in shop_name.upper():
        df['*Shop'] = 'FIXI'
    elif 'ELTY' in shop_name.upper() or 'PTS BOOKCAFE' in shop_name.upper():
        df['*Shop'] = 'BOOKCAFE'
    elif 'BOOKCAFE' in shop_name.upper() or 'PTS BOOKCAFE' in shop_name.upper():
        df['*Shop'] = 'BOOKCAFE'
    else:
        df['*Shop'] = shop_name.upper()

    output_filename = f"{shop_name}_ERP_{date_part}_{current_time}"
    
    return save_to_excel(df, output_filename, ERP_OUTPUT_FOLDER)
    
    # # Reindex only if all columns are available in the DataFrame
    # column_order = ['*Online order id', '*Shop', 'Payment time', 'Buyer ID', "*Recipient's name",
    #                 "Recipient's mobile phone", 'Recipient Email', '*Recipient Address 1','*Recipient Address 2',
    #                 '*Recipient District/Country','*Recipient City', '*Recipient Province/State', "*Recipient's country",
    #                 '*Recipient Zip Code','*Freight', '*Commodity code', 'Item Title', '*Commodity Color','*Commodity Size','*Quantity of goods',
    #                 '*Commodity price', '*Commodity tax','Payment Method','*Payment Collection Amount', 'Currency', 'Message', 'Remarks','*courier',]
    
    # # Ensure all columns are present before reindexing
    # available_columns = [col for col in column_order if col in df.columns]
    # df = df.reindex(columns=available_columns)
    
    # return save_to_excel(df, output_filename, ERP_OUTPUT_FOLDER)

def get_date_from_filename(filename):
    # Use regex to extract date in the format YYYY-MM-DD from the filename
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return match.group(1)
    else:
        # If no date found, use the current date
        return datetime.now().strftime("%Y-%m-%d")

def process_wms_to_oms(file):
    # Read the uploaded file's content into a DataFrame
    df = pd.read_excel(file, usecols=['ERP Order#', 'Note', 'Owner'])
    df['Owner Code'] = df['Owner'].apply(lambda owner: 'IMAN-01' if 'IMAN' in owner.upper() 
                                          else 'FIXI-01' if 'FIXI' in owner.upper() 
                                          else 'BOOKCAFE-01' if 'PTS BOOKCAFE' in owner.upper() 
                                          else 'Unknown')
    df.rename(columns={'ERP Order#': 'ExtOrder', 'Note': 'Tracking'}, inplace=True)
    df['Carrier Service'] = df['Tracking'].apply(decode_courier)
    df['Carrier Code'] = df['Carrier Service']
    
    template_headers = ['Owner Code', 'Carrier Code', 'Carrier Service', 'ExtOrder', 'Tracking']
    df_reindexed = df.reindex(columns=template_headers)
    
    owner_code = df['Owner Code'].iloc[0] if not df.empty else 'Unknown'
    
    # Extract the date from the input filename
    input_date = get_date_from_filename(file.filename)
    
    # Format the base filename with the extracted date and current time
    current_time = datetime.now().strftime("%H%M%S")
    base_filename = f'{owner_code}_WMS_{input_date}_{current_time}'
    
    # Split the DataFrame into chunks of 100 rows and save each as a new file
    output_files = []
    for i, chunk in enumerate(range(0, len(df_reindexed), 99)):
        part_df = df_reindexed.iloc[chunk:chunk + 99]
        part_number = i + 1  # Start part numbering from 1
        output_filename = f"{base_filename}_{part_number}"
        
        # Save the part file
        file_path = save_to_excel(part_df, output_filename, OMS_OUTPUT_FOLDER)
        output_files.append(file_path)
    
    return output_files  # Returns a list of file paths for all parts

def decode_courier(airwaybill):
    if airwaybill.startswith('7'):
        return 'DHL'
    elif airwaybill.startswith('ERD'):
        return 'POSLAJU'
    elif airwaybill.startswith('D3LYV1'):
        return 'NINJA_VAN'
    elif airwaybill.startswith('MY'):
        return 'SHIPDOC'
    else:
        return 'UNKNOWN'

# Helper function to extract publisher name from page text
def extract_publisher_name(page_text):
    if "IMAN PUBLICATION" in page_text:
        return "IMAN"
    elif "PTS BOOKCAFE" in page_text:
        return "PTSBOOKCAFE"
    elif "Buku Fixi" in page_text:
        return "BukuFixi"
    else:
        return None
    
    # Function to extract the date from the filename

def extract_date_from_filename(filename):
    # This regex will capture patterns like "29 SEPT" or "22 SEPT"
    match = re.search(r'(\d{1,2} [A-Z]+)', filename.upper())
    if match:
        # Convert the extracted date to the desired format (e.g., "29 SEPT" -> "29_Sept")
        date_str = match.group(1).title()
        return date_str.replace(" ", "_")  # Replace space with an underscore
    return None  # Return None if no date found

# Helper function to split a PDF into 20-page batches and save them
def split_pdf_by_20_pages(reader, output_folder, service_name, page_numbers, batch_num, publisher_name, extracted_date):
    writer = PyPDF2.PdfWriter()

    for page_num in page_numbers:
        writer.add_page(reader.pages[page_num])

    # Create the file name based on the publisher name or fallback to service name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Include seconds in timestamp
    if publisher_name:
        output_filename = os.path.join(output_folder, f"AWB_{publisher_name}_{extracted_date}_{timestamp}_{batch_num}.pdf")
    else:
        output_filename = os.path.join(output_folder, f"AWB_{service_name}_{extracted_date}_{timestamp}_{batch_num}.pdf")

    # Save the split PDF file
    with open(output_filename, 'wb') as output_file:
        writer.write(output_file)

# Process the uploaded PDF and classify it based on its content using pdfplumber for text extraction
def process_pdf(file_path):
    base_name = os.path.basename(file_path).rsplit('.', 1)[0]
    pdf_reader = PyPDF2.PdfReader(file_path)
    total_pages = len(pdf_reader.pages)

    # Extract date from the filename (e.g., "29 SEPT" or "22 SEPT")
    extracted_date = extract_date_from_filename(base_name)

    # Fallback to today's date if no date found in the filename
    if not extracted_date:
        extracted_date = datetime.now().strftime("%Y%m%d")  # YYYYMMDD format

    publisher_name = None  # Store detected publisher name for dynamic naming

    with pdfplumber.open(file_path) as pdf:  # Use pdfplumber for text extraction
        # Handle Pos Laju separately if it is detected in the file name
        if "pos laju" in base_name.lower().replace("  ", " "):
            output_folder = os.path.join(POSLAJU_FOLDER, extracted_date)
            os.makedirs(output_folder, exist_ok=True)
            batch_num = 1

            # Process Pos Laju file by splitting into 20-page batches
            for start_page in range(0, total_pages, 20):
                end_page = min(start_page + 20, total_pages)
                pages_range = range(start_page, end_page)

                # Use the detected publisher name (if found) when saving files
                split_pdf_by_20_pages(pdf_reader, output_folder, 'PosLaju', pages_range, batch_num, publisher_name, extracted_date)
                batch_num += 1

            logging.debug(f"Pos Laju file processed and saved to {output_folder}")
        
        else:
            # Handle other couriers (Ninja Van, DHL, Others)
            dhl_output_folder = os.path.join(DHL_FOLDER, extracted_date)
            ninja_output_folder = os.path.join(NINJA_FOLDER, extracted_date)
            gdex_output_folder = os.path.join(GDEX_FOLDER, extracted_date)
            others_output_folder = os.path.join(OTHERS_FOLDER, extracted_date)
            
            os.makedirs(dhl_output_folder, exist_ok=True)
            os.makedirs(ninja_output_folder, exist_ok=True)
            os.makedirs(others_output_folder, exist_ok=True)
            os.makedirs(gdex_output_folder, exist_ok=True)

            dhl_pages = []
            ninja_pages = []
            others_pages = []
            gdex_pages = []

            for page_num in range(len(pdf.pages)):
                page = pdf.pages[page_num]
                text = page.extract_text()

                logging.debug(f"Raw extracted text from Page {page_num + 1}:")

                if text:
                    text_lower = text.lower()

                    # Detect publisher name for dynamic file naming
                    if publisher_name is None:
                        publisher_name = extract_publisher_name(text)

                    # DHL detection
                    if 'dhl' in text_lower or re.search(r'\bDHL ID\b', text):
                        logging.debug(f"DHL detected on Page {page_num + 1}")
                        dhl_pages.append(page_num)
                    # Ninja Van detection
                    elif 'ninja' in text_lower or 'd3lyv1' in text_lower:
                        logging.debug(f"Ninja Van detected on Page {page_num + 1}")
                        ninja_pages.append(page_num)
                    # If text is unreadable or doesn't match, classify as Others
                    elif 'gdex' in text_lower:
                        logging.debug(f"GDEX detected on Page {page_num + 1}")
                        gdex_pages.append(page_num)
                    else:
                        logging.debug(f"Classified as Others: Page {page_num + 1}")
                        others_pages.append(page_num)
                else:
                    # If no text extracted, also classify as Others
                    logging.debug(f"No text detected on Page {page_num + 1}, classified as Others")
                    others_pages.append(page_num)

            # Save files in respective folders for each courier
            if dhl_pages:
                logging.debug(f"Saving {len(dhl_pages)} DHL pages")
                batch_num = 1
                for start_page in range(0, len(dhl_pages), 20):
                    end_page = min(start_page + 20, len(dhl_pages))
                    split_pdf_by_20_pages(pdf_reader, dhl_output_folder, 'DHL', dhl_pages[start_page:end_page], batch_num, publisher_name, extracted_date)
                    batch_num += 1

            if ninja_pages:
                logging.debug(f"Saving {len(ninja_pages)} Ninja Van pages")
                batch_num = 1
                for start_page in range(0, len(ninja_pages), 20):
                    end_page = min(start_page + 20, len(ninja_pages))
                    split_pdf_by_20_pages(pdf_reader, ninja_output_folder, 'Ninja', ninja_pages[start_page:end_page], batch_num, publisher_name, extracted_date)
                    batch_num += 1
                    

            if others_pages:
                logging.debug(f"Saving {len(others_pages)} Other pages")
                batch_num = 1
                for start_page in range(0, len(others_pages), 20):
                    end_page = min(start_page + 20, len(others_pages))
                    split_pdf_by_20_pages(pdf_reader, others_output_folder, 'Others', others_pages[start_page:end_page], batch_num, publisher_name, extracted_date)
                    batch_num += 1

            if gdex_pages:
                logging.debug(f"Saving {len(gdex_pages)} Gdex pages")
                batch_num = 1
                for start_page in range(0, len(gdex_pages), 20):
                    end_page = min(start_page + 20, len(gdex_pages))
                    split_pdf_by_20_pages(pdf_reader, gdex_output_folder, 'Gdex', gdex_pages[start_page:end_page], batch_num, publisher_name, extracted_date)
                    batch_num += 1

# Route for Split PDF upload
@app.route('/upload_split_pdf', methods=['POST'])
def handle_split_pdf():
    if 'file' not in request.files:
        return "No file part"
    file = request.files['file']
    if file.filename == '':
        return "No selected file"
    if file:
        try:
            # Save the uploaded PDF temporarily
            file_path = os.path.join(AWB_OUTPUT_FOLDER, file.filename)
            file.save(file_path)

            # Process the PDF (classify and split)
            process_pdf(file_path)

            # Optionally remove the input file after processing
            os.remove(file_path)

            return f"PDF has been split and saved to respective folders."
        except PyPDF2.errors.PdfReadError:
            # Handle PDF read error
            return "The uploaded PDF is corrupted or cannot be processed.", 400
        except Exception as e:
            # Handle other errors
            return f"An error occurred: {str(e)}", 500


# Routes for file uploads
@app.route('/upload', methods=['POST'])
def handle_bc_to_erp():
    if 'file' not in request.files:
        return "No file part"
    file = request.files['file']
    if file.filename == '':
        return "No selected file"
    if file and allowed_file(file.filename):
        # Process the file in-memory without saving it to disk
        processed_file = process_bc_to_erp(file)  # Pass the file object directly
        return send_file(processed_file, as_attachment=True)



@app.route('/upload_wms', methods=['POST'])
def handle_wms_to_oms():
    if 'file' not in request.files:
        return "No file part"
    file = request.files['file']
    if file.filename == '':
        return "No selected file"
    if file and allowed_file(file.filename):
        try:
            # Attempt to read the file to validate its format
            df = pd.read_excel(file, nrows=1)  # Read only the first row to check columns
            required_columns = {'ERP Order#', 'Note', 'Owner'}
            if not required_columns.issubset(df.columns):
                return "The uploaded file is not in the expected format. Please check the columns.", 400
            
            # Process the file if the validation is successful
            processed_files = process_wms_to_oms(file)
            # Send multiple files as attachments (or you may package them as a ZIP file if needed)
            # For now, we'll return just a success message and list of files.
            return f"Files processed successfully: {', '.join(processed_files)}", 200
        except Exception as e:
            # Handle processing error
            return f"An error occurred while processing the file: {str(e)}", 500



# Homepage
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5005, debug=True)
