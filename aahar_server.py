import json
import time
import threading
from datetime import datetime
from flask import Flask, Response, jsonify, send_from_directory
import os
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd

app = Flask(__name__)
CORS(app)

if not firebase_admin._apps:
    cred = credentials.Certificate(r"C:\Users\JWPL\Desktop\Aahar-Live\aahar-database.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

latest_data = {}
data_lock = threading.Lock()
sse_clients = []

def flatten_doc(doc):
    row = doc.to_dict()
    row["doc_id"] = doc.id
    for key, value in list(row.items()):
        if hasattr(value, 'seconds'):
            row[key] = datetime.utcfromtimestamp(value.seconds).strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, datetime) and value.tzinfo is not None:
            row[key] = value.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
    return row

def parse_list_field(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(i).strip() for i in value if i]
    try:
        import numpy as np
        if isinstance(value, np.ndarray):
            return [str(i).strip() for i in value if i]
    except:
        pass
    try:
        if float(value) != float(value):
            return []
    except:
        pass
    try:
        cleaned = str(value).strip()
        if cleaned in ('', '[]', 'nan', 'None'):
            return []
        if cleaned.startswith('['):
            import ast
            return ast.literal_eval(cleaned)
        return [cleaned]
    except:
        return [str(value)]

def process_data(docs):
    if not docs:
        return {}

    df = pd.json_normalize([flatten_doc(d) for d in docs])
    total_leads = len(df)

    # Leads per rep
    leads_per_rep = {}
    if 'entered_by_email' in df.columns:
        leads_per_rep = df['entered_by_email'].fillna('Unknown').value_counts().to_dict()

    tablet_data = leads_per_rep

    # Area of interest
    interest_counts = {}
    ALLOWED_CATS = {'oil', 'beverage', 'food', 'others'}
    category_counts = {}
    item_counts = {}
    if 'area_of_interest' in df.columns:
        for val in df['area_of_interest']:
            for interest in parse_list_field(val):
                interest = interest.strip()
                if not interest:
                    continue
                interest_counts[interest] = interest_counts.get(interest, 0) + 1
                if ' - ' in interest:
                    parts = interest.split(' - ', 1)
                    cat = parts[0].strip()
                    item = parts[1].strip()
                    if cat.lower() in ALLOWED_CATS and item:
                        category_counts[cat] = category_counts.get(cat, 0) + 1
                        item_counts[item] = item_counts.get(item, 0) + 1
                else:
                    if interest.lower() in ALLOWED_CATS:
                        category_counts[interest] = category_counts.get(interest, 0) + 1
    interest_counts = dict(sorted(interest_counts.items(), key=lambda x: x[1], reverse=True))
    category_counts = dict(sorted(category_counts.items(), key=lambda x: x[1], reverse=True))
    item_counts = dict(sorted(item_counts.items(), key=lambda x: x[1], reverse=True))

    # Leads per day — Day 1, Day 2, Day 3 ...
    leads_per_day = {}
    day_label_map = {}   # ← FIX: always defined before use
    date_col = 'date' if 'date' in df.columns else 'created_at'
    if date_col in df.columns:
        df['_date_parsed'] = pd.to_datetime(df[date_col], errors='coerce')
        df['_day_date'] = df['_date_parsed'].dt.date
        unique_days = sorted(df['_day_date'].dropna().unique())
        day_label_map = {d: f'Day {i+1}' for i, d in enumerate(unique_days)}
        df['_day_label'] = df['_day_date'].map(day_label_map)
        day_counts = df['_day_label'].dropna().value_counts()
        leads_per_day = dict(sorted(day_counts.items(), key=lambda x: int(x[0].split()[1])))

    # Follow-up vs New
    followup_counts = {'New Lead': 0, 'Follow-up': 0}
    if 'is_follow_up_contact' in df.columns:
        for val in df['is_follow_up_contact']:
            if val is True or str(val).lower() == 'true':
                followup_counts['Follow-up'] += 1
            else:
                followup_counts['New Lead'] += 1

    # Recent leads
    recent_leads = []
    all_reps = []   # ← FIX: always defined before use

    def bool_flag(val):
        return val is True or str(val).strip().lower() == 'true'

    for _, row in df.sort_values(date_col, ascending=False).iterrows():
        image_urls = parse_list_field(row.get('business_card_image_urls', []))

        # Derive day label
        day_label = '—'
        try:
            d = pd.to_datetime(row.get(date_col)).date()
            day_label = day_label_map.get(d, '—')
        except:
            pass

        # Derive categories from area_of_interest
        interests_raw = parse_list_field(row.get('area_of_interest', []))
        cats = list({
            i.split(' - ')[0].strip()
            for i in interests_raw
            if i.split(' - ')[0].strip().lower() in ALLOWED_CATS
        } | {
            i.strip()
            for i in interests_raw
            if i.strip().lower() in ALLOWED_CATS
        })

        # Derive intent types from area_of_interest entries
        intent_set = set()
        SALES_CATS = {'oil', 'food', 'beverage'}
        for entry in interests_raw:
            prefix = entry.split(' - ')[0].strip().lower()
            if prefix in SALES_CATS:
                intent_set.add('Sales')
            elif prefix == 'purchase':
                intent_set.add('Purchase')
            elif prefix == 'service' or prefix == 'services':
                intent_set.add('Services')
            elif prefix:
                intent_set.add('Others')

        recent_leads.append({
            'name':             str(row.get('name', 'N/A')),
            'organization':     str(row.get('organization', 'N/A')),
            'designation':      str(row.get('designation', 'N/A')),
            'entered_by':       str(row.get('entered_by_email', 'N/A')),
            'meeting_with':     str(row.get('meeting_with', '—')) if pd.notna(row.get('meeting_with')) else '—',
            'meeting_team':     str(row.get('meeting_with_team', '—')) if pd.notna(row.get('meeting_with_team')) else '—',
            'time':             str(row.get(date_col, 'N/A')),
            'is_follow_up':     bool_flag(row.get('is_follow_up_contact', False)),
            'is_important':     bool_flag(row.get('is_import_contact', False)),
            'image_urls':       image_urls,
            'day':              day_label,
            'categories':       cats,
            'area_of_interest': ', '.join(interests_raw) if interests_raw else '',
            'intent_types':     sorted(intent_set),
            # ── New fields ──
            'phone':            str(row.get('phone', '')) if pd.notna(row.get('phone', '')) else '',
            'email':            str(row.get('email', '')) if pd.notna(row.get('email', '')) else '',
            'company_strength': str(row.get('company_strength', '')) if pd.notna(row.get('company_strength', '')) else '',
            'company_turnover': str(row.get('company_turnover', '')) if pd.notna(row.get('company_turnover', '')) else '',
            'website':          str(row.get('website', '')) if pd.notna(row.get('website', '')) else '',
            'remarks':          str(row.get('remarks', '')) if pd.notna(row.get('remarks', '')) else '',
            'other_interest':   str(row.get('other_interest', '')) if pd.notna(row.get('other_interest', '')) else '',
            'registration_id':  str(row.get('registration_id', '')) if pd.notna(row.get('registration_id', '')) else '',
            'is_stock':         bool_flag(row.get('is_stock_contact', False)),
        })

    # ← FIX: build all_reps from recent_leads after the loop
    all_reps = sorted(set(
        r.get('entered_by', '').split('@')[0]
        for r in recent_leads
        if r.get('entered_by') and r.get('entered_by') != 'N/A'
    ))

    # Meeting With
    meeting_with_counts = {}
    if 'meeting_with' in df.columns:
        meeting_with_counts = df['meeting_with'].dropna().value_counts().to_dict()

    # Meeting Team
    meeting_team_counts = {}
    if 'meeting_with_team' in df.columns:
        meeting_team_counts = df['meeting_with_team'].dropna().value_counts().to_dict()

    return {
        'total_leads':        total_leads,
        'leads_per_rep':      leads_per_rep,
        'tablet_data':        tablet_data,
        'interest_counts':    interest_counts,
        'category_counts':    category_counts,
        'item_counts':        item_counts,
        'leads_per_day':      leads_per_day,
        'followup_counts':    followup_counts,
        'recent_leads':       recent_leads,
        'all_reps':           all_reps,
        'meeting_with_counts': meeting_with_counts,
        'meeting_team_counts': meeting_team_counts,
        'last_updated':       datetime.now().strftime('%d %b %Y, %H:%M:%S')
    }

all_docs = {}

def on_snapshot(col_snapshot, changes, read_time):
    global latest_data
    for change in changes:
        doc = change.document
        if change.type.name == 'REMOVED':
            all_docs.pop(doc.id, None)
        else:
            all_docs[doc.id] = doc
    with data_lock:
        latest_data = process_data(list(all_docs.values()))
    payload = json.dumps(latest_data)
    for client_queue in list(sse_clients):
        try:
            client_queue.append(payload)
        except:
            pass

col_ref = db.collection("leads")
col_ref.on_snapshot(on_snapshot)

DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))

@app.route('/')
def index():
    return send_from_directory(DASHBOARD_DIR, 'aahar_dashboard.html')

@app.route('/stream')
def stream():
    client_queue = []
    sse_clients.append(client_queue)
    def generate():
        with data_lock:
            initial = json.dumps(latest_data)
        yield f"data: {initial}\n\n"
        try:
            while True:
                if client_queue:
                    yield f"data: {client_queue.pop(0)}\n\n"
                else:
                    yield f": heartbeat\n\n"
                    time.sleep(15)
        except GeneratorExit:
            if client_queue in sse_clients:
                sse_clients.remove(client_queue)
    headers = {
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
    }
    return Response(generate(), mimetype='text/event-stream', headers=headers)

@app.route('/data')
def data():
    with data_lock:
        return jsonify(latest_data)

@app.route('/fields')
def fields():
    with data_lock:
        docs = list(all_docs.values())
    if not docs:
        return jsonify({})
    flat = flatten_doc(docs[0])
    return jsonify({k: str(type(v).__name__) + ' | ' + str(v)[:80] for k,v in flat.items()})

@app.route('/export')
def export():
    with data_lock:
        docs = list(all_docs.values())
    if not docs:
        return jsonify([])
    rows = []
    for doc in docs:
        flat = flatten_doc(doc)
        interests = parse_list_field(flat.get('area_of_interest', []))
        rows.append({
            'name':             str(flat.get('name', '')),
            'email':            str(flat.get('email', '')),
            'phone':            str(flat.get('phone', '')),
            'organization':     str(flat.get('organization', '')),
            'designation':      str(flat.get('designation', '')),
            'company_strength': str(flat.get('company_strength', '')),
            'meeting_with':     str(flat.get('meeting_with', '')),
            'meeting_team':     str(flat.get('meeting_with_team', '')),
            'area_of_interest': ', '.join(interests),
            'remarks':          str(flat.get('remarks', '')),
            'is_follow_up':     str(flat.get('is_follow_up_contact', '')),
            'is_important':     str(flat.get('is_import_contact', '')),
            'captured_by':      str(flat.get('entered_by_email', '')),
            'date':             str(flat.get('date', flat.get('created_at', ''))),
            'image_urls':       ', '.join(parse_list_field(flat.get('business_card_image_urls', []))),
        })
    rows.sort(key=lambda x: x['date'], reverse=True)
    return jsonify(rows)

if __name__ == '__main__':
    print("\n" + "="*50)
    print("🚀  Aahar Dashboard Server started!")
    print("="*50)
    print("👉  Open dashboard at: http://localhost:5000")
    print("📡  Data API at:       http://localhost:5000/data")
    print("👂  Listening for live Firestore updates...")
    print("="*50 + "\n")
    app.run(debug=False, threaded=True, port=5000)
