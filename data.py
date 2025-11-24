import requests
import pandas as pd
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import streamlit as st

SCODA_URL: str = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
DEFAULT_FROM_DATE: str = "2024-01-01T00:00:00"

SCHEMA_COLUMNS: List[str] = [
    'id', 'case_number', 'date', 'block', 'iucr', 'primary_type',
    'description', 'location_description', 'arrest', 'domestic', 'beat',
    'district', 'ward', 'community_area', 'fbi_code', 'year', 'updated_on',
    'x_coordinate', 'y_coordinate', 'latitude', 'longitude', 'location'
]

# Tipos de crimen para Arequipa 
CRIME_TYPES_AREQUIPA: Dict[str, List[str]] = {
    'ROBO': ['Robo con violencia', 'Robo de vehículo', 'Robo a transeúnte'],
    'ASALTO': ['Asalto y lesiones', 'Agresión física', 'Riña callejera'],
    'HURTO': ['Hurto simple', 'Hurto de celular', 'Carterismo'],
    'VANDALISMO': ['Daño a propiedad', 'Grafiti', 'Destrucción de bienes'],
    'VIOLENCIA FAMILIAR': ['Violencia doméstica', 'Maltrato familiar'],
    'ESTAFA': ['Fraude', 'Estafa telefónica', 'Clonación de tarjetas'],
}

LOCATIONS_AREQUIPA: List[str] = [
    'CALLE', 'AVENIDA', 'PARQUE', 'PLAZA', 'TIENDA', 'RESTAURANTE',
    'RESIDENCIA', 'BANCO', 'MERCADO', 'TRANSPORTE PÚBLICO', 'ESTACIONAMIENTO'
]


def _records_to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=SCHEMA_COLUMNS)
    df = pd.DataFrame(records)
    for c in SCHEMA_COLUMNS:
        if c not in df.columns:
            df[c] = None
    for col in ['date', 'updated_on']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    for coord in ['latitude', 'longitude']:
        if coord in df.columns:
            df[coord] = pd.to_numeric(df[coord], errors='coerce')
    if 'location' in df.columns:
        df['location'] = df['location'].apply(lambda x: str(x) if not pd.isna(x) else None)
    return df[SCHEMA_COLUMNS]


def fetch_latest(limit: int = 5000, force: bool = False, refresh_interval: int = 60) -> pd.DataFrame:
    key_time = '_chicago_last_fetch_time'
    key_df = '_chicago_last_df'
    key_chicago = '_chicago_base_df'
    now = time.time()
    last = st.session_state.get(key_time, 0)
    
    # Verificar si necesitamos refrescar desde la API
    need_refresh = force or (now - last >= refresh_interval) or (key_chicago not in st.session_state)
    
    if need_refresh:
        params = {'$limit': limit, '$order': 'date DESC'}
        try:
            resp = requests.get(SCODA_URL, params=params, timeout=30)
            resp.raise_for_status()
            records = resp.json()
            chicago_df = _records_to_dataframe(records)
            st.session_state[key_chicago] = chicago_df
            st.session_state[key_time] = now
        except Exception as e:
            st.error(f'Error fetching data from API: {e}')
            # Si falla, usar datos anteriores si existen
            chicago_df = st.session_state.get(key_chicago, pd.DataFrame(columns=SCHEMA_COLUMNS))
    else:
        chicago_df = st.session_state.get(key_chicago, pd.DataFrame(columns=SCHEMA_COLUMNS))
    
    # Combinar con datos sintéticos de Arequipa si existen
    arequipa_df = st.session_state.get('_arequipa_records', pd.DataFrame(columns=SCHEMA_COLUMNS))
    
    if not arequipa_df.empty:
        combined_df = pd.concat([arequipa_df, chicago_df], ignore_index=True)
    else:
        combined_df = chicago_df

    # Convertir columna 'year' a número para evitar error Arrow
    if 'year' in combined_df.columns:
        combined_df['year'] = pd.to_numeric(combined_df['year'], errors='coerce').astype('Int64')

    # Ordenar por fecha descendente
    if 'date' in combined_df.columns:
        combined_df = combined_df.sort_values('date', ascending=False)


    
    st.session_state[key_df] = combined_df
    return combined_df


def _point_in_polygon(point: Tuple[float, float], polygon: List[Tuple[float, float]]) -> bool:
    """Verifica si un punto está dentro de un polígono usando ray casting."""
    lat, lon = point
    n = len(polygon)
    inside = False
    
    p1_lat, p1_lon = polygon[0]
    for i in range(1, n + 1):
        p2_lat, p2_lon = polygon[i % n]
        if lon > min(p1_lon, p2_lon):
            if lon <= max(p1_lon, p2_lon):
                if lat <= max(p1_lat, p2_lat):
                    if p1_lon != p2_lon:
                        xinters = (lon - p1_lon) * (p2_lat - p1_lat) / (p2_lon - p1_lon) + p1_lat
                    if p1_lat == p2_lat or lat <= xinters:
                        inside = not inside
        p1_lat, p1_lon = p2_lat, p2_lon
    
    return inside


def _generate_point_in_bounds(bounds: List[Tuple[float, float]]) -> Tuple[float, float]:
    lats = [b[0] for b in bounds]
    lons = [b[1] for b in bounds]
    
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)
    
    for _ in range(100):
        lat = random.uniform(min_lat, max_lat)
        lon = random.uniform(min_lon, max_lon)
        
        if _point_in_polygon((lat, lon), bounds):
            return (lat, lon)
    
    return (sum(lats) / len(lats), sum(lons) / len(lons))


def generate_random_records_in_zone(
    n: int,
    zone_bounds: List[Tuple[float, float]],
    crime_types: Optional[List[str]] = None,
    days_back: int = 30,
    store_in_session: bool = True
) -> pd.DataFrame:

    rows = []
    now = datetime.utcnow()
    
    if crime_types is None:
        crime_types = list(CRIME_TYPES_AREQUIPA.keys())
    
    for i in range(n):
        # Generar coordenadas dentro de la zona
        lat, lon = _generate_point_in_bounds(zone_bounds)
        
        # Seleccionar tipo de crimen
        primary = random.choice(crime_types)
        description = random.choice(CRIME_TYPES_AREQUIPA.get(primary, ['Incidente']))
        
        # Generar fecha aleatoria en los últimos días
        days_ago = random.randint(0, days_back)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        record_date = now - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        row = {
            'id': f'ARQ-{int(time.time()*1000)}-{i}',
            'case_number': f'AQP{record_date.year}{i:06d}',
            'date': record_date.isoformat(),
            'block': f'{random.choice(["AV", "CALLE", "JR"])} {random.randint(100, 999)}',
            'iucr': f'{random.randint(1000, 9999)}',
            'primary_type': primary,
            'description': description,
            'location_description': random.choice(LOCATIONS_AREQUIPA),
            'arrest': random.random() < 0.15,
            'domestic': random.random() < 0.25 if primary == 'VIOLENCIA FAMILIAR' else random.random() < 0.05,
            'beat': f'{random.randint(100, 999)}',
            'district': f'{random.randint(1, 10):02d}',
            'ward': f'{random.randint(1, 29)}',
            'community_area': f'{random.randint(1, 77)}',
            'fbi_code': None,
            'year': record_date.year,
            'updated_on': now.isoformat(),
            'x_coordinate': None,
            'y_coordinate': None,
            'latitude': lat,
            'longitude': lon,
            'location': f'({lat}, {lon})'
        }
        rows.append(row)
    
    df = _records_to_dataframe(rows)
    if store_in_session:
        # Almacenar los registros en la sesión como datos de Arequipa
        add_records_to_session(df, is_arequipa=True)
    return df


def generate_random_records(n: int, base_lat: Optional[float] = None, base_lon: Optional[float] = None) -> pd.DataFrame:
    ##Versión original - genera registros aleatorios simples
    rows = []
    now = datetime.utcnow()
    for i in range(n):
        lat = None
        lon = None
        if base_lat is not None and base_lon is not None:
            lat = base_lat + random.uniform(-0.01, 0.01)
            lon = base_lon + random.uniform(-0.01, 0.01)
        row = {
            'id': f'fake-{int(time.time()*1000)}-{i}',
            'case_number': f'FAKE{i:06d}',
            'date': now.isoformat(),
            'block': 'UNKNOWN',
            'iucr': '0000',
            'primary_type': random.choice(['THEFT', 'BATTERY', 'ROBBERY', 'CRIMINAL DAMAGE', 'ASSAULT']),
            'description': 'Synthetic record for testing',
            'location_description': 'RESIDENCE',
            'arrest': random.choice([True, False]),
            'domestic': random.choice([True, False]),
            'beat': None,
            'district': None,
            'ward': None,
            'community_area': None,
            'fbi_code': None,
            'year': now.year,
            'updated_on': now.isoformat(),
            'x_coordinate': None,
            'y_coordinate': None,
            'latitude': lat,
            'longitude': lon,
            'location': None
        }
        rows.append(row)
    return _records_to_dataframe(rows)


def persist_dataframe_to_sqlite(df: pd.DataFrame, db_path: str = 'chicago.db', table: str = 'crimes') -> None:
    import sqlite3
    
    # Convertir el diccionario a una cadena
    if 'location' in df.columns and df['location'].dtype == 'object':
        df['location'] = df['location'].apply(lambda x: str(x) if isinstance(x, dict) else x)
    
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table, conn, if_exists='append', index=False)
    finally:
        conn.close()


def add_records_to_session(df: pd.DataFrame, is_arequipa: bool = False) -> None:
    if is_arequipa:
        key = '_arequipa_records'
        existing = st.session_state.get(key, pd.DataFrame(columns=SCHEMA_COLUMNS))
        st.session_state[key] = pd.concat([df, existing], ignore_index=True)
    else:
        key_df = '_chicago_last_df'
        existing = st.session_state.get(key_df, pd.DataFrame(columns=SCHEMA_COLUMNS))
        st.session_state[key_df] = pd.concat([df, existing], ignore_index=True)


def get_arequipa_records() -> pd.DataFrame:
    """Obtiene los registros sintéticos de Arequipa almacenados en la sesión."""
    return st.session_state.get('_arequipa_records', pd.DataFrame(columns=SCHEMA_COLUMNS))


def clear_arequipa_records() -> None:
    """Limpia los registros sintéticos de Arequipa de la sesión."""
    if '_arequipa_records' in st.session_state:
        del st.session_state['_arequipa_records']


def get_arequipa_records() -> pd.DataFrame:
    """Obtiene los registros sintéticos de Arequipa almacenados en la sesión."""
    return st.session_state.get('_arequipa_records', pd.DataFrame(columns=SCHEMA_COLUMNS))


def clear_arequipa_records() -> None:
    """Limpia los registros sintéticos de Arequipa de la sesión."""
    if '_arequipa_records' in st.session_state:
        del st.session_state['_arequipa_records']


