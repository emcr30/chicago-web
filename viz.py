""""Muestra un gráfico de barras con el conteo de delitos por tipo principal.

@param df DataFrame que contiene los registros de crímenes, incluyendo la columna 'primary_type'.

@returns None. Muestra el gráfico directamente en Streamlit.
"""
import streamlit as st
import pandas as pd
from typing import Any
try:
    import pydeck as pdk
    PDK_AVAILABLE: bool = True
except Exception:
    PDK_AVAILABLE: bool = False


def show_primary_type_bar(df: pd.DataFrame) -> None:
    st.subheader('Conteo por Primary Type')
    counts = df['primary_type'].fillna('UNKNOWN').value_counts().rename_axis('primary_type').reset_index(name='counts')
    st.bar_chart(counts.set_index('primary_type'))


""""Muestra un mapa con puntos y zonas de calor de los delitos registrados.

@param df DataFrame con coordenadas de delitos (columnas 'latitude' y 'longitude').
@param heat_threshold Umbral mínimo de incidentes para considerar una zona como punto caliente.

@returns None. Muestra el mapa directamente en Streamlit.
"""
def show_map_points_and_heat(df: pd.DataFrame, heat_threshold: int = 50) -> None:
    st.subheader('Mapa de puntos y calor')
    mdf = df.dropna(subset=['latitude', 'longitude']).copy()
    if mdf.empty:
        st.info('No hay coordenadas válidas para mostrar')
        return

    #  map
    st.map(mdf.rename(columns={'latitude': 'lat', 'longitude': 'lon'})[['lat', 'lon']])

    # agregación por hex para detectar hotspots
    if PDK_AVAILABLE:
        try:
            # usa pydeck HexagonLayer
            view_state = pdk.ViewState(latitude=mdf['latitude'].mean(), longitude=mdf['longitude'].mean(), zoom=10, pitch=40)
            hex_layer = pdk.Layer(
                "HexagonLayer",
                data=mdf[['latitude', 'longitude']].rename(columns={'latitude': 'lat', 'longitude': 'lon'}),
                get_position='[lon, lat]',
                radius=200,
                elevation_scale=50,
                elevation_range=[0, 3000],
                pickable=True,
                extruded=True,
            )

            r = pdk.Deck(layers=[hex_layer], initial_view_state=view_state, tooltip={"text": "# of incidents: {position} (hover for details)"})
            st.pydeck_chart(r)
        except Exception as e:
            st.write('No se pudo generar mapa avanzado con pydeck:', e)
    else:
        st.info('pydeck no está disponible: mostrando mapa básico')
        st.map(mdf.rename(columns={'latitude': 'lat', 'longitude': 'lon'})[['lat', 'lon']])

    try:
        bins = mdf.copy()
        bins['lat_bin'] = bins['latitude'].round(2)
        bins['lon_bin'] = bins['longitude'].round(2)
        grouped = bins.groupby(['lat_bin', 'lon_bin']).size().reset_index(name='count')
        hotspots = grouped[grouped['count'] > heat_threshold]
        if not hotspots.empty:
            st.warning(f'Se detectaron {len(hotspots)} zonas con más de {heat_threshold} delitos (coarse bins).')
            st.dataframe(hotspots)
    except Exception as e:
        st.write('No se pudo calcular hotspots:', e)


""""Muestra un gráfico de barras con las 10 ubicaciones más frecuentes donde ocurrieron delitos.

@param df DataFrame que contiene la columna 'location_description' con las ubicaciones de los crímenes.

@returns None. Muestra el gráfico directamente en Streamlit.
"""
def show_additional_charts(df: pd.DataFrame) -> None:
    st.subheader('Top 10 ubicaciones')
    try:
        top = df['location_description'].fillna('UNKNOWN').value_counts().head(10)
        st.bar_chart(top)
    except Exception:
        st.write('No se pudo generar top ubicaciones')
