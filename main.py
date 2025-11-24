""""Panel principal del sistema CRIMENGO.

Permite el control de la interfaz pública y de administrador para visualizar,
generar y administrar registros de crímenes en Arequipa.

@imports Importa módulos para manejo de datos, visualización, autenticación
y conexión a base de datos PostgreSQL.

@returns Inicializa y ejecuta la aplicación principal de Streamlit.
"""
import streamlit as st
import pandas as pd
import requests
from typing import Any, Tuple
try:
    import CHICAGO.data as data_module
    from CHICAGO.viz import show_primary_type_bar, show_map_points_and_heat, show_additional_charts
    from CHICAGO.auth import admin_login_ui, admin_logout
    from CHICAGO.db_postgres import insert_crimes
except Exception:
    import data as data_module
    from viz import show_primary_type_bar, show_map_points_and_heat, show_additional_charts
    from auth import admin_login_ui, admin_logout
    from db_postgres import insert_crimes
import inspect

DEFAULT_LIMIT: int = 5000

# Definición de zonas de Arequipa con sus coordenadas
AREQUIPA_ZONES: dict[str, dict[str, Any]] = {
    "Centro Histórico": {
        "bounds": [
            (-16.424240, -71.556179),
            (-16.424528, -71.556496),
            (-16.423735, -71.557225),
            (-16.423735, -71.557225)
        ],
        "center": (-16.424060, -71.556775)
    },
    "Yanahuara": {
        "bounds": [
            (-16.390, -71.545),
            (-16.395, -71.550),
            (-16.400, -71.545),
            (-16.395, -71.540)
        ],
        "center": (-16.395, -71.545)
    }
}


#Panel de control del administrador.

#Permite generar datos sintéticos, actualizar la base de datos PostgreSQL,
#guardar o limpiar datos locales, y exportar archivos CSV.

#returns Una tupla con el nombre de la zona seleccionada y su información.

def admin_panel() -> Tuple[str, dict[str, Any]]:
    st.sidebar.markdown("---")
    st.sidebar.success("✅ Sesión: Administrador")
    
    if st.sidebar.button("Cerrar Sesión"):
        admin_logout()
        st.experimental_rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Panel de Administración")
    
    # Selección de zona
    zone_name = st.sidebar.selectbox(
        "Zona de Arequipa",
        options=list(AREQUIPA_ZONES.keys())
    )
    
    zone_info = AREQUIPA_ZONES[zone_name]
    
    # Generar datos sintéticos
    st.sidebar.markdown("### 📊 Generar Datos")
    inject_count = st.sidebar.number_input(
        'Registros sintéticos',
        min_value=10,
        max_value=1000,
        value=50,
        step=10
    )
    crime_types = st.sidebar.multiselect(
        "Tipos de crimen",
        options=['ROBO', 'ASALTO', 'HURTO', 'VANDALISMO', 'VIOLENCIA FAMILIAR'],
        default=['ROBO', 'ASALTO', 'HURTO']
    )
    if st.sidebar.button('🎲 Generar Datos en Zona (PostgreSQL)'):
        try:
            # Generar datos sintéticos en la zona seleccionada
            if hasattr(data_module, 'generate_random_records_in_zone'):
                gen_fn = getattr(data_module, 'generate_random_records_in_zone')
                synth = gen_fn(n=int(inject_count), zone_bounds=zone_info["bounds"], crime_types=crime_types if crime_types else None)
            else:
                gen_fn = getattr(data_module, 'generate_random_records')
                synth = gen_fn(int(inject_count))
            
            # Insertar en base de datos (Postgres o SQLite según DB_MODE)
            records = synth.to_dict(orient='records')
            insert_crimes(records)
            st.sidebar.success(f'{len(records)} registros generados e insertados en base de datos')
        except Exception as e:
            st.sidebar.error(f'Error al generar/insertar: {e}')
    
    # Actualizar base con registros reales
    st.sidebar.markdown("### 🔄 Actualizar Base de Datos")
    if st.sidebar.button('Actualizar con últimos 5000 de Chicago (PostgreSQL)'):
        try:
            df_chicago = data_module.fetch_latest(limit=5000)
            records = df_chicago.to_dict(orient='records')
            insert_crimes(records)
            st.sidebar.success(f'Se insertaron/actualizaron {len(records)} registros en PostgreSQL')
        except Exception as e:
            st.sidebar.error(f'Error al actualizar base: {e}')
    
    # Gestión de base de datos local
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Base de Datos")
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if col1.button('Guardar'):
            df = st.session_state.get('_chicago_last_df', pd.DataFrame())       
            if not df.empty:
                try:
                    persist_fn = getattr(data_module, 'persist_dataframe_to_sqlite')
                    persist_fn(df)
                    st.sidebar.success(' Guardado')
                except Exception as e:
                    st.sidebar.error(f'Error al guardar: {e}')
            else:
                st.sidebar.warning('No hay datos')

    with col2:
        if col2.button('Limpiar'):
            import os
            try:
                os.remove('chicago.db')
                st.sidebar.success(' DB eliminada')
            except FileNotFoundError:
                st.sidebar.info('No existe DB')
            except Exception as e:
                st.sidebar.error(f'Error: {e}')
    
    # Exportar datos a CSV
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📤 Exportar Datos")
    
    df = st.session_state.get('_chicago_last_df', pd.DataFrame())
    if not df.empty:
        csv = df.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(
            label="Descargar CSV",
            data=csv,
            file_name=f"crimenes_arequipa_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    
    return zone_name, zone_info


#Vista pública del sistema (sin controles de administrador).

#Permite a los usuarios visualizar los datos generales y acceder
#al botón de inicio de sesión para administradores.

def public_view() -> None:
    st.sidebar.info(" Vista Pública")
    st.sidebar.markdown("Los datos se actualizan automáticamente")
    
    # Botón para acceder como administrador
    if st.sidebar.button("Acceso Administrador"):
        st.rerun()


#Función principal de la aplicación Streamlit.

#Controla la configuración general, autenticación, visualización de métricas,
#gráficos, mapas y tablas de datos de los crímenes en Arequipa.

#@returns None. Renderiza toda la interfaz principal.

def app() -> None:
    st.set_page_config(
        page_title='Sistema de Alertas - Arequipa',
        layout='wide',
        initial_sidebar_state='expanded'
    )
    
    st.title('CRIMENGO')
    
    # Verificar autenticación de administrador
    is_admin = admin_login_ui()
    
    # Mostrar panel adecuado según rol
    if is_admin:
        zone_name, zone_info = admin_panel()
    else:
        public_view()
        zone_name = "Centro Histórico"
        zone_info = AREQUIPA_ZONES[zone_name]
    
    # Configuración lateral de parámetros
    with st.sidebar:
        st.markdown("---")
        st.subheader("📡 Configuración")
        limit = st.number_input(
            'Registros a mostrar',
            min_value=100,
            max_value=10000,
            value=2000,
            step=100
        )
        
        if is_admin:
            auto_refresh = st.checkbox('Auto-refresh 60s', value=False)
            force_refresh = st.button(' Refrescar Ahora')
        else:
            auto_refresh = True
            force_refresh = False
    
    # Obtener datos actualizados
    fetch_fn = getattr(data_module, 'fetch_latest')
    df = fetch_fn(
        limit=int(limit),
        force=force_refresh,
        refresh_interval=60 if auto_refresh else 999999
    )
    
    # Mostrar métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Registros", len(df))
    
    with col2:
        if 'date' in df.columns and not df['date'].isna().all():
            latest = df['date'].max()
            st.metric("Último Reporte", latest.strftime('%d/%m/%Y %H:%M') if pd.notna(latest) else 'N/A')
        else:
            st.metric("Último Reporte", "N/A")
    
    with col3:
        arrests = df['arrest'].sum() if 'arrest' in df.columns else 0
        st.metric("Arrestos", int(arrests))
    
    with col4:
        domestic = df['domestic'].sum() if 'domestic' in df.columns else 0
        st.metric("Domésticos", int(domestic))
    
    # Secciones con pestañas (mapa, estadísticas, datos)
    tab1, tab2, tab3 = st.tabs(["Mapa", "Estadísticas", "Datos"])
    
    with tab1:
        st.subheader(f"Mapa de Incidentes - {zone_name}")
        show_map_points_and_heat(df, heat_threshold=30)
    
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            show_primary_type_bar(df)
        with col2:
            show_additional_charts(df)
    
    with tab3:
        st.subheader("Tabla de Datos")
        
        # Filtros interactivos
        col1, col2 = st.columns(2)
        with col1:
            if 'primary_type' in df.columns:
                types = st.multiselect(
                    "Filtrar por tipo",
                    options=df['primary_type'].dropna().unique(),
                    default=None
                )
                if types:
                    df = df[df['primary_type'].isin(types)]
        
        with col2:
            if 'arrest' in df.columns:
                show_arrests = st.checkbox("Solo con arresto", value=False)
                if show_arrests:
                    df = df[df['arrest'] == True]
        
        # Ajustar la altura de la tabla para mostrar más filas generadas
        try:
            rows_count = len(df)
        except Exception:
            rows_count = 0
        per_row_px = 32
        max_height = 1200
        min_height = 400
        desired_height = min(max_height, max(min_height, per_row_px * rows_count + 140))

        st.dataframe(
            df,
            height=int(desired_height)
        )
        
        # Información técnica solo visible para admin
        if is_admin:
            with st.expander("ℹ️ Información Técnica"):
                st.write("**Columnas disponibles:**", list(df.columns))
                st.write("**Registros nulos por columna:**")
                st.write(df.isnull().sum())


if __name__ == '__main__':
    app()

