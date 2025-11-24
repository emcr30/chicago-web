# CrimeGO

Este proyecto crea una app Streamlit que consume la API pública de la ciudad de Chicago y permite: 

- Traer datos desde la API (filtrando desde 2024 por defecto).
- Inyectar registros sintéticos con la estructura descrita (para geolocalizar, p. ej. cerca de tu casa).
- Persistir (opcional) los datos en una base SQLite local `chicago.db`.
- Visualizaciones simples: conteo por tipo, serie temporal por mes y mapa de puntos.

Requisitos

- Python 3.10+ (recomendado)
- Instalar dependencias:

```bash
python -m pip install -r requirements.txt
```

Ejecutar localmente

```bash
streamlit run main.py
```

Notas sobre volumen de datos y despliegue en la nube

- La API contiene muchos registros. Por defecto la app trae hasta 5k registros en memoria. Si quieres persistir todo 2024+ la app paginará y guardará en `chicago.db`.
- Para producción y datos grandes recomiendo usar un RDS/managed DB (Azure SQL, AWS RDS/Postgres) o un data lake. Si quieres puedo añadir la integración a Azure SQL o a AWS RDS.

Siguientes pasos sugeridos

- Añadir autenticación (si el servicio se publica en la nube).
- Mover persistencia a Postgres en la nube y añadir ETL programado para mantener datos recientes.
- Mejorar visualizaciones con Plotly o Kepler for large geospatial views.

Se puede visualizar la web ingresando al siguiente enlace:
[CRIMEN GO](https://crimengo.azurewebsites.net/)
