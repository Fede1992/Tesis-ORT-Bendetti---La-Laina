# src/structs/transcripts.py
from __future__ import annotations
from enum import Enum
from typing import Optional, List
from datetime import date
from pydantic import BaseModel, HttpUrl, Field

class Chamber(Enum):
    DIPUTADOS = "Cámara de Diputados"
    SENADORES = "Cámara de Senadores"
    ASAMBLEA_GENERAL = "Asamblea General"
    COMISION_PERMANENTE = "Comisión Permanente"
    DESCONOCIDA = "Desconocida"

class DocumentType(Enum):
    DIARIO_DE_SESION = "Diario de Sesión"
    OTRO = "Otro"  # por si luego aparecen resoluciones, versiones taquigráficas, etc.

class TranscriptDoc(BaseModel):
    # Identificación y metadatos del documento PDF
    doc_id: str = Field(..., description="ID numérico del 'diario de sesión' en el sitio")
    chamber: Chamber = Chamber.DESCONOCIDA
    document_type: DocumentType = DocumentType.DIARIO_DE_SESION

    # Metadata opcional que podrías extraer del índice o ya del PDF
    session_date: Optional[date] = None
    title: Optional[str] = None          # ej. 'Diario de Sesión Nº 1234'
    legislature: Optional[str] = None    # si lo encontrás, p.ej. '49ª Legislatura'
    session_number: Optional[str] = None # si aparece en el título
    page_count: Optional[int] = None

    # Orígenes
    index_url: Optional[HttpUrl] = None  # URL de la página de listado donde lo viste
    pdf_url: HttpUrl                     # URL final del PDF (/IMG)

    # Archivos locales (paths relativos) y tamaños
    pdf_path: str                         # data/raw/pdfs/diario_<id>.pdf
    txt_path: Optional[str] = None        # data/txt/diario_<id>.txt si extraes texto
    text_len: Optional[int] = None        # len del .txt para sanity check
    sha1: Optional[str] = None            # hash del PDF (integridad / dedupe)

    # Campos de control
    notes: Optional[str] = None           # para flags (ej. descarga reintentada, etc.)

class Speech(BaseModel):
    """
    Para una etapa 2 (cuando puedas segmentar el texto):
    cada intervención normalizada. Probablemente no la uses
    hasta que dejes de depender del PDF crudo.
    """
    doc_id: str
    chamber: Chamber
    session_date: Optional[date] = None

    speaker: Optional[str] = None
    role: Optional[str] = None
    time_str: Optional[str] = None

    text: str
    # trazabilidad
    pdf_url: Optional[HttpUrl] = None
    index_url: Optional[HttpUrl] = None

class PageText(BaseModel):
    """
    Alternativa intermedia si primero querés texto por página.
    Útil si luego corrés heurísticas/NLP para segmentar en Speech.
    """
    doc_id: str
    page: int
    text: str

