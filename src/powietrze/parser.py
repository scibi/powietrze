"""Parser plików xlsx z danymi jakości powietrza."""

import zipfile
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Iterator

import pandas as pd


@dataclass
class MeasurementRecord:
    """Rekord pomiaru z pliku xlsx."""

    station_code: str
    indicator_name: str
    unit: str
    averaging_time: str
    measured_at: datetime
    value: float | None
    source_file: str


@dataclass
class FileMetadata:
    """Metadane pliku xlsx."""

    indicator_name: str
    averaging_time: str
    station_codes: list[str]
    units: list[str]
    data_start_col: int  # Kolumna od której zaczynają się dane
    data_start_row: int  # Wiersz od którego zaczynają się dane


def detect_data_start_row(df: pd.DataFrame) -> int:
    """
    Wykrywa wiersz od którego zaczynają się dane pomiarowe.
    Szuka pierwszego wiersza gdzie kolumna 0 zawiera timestamp/datę.
    
    Returns:
        Indeks wiersza z pierwszymi danymi
    """
    for idx in range(5, min(15, len(df))):  # Szukaj między wierszem 5 a 15
        val = df.iloc[idx, 0]
        if pd.isna(val):
            continue
        # Sprawdź czy to timestamp
        if isinstance(val, (datetime, pd.Timestamp)):
            return idx
        # Sprawdź czy string wygląda jak data
        if isinstance(val, str):
            try:
                datetime.fromisoformat(val.replace(" ", "T").split(".")[0])
                return idx
            except ValueError:
                continue
    # Domyślnie zakładamy wiersz 6
    return 6


def detect_format(df: pd.DataFrame) -> int:
    """
    Wykrywa format pliku xlsx i zwraca indeks kolumny startowej dla danych.
    
    Standardowy format: kolumna 0 = timestamp, dane od kolumny 1
    Format Depozycja: kolumna 0 = "Data od", kolumna 1 = "Data do", dane od kolumny 2
    
    Returns:
        Indeks kolumny od której zaczynają się dane pomiarowe
    """
    # Sprawdź czy w wierszu 5 (nagłówek "Kod stanowiska") kolumna 0 to "Data od"
    row5_col0 = str(df.iloc[5, 0]) if not pd.isna(df.iloc[5, 0]) else ""
    
    if "Data" in row5_col0:
        # Format z "Data od" / "Data do" - dane zaczynają się od kolumny 2
        return 2
    else:
        # Standardowy format - dane od kolumny 1
        return 1


def parse_xlsx_metadata(df: pd.DataFrame) -> FileMetadata:
    """
    Parsuje nagłówki pliku xlsx i zwraca metadane.

    Struktura nagłówków (wiersze 0-5 lub 0-6):
    - Wiersz 0: "Nr" i numery kolumn
    - Wiersz 1: "Kod stacji" - kody stacji pomiarowych
    - Wiersz 2: "Wskaźnik" - nazwa wskaźnika (np. PM10)
    - Wiersz 3: "Czas uśredniania" - np. 24g, 1g
    - Wiersz 4: "Jednostka" - np. ug/m3
    - Wiersz 5: "Kod stanowiska" - pełny kod (może być też rok w niektórych plikach)
    - Wiersz 6 (opcjonalny): "Czas pomiaru" - dodatkowy nagłówek w niektórych plikach
    """
    # Wykryj format pliku
    data_start_col = detect_format(df)
    data_start_row = detect_data_start_row(df)
    
    # Kody stacji są w wierszu 1, od odpowiedniej kolumny
    station_codes = df.iloc[1, data_start_col:].tolist()

    # Wskaźnik jest taki sam dla wszystkich kolumn - bierzemy z pierwszej kolumny danych
    indicator_name = str(df.iloc[2, data_start_col])

    # Czas uśredniania - tak samo
    averaging_time = str(df.iloc[3, data_start_col])

    # Jednostki dla każdej kolumny (mogą być różne teoretycznie)
    units = df.iloc[4, data_start_col:].tolist()

    return FileMetadata(
        indicator_name=indicator_name,
        averaging_time=averaging_time,
        station_codes=[str(code) for code in station_codes],
        units=[str(unit) for unit in units],
        data_start_col=data_start_col,
        data_start_row=data_start_row,
    )


def parse_xlsx_file(
    file_content: bytes, source_filename: str
) -> Iterator[MeasurementRecord]:
    """
    Parsuje plik xlsx i generuje rekordy pomiarów.

    Args:
        file_content: Zawartość pliku xlsx jako bytes
        source_filename: Nazwa pliku źródłowego

    Yields:
        MeasurementRecord dla każdego pomiaru
    """
    # Wczytaj cały plik bez nagłówków
    df = pd.read_excel(BytesIO(file_content), header=None)

    # Parsuj metadane z nagłówków
    metadata = parse_xlsx_metadata(df)

    # Dane pomiarowe zaczynają się od wykrytego wiersza
    data_df = df.iloc[metadata.data_start_row:]

    # Iteruj po wierszach z danymi
    for _, row in data_df.iterrows():
        # Pierwsza kolumna to timestamp (kolumna 0)
        timestamp = row.iloc[0]

        # Konwertuj timestamp do datetime
        if pd.isna(timestamp):
            continue

        if isinstance(timestamp, str):
            try:
                measured_at = datetime.fromisoformat(timestamp)
            except ValueError:
                # Pomijamy wiersze z nieprawidłowym formatem daty
                continue
        elif isinstance(timestamp, pd.Timestamp):
            measured_at = timestamp.to_pydatetime()
        else:
            try:
                measured_at = pd.to_datetime(timestamp).to_pydatetime()
            except Exception:
                continue

        # Iteruj po kolumnach z danymi (od data_start_col)
        for col_idx, station_code in enumerate(metadata.station_codes):
            # Indeks w wierszu to data_start_col + col_idx
            actual_col = metadata.data_start_col + col_idx
            value = row.iloc[actual_col]

            # Pomijamy NaN wartości
            if pd.isna(value):
                value_float = None
            else:
                try:
                    # Obsługa przecinka jako separatora dziesiętnego
                    if isinstance(value, str):
                        value = value.replace(",", ".")
                    value_float = float(value)
                except (ValueError, TypeError):
                    value_float = None

            yield MeasurementRecord(
                station_code=station_code,
                indicator_name=metadata.indicator_name,
                unit=metadata.units[col_idx],
                averaging_time=metadata.averaging_time,
                measured_at=measured_at,
                value=value_float,
                source_file=source_filename,
            )


# Pliki do pominięcia podczas importu (inna struktura danych)
SKIP_PATTERNS = [
    "Depozycja",
]


def should_skip_file(filename: str) -> bool:
    """Sprawdza czy plik powinien być pominięty podczas importu."""
    return any(pattern in filename for pattern in SKIP_PATTERNS)


def iter_xlsx_from_zip(zip_path: Path) -> Iterator[tuple[str, bytes]]:
    """
    Iteruje po plikach xlsx w archiwum zip.

    Args:
        zip_path: Ścieżka do pliku zip

    Yields:
        Tuple (nazwa_pliku, zawartość_bytes) dla każdego pliku xlsx
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        for filename in zf.namelist():
            if filename.endswith(".xlsx") and not should_skip_file(filename):
                content = zf.read(filename)
                yield filename, content


def parse_zip_archive(zip_path: Path) -> Iterator[MeasurementRecord]:
    """
    Parsuje wszystkie pliki xlsx z archiwum zip.

    Args:
        zip_path: Ścieżka do pliku zip

    Yields:
        MeasurementRecord dla każdego pomiaru ze wszystkich plików
    """
    for filename, content in iter_xlsx_from_zip(zip_path):
        yield from parse_xlsx_file(content, filename)
