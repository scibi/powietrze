"""Logika importu danych do bazy PostgreSQL."""

import csv
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Callable

from sqlalchemy import text
from sqlalchemy.orm import Session

from .db import (
    ImportFile,
    ImportStatus,
    Indicator,
    Station,
    get_engine,
    get_or_create_import_file,
    get_or_create_indicator,
    get_or_create_station,
    get_session,
    init_db,
    is_file_completed,
)
from .parser import MeasurementRecord, iter_xlsx_from_zip, parse_xlsx_file


def prepare_measurements_data(
    session: Session,
    records: list[MeasurementRecord],
    import_file_id: int,
) -> tuple[StringIO, int, int]:
    """
    Przygotowuje dane do COPY - rozwiązuje stacje i wskaźniki, tworzy CSV.

    Returns:
        Tuple (StringIO z danymi CSV, liczba_rekordów, liczba_pominiętych)
    """
    # Cache dla stacji i wskaźników
    station_cache: dict[str, int] = {}
    indicator_cache: dict[tuple[str, str], int] = {}

    # Przygotuj bufor CSV
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer, delimiter="\t", lineterminator="\n")

    imported = 0
    skipped = 0

    for record in records:
        # Pomijamy rekordy bez wartości
        if record.value is None:
            skipped += 1
            continue

        # Pobierz lub utwórz stację (z cache)
        if record.station_code not in station_cache:
            station = get_or_create_station(session, record.station_code)
            station_cache[record.station_code] = station.id
        station_id = station_cache[record.station_code]

        # Pobierz lub utwórz wskaźnik (z cache)
        indicator_key = (record.indicator_name, record.unit)
        if indicator_key not in indicator_cache:
            indicator = get_or_create_indicator(
                session, record.indicator_name, record.unit
            )
            indicator_cache[indicator_key] = indicator.id
        indicator_id = indicator_cache[indicator_key]

        # Zapisz wiersz do CSV
        writer.writerow(
            [
                station_id,
                indicator_id,
                import_file_id,
                record.averaging_time,
                record.measured_at.isoformat(),
                f"{record.value:.2f}",
            ]
        )
        imported += 1

    # Commit stacji i wskaźników
    session.commit()

    # Przewiń bufor na początek
    csv_buffer.seek(0)

    return csv_buffer, imported, skipped


def copy_measurements_to_db(
    session: Session,
    csv_buffer: StringIO,
) -> None:
    """
    Kopiuje dane z CSV do bazy używając COPY i tabeli tymczasowej.
    Obsługuje ON CONFLICT przez INSERT ... ON CONFLICT z tabeli tymczasowej.
    """
    connection = session.connection()
    raw_connection = connection.connection.dbapi_connection
    cursor = raw_connection.cursor()

    try:
        # Utwórz tabelę tymczasową
        cursor.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS measurements_staging (
                station_id INTEGER,
                indicator_id INTEGER,
                import_file_id INTEGER,
                averaging_time TEXT,
                measured_at TIMESTAMP,
                value NUMERIC(10, 2)
            ) ON COMMIT DROP
            """
        )

        # COPY dane do tabeli tymczasowej
        cursor.copy_expert(
            """
            COPY measurements_staging (
                station_id, indicator_id, import_file_id,
                averaging_time, measured_at, value
            ) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t')
            """,
            csv_buffer,
        )

        # INSERT z tabeli tymczasowej do docelowej z ON CONFLICT
        # Używamy DISTINCT ON aby usunąć duplikaty w ramach partii
        cursor.execute(
            """
            INSERT INTO measurements (
                station_id, indicator_id, import_file_id,
                averaging_time, measured_at, value
            )
            SELECT DISTINCT ON (station_id, indicator_id, averaging_time, measured_at)
                station_id, indicator_id, import_file_id,
                averaging_time, measured_at, value
            FROM measurements_staging
            ORDER BY station_id, indicator_id, averaging_time, measured_at,
                     import_file_id DESC
            ON CONFLICT ON CONSTRAINT uq_measurement
            DO UPDATE SET
                value = EXCLUDED.value,
                import_file_id = EXCLUDED.import_file_id
            """
        )

    finally:
        cursor.close()


def import_measurements_fast(
    session: Session,
    records: list[MeasurementRecord],
    import_file: ImportFile,
) -> tuple[int, int]:
    """
    Szybki import pomiarów używając COPY.

    Args:
        session: Sesja SQLAlchemy
        records: Lista rekordów do zaimportowania
        import_file: Rekord pliku importu

    Returns:
        Tuple (liczba_zaimportowanych, liczba_pominiętych)
    """
    if not records:
        return 0, 0

    # Przygotuj dane CSV
    csv_buffer, imported, skipped = prepare_measurements_data(
        session, records, import_file.id
    )

    if imported > 0:
        # Kopiuj do bazy
        copy_measurements_to_db(session, csv_buffer)

    return imported, skipped


def import_zip_file(
    db_url: str,
    zip_path: Path,
    on_file_start: Callable[[str], None] | None = None,
    on_file_complete: Callable[[str, int, int], None] | None = None,
    on_file_skip: Callable[[str], None] | None = None,
    on_progress: Callable[[int], None] | None = None,
    batch_size: int = 50000,
) -> tuple[int, int, int]:
    """
    Importuje wszystkie dane z archiwum zip do bazy danych.

    Args:
        db_url: URL połączenia z bazą PostgreSQL
        zip_path: Ścieżka do pliku zip
        on_file_start: Callback wywoływany na początku przetwarzania pliku
        on_file_complete: Callback wywoływany po zakończeniu przetwarzania pliku
        on_file_skip: Callback wywoływany gdy plik jest pomijany (już zaimportowany)
        on_progress: Callback wywoływany co batch_size rekordów
        batch_size: Rozmiar partii do commitowania

    Returns:
        Tuple (liczba_zaimportowanych, liczba_pominiętych, liczba_plików_pominiętych)
    """
    # Inicjalizuj bazę danych
    init_db(db_url)

    session = get_session(db_url)
    total_imported = 0
    total_skipped = 0
    files_skipped = 0
    archive_name = str(zip_path)

    try:
        for filename, content in iter_xlsx_from_zip(zip_path):
            # Sprawdź czy plik już został zaimportowany
            if is_file_completed(session, archive_name, filename):
                files_skipped += 1
                if on_file_skip:
                    on_file_skip(filename)
                continue

            if on_file_start:
                on_file_start(filename)

            # Pobierz lub utwórz rekord pliku importu
            import_file = get_or_create_import_file(session, archive_name, filename)
            import_file.status = ImportStatus.IN_PROGRESS
            session.commit()

            try:
                # Parsuj plik i zbierz rekordy
                records = list(parse_xlsx_file(content, filename))

                file_imported = 0
                file_skipped = 0

                # Importuj w partiach (większe partie dla COPY)
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]
                    imported, skipped = import_measurements_fast(
                        session, batch, import_file
                    )
                    file_imported += imported
                    file_skipped += skipped
                    session.commit()

                    if on_progress:
                        on_progress(file_imported)

                # Oznacz plik jako zakończony
                import_file.status = ImportStatus.COMPLETED
                import_file.records_imported = file_imported
                import_file.records_skipped = file_skipped
                import_file.completed_at = datetime.now()
                session.commit()

                total_imported += file_imported
                total_skipped += file_skipped

                if on_file_complete:
                    on_file_complete(filename, file_imported, file_skipped)

            except Exception as e:
                # Oznacz plik jako błędny
                session.rollback()
                import_file = get_or_create_import_file(session, archive_name, filename)
                import_file.status = ImportStatus.FAILED
                import_file.error_message = str(e)
                session.commit()
                raise

    finally:
        session.close()

    return total_imported, total_skipped, files_skipped


def import_multiple_zips(
    db_url: str,
    zip_paths: list[Path],
    on_archive_start: Callable[[str], None] | None = None,
    on_archive_complete: Callable[[str, int, int, int], None] | None = None,
    on_file_start: Callable[[str], None] | None = None,
    on_file_complete: Callable[[str, int, int], None] | None = None,
    on_file_skip: Callable[[str], None] | None = None,
) -> tuple[int, int, int]:
    """
    Importuje dane z wielu archiwów zip.

    Args:
        db_url: URL połączenia z bazą PostgreSQL
        zip_paths: Lista ścieżek do plików zip
        on_archive_start: Callback na początku przetwarzania archiwum
        on_archive_complete: Callback po zakończeniu przetwarzania archiwum
        on_file_start: Callback na początku przetwarzania pliku xlsx
        on_file_complete: Callback po zakończeniu przetwarzania pliku xlsx
        on_file_skip: Callback gdy plik jest pomijany

    Returns:
        Tuple (liczba_zaimportowanych, liczba_pominiętych, liczba_plików_pominiętych)
    """
    total_imported = 0
    total_skipped = 0
    total_files_skipped = 0

    for zip_path in zip_paths:
        if on_archive_start:
            on_archive_start(str(zip_path))

        imported, skipped, files_skipped = import_zip_file(
            db_url=db_url,
            zip_path=zip_path,
            on_file_start=on_file_start,
            on_file_complete=on_file_complete,
            on_file_skip=on_file_skip,
        )

        total_imported += imported
        total_skipped += skipped
        total_files_skipped += files_skipped

        if on_archive_complete:
            on_archive_complete(str(zip_path), imported, skipped, files_skipped)

    return total_imported, total_skipped, total_files_skipped
