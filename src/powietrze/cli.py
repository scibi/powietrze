"""Interfejs wiersza poleceń (CLI) dla narzędzia powietrze."""

from pathlib import Path
from typing import Annotated

import typer

from .db import ImportFile, ImportStatus, init_db
from .importer import import_multiple_zips

app = typer.Typer(
    name="powietrze",
    help="Narzędzie do importu danych jakości powietrza do PostgreSQL",
)


@app.command("init-db")
def cmd_init_db(
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ],
) -> None:
    """Inicjalizuje bazę danych - tworzy wymagane tabele."""
    typer.echo("Inicjalizacja bazy danych...")
    init_db(db_url)
    typer.echo("Baza danych zainicjalizowana pomyślnie.")


@app.command("import")
def cmd_import(
    zip_files: Annotated[
        list[Path],
        typer.Argument(
            help="Ścieżki do plików zip z danymi xlsx",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ],
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Wyświetl szczegółowe informacje o postępie",
        ),
    ] = False,
) -> None:
    """Importuje dane z plików zip do bazy PostgreSQL."""

    def on_archive_start(path: str) -> None:
        typer.echo(f"\n📦 Przetwarzanie archiwum: {path}")

    def on_archive_complete(
        path: str, imported: int, skipped: int, files_skipped: int
    ) -> None:
        typer.echo(
            f"   ✅ Zakończono: {imported} zaimportowanych, "
            f"{skipped} pominiętych (NaN), {files_skipped} plików pominiętych"
        )

    def on_file_start(filename: str) -> None:
        if verbose:
            typer.echo(f"   📄 Przetwarzanie: {filename}")

    def on_file_complete(filename: str, imported: int, skipped: int) -> None:
        if verbose:
            typer.echo(f"      → {imported} zaimportowanych, {skipped} pominiętych")

    def on_file_skip(filename: str) -> None:
        if verbose:
            typer.echo(f"   ⏭️  Pominięto (już zaimportowany): {filename}")

    typer.echo("🌍 Import danych jakości powietrza")
    typer.echo(f"   Pliki: {len(zip_files)}")

    try:
        total_imported, total_skipped, total_files_skipped = import_multiple_zips(
            db_url=db_url,
            zip_paths=zip_files,
            on_archive_start=on_archive_start,
            on_archive_complete=on_archive_complete,
            on_file_start=on_file_start,
            on_file_complete=on_file_complete,
            on_file_skip=on_file_skip,
        )

        typer.echo(f"\n🎉 Import zakończony!")
        typer.echo(f"   Łącznie zaimportowano: {total_imported} pomiarów")
        typer.echo(f"   Łącznie pominięto: {total_skipped} (wartości NaN)")
        typer.echo(f"   Plików pominiętych: {total_files_skipped} (już zaimportowane)")

    except Exception as e:
        typer.echo(f"\n❌ Błąd podczas importu: {e}", err=True)
        raise typer.Exit(code=1)


@app.command("stats")
def cmd_stats(
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ],
) -> None:
    """Wyświetla statystyki zaimportowanych danych."""
    from sqlalchemy import func

    from .db import Indicator, Measurement, Station, get_session

    session = get_session(db_url)

    try:
        stations_count = session.query(func.count(Station.id)).scalar()
        indicators_count = session.query(func.count(Indicator.id)).scalar()
        measurements_count = session.query(func.count(Measurement.id)).scalar()

        typer.echo("📊 Statystyki bazy danych:")
        typer.echo(f"   Stacje pomiarowe: {stations_count}")
        typer.echo(f"   Wskaźniki: {indicators_count}")
        typer.echo(f"   Pomiary: {measurements_count}")

        # Top 5 stacji z największą liczbą pomiarów
        top_stations = (
            session.query(Station.code, func.count(Measurement.id).label("count"))
            .join(Measurement)
            .group_by(Station.code)
            .order_by(func.count(Measurement.id).desc())
            .limit(5)
            .all()
        )

        if top_stations:
            typer.echo("\n   Top 5 stacji (liczba pomiarów):")
            for station_code, count in top_stations:
                typer.echo(f"      {station_code}: {count}")

        # Wskaźniki
        indicators = (
            session.query(
                Indicator.name,
                Indicator.unit,
                func.count(Measurement.id).label("count"),
            )
            .join(Measurement)
            .group_by(Indicator.name, Indicator.unit)
            .order_by(func.count(Measurement.id).desc())
            .all()
        )

        if indicators:
            typer.echo("\n   Wskaźniki (liczba pomiarów):")
            for name, unit, count in indicators:
                typer.echo(f"      {name} [{unit}]: {count}")

    finally:
        session.close()


@app.command("import-status")
def cmd_import_status(
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ],
    show_all: Annotated[
        bool,
        typer.Option(
            "--all",
            "-a",
            help="Pokaż wszystkie pliki (domyślnie tylko niezakończone)",
        ),
    ] = False,
) -> None:
    """Wyświetla status importu plików."""
    from .db import get_session

    session = get_session(db_url)

    try:
        query = session.query(ImportFile).order_by(
            ImportFile.archive_name, ImportFile.filename
        )

        if not show_all:
            query = query.filter(ImportFile.status != ImportStatus.COMPLETED)

        files = query.all()

        if not files:
            if show_all:
                typer.echo("Brak zaimportowanych plików.")
            else:
                typer.echo("Wszystkie pliki zostały pomyślnie zaimportowane.")
            return

        typer.echo("📁 Status importu plików:\n")

        status_icons = {
            ImportStatus.PENDING: "⏳",
            ImportStatus.IN_PROGRESS: "🔄",
            ImportStatus.COMPLETED: "✅",
            ImportStatus.FAILED: "❌",
        }

        current_archive = None
        for f in files:
            if f.archive_name != current_archive:
                current_archive = f.archive_name
                typer.echo(f"📦 {current_archive}")

            icon = status_icons.get(f.status, "?")
            line = f"   {icon} {f.filename}"

            if f.status == ImportStatus.COMPLETED:
                line += f" ({f.records_imported} zaimportowanych)"
            elif f.status == ImportStatus.FAILED and f.error_message:
                line += f" - {f.error_message[:50]}..."

            typer.echo(line)

    finally:
        session.close()


@app.command("reset-failed")
def cmd_reset_failed(
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ],
) -> None:
    """Resetuje status plików z błędami do 'pending', aby można było ponowić import."""
    from .db import get_session

    session = get_session(db_url)

    try:
        updated = (
            session.query(ImportFile)
            .filter(ImportFile.status.in_([ImportStatus.FAILED, ImportStatus.IN_PROGRESS]))
            .update({ImportFile.status: ImportStatus.PENDING}, synchronize_session=False)
        )
        session.commit()

        typer.echo(f"Zresetowano {updated} plików do statusu 'pending'.")

    finally:
        session.close()


if __name__ == "__main__":
    app()
