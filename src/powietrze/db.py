"""Modele bazy danych SQLAlchemy."""

from datetime import datetime
from decimal import Decimal
from enum import Enum as PyEnum

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker


class Base(DeclarativeBase):
    """Bazowa klasa dla modeli SQLAlchemy."""

    pass


class ImportStatus(PyEnum):
    """Status importu pliku."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class Station(Base):
    """Model stacji pomiarowej."""

    __tablename__ = "stations"

    id = Column(Integer, primary_key=True)
    code = Column(Text, unique=True, nullable=False, index=True)

    measurements = relationship("Measurement", back_populates="station")

    def __repr__(self) -> str:
        return f"<Station(code={self.code!r})>"


class Indicator(Base):
    """Model wskaźnika pomiarowego (np. PM10, NO2)."""

    __tablename__ = "indicators"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    unit = Column(Text)

    __table_args__ = (UniqueConstraint("name", "unit", name="uq_indicator_name_unit"),)

    measurements = relationship("Measurement", back_populates="indicator")

    def __repr__(self) -> str:
        return f"<Indicator(name={self.name!r}, unit={self.unit!r})>"


class ImportFile(Base):
    """Model pliku importu - śledzi status importu poszczególnych plików."""

    __tablename__ = "import_files"

    id = Column(Integer, primary_key=True)
    archive_name = Column(Text, nullable=False)  # np. "dane/2024.zip"
    filename = Column(Text, nullable=False)  # np. "2024_PM10_24g.xlsx"
    status = Column(
        Enum(ImportStatus, name="import_status"),
        nullable=False,
        default=ImportStatus.PENDING,
    )
    records_imported = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    error_message = Column(Text)
    created_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("archive_name", "filename", name="uq_import_file"),
    )

    measurements = relationship("Measurement", back_populates="import_file")

    def __repr__(self) -> str:
        return f"<ImportFile(filename={self.filename!r}, status={self.status})>"


class Measurement(Base):
    """Model pomiaru jakości powietrza."""

    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    indicator_id = Column(Integer, ForeignKey("indicators.id"), nullable=False)
    import_file_id = Column(Integer, ForeignKey("import_files.id"), nullable=False)
    averaging_time = Column(Text, nullable=False)  # np. "1g", "24g", "1m"
    measured_at = Column(DateTime, nullable=False)
    value = Column(Numeric(10, 2))

    station = relationship("Station", back_populates="measurements")
    indicator = relationship("Indicator", back_populates="measurements")
    import_file = relationship("ImportFile", back_populates="measurements")

    __table_args__ = (
        UniqueConstraint(
            "station_id",
            "indicator_id",
            "averaging_time",
            "measured_at",
            name="uq_measurement",
        ),
        Index("ix_measurement_measured_at", "measured_at"),
        Index("ix_measurement_station_indicator", "station_id", "indicator_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<Measurement(station_id={self.station_id}, "
            f"indicator_id={self.indicator_id}, "
            f"measured_at={self.measured_at}, value={self.value})>"
        )


def get_engine(db_url: str):
    """Tworzy silnik SQLAlchemy dla podanego URL bazy danych."""
    return create_engine(db_url)


def get_session(db_url: str) -> Session:
    """Tworzy sesję SQLAlchemy dla podanego URL bazy danych."""
    engine = get_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def init_db(db_url: str) -> None:
    """Inicjalizuje bazę danych - tworzy wszystkie tabele."""
    engine = get_engine(db_url)
    Base.metadata.create_all(engine)


def get_or_create_station(session: Session, code: str) -> Station:
    """Pobiera lub tworzy stację o podanym kodzie."""
    station = session.query(Station).filter(Station.code == code).first()
    if station is None:
        station = Station(code=code)
        session.add(station)
        session.flush()
    return station


def get_or_create_indicator(session: Session, name: str, unit: str) -> Indicator:
    """Pobiera lub tworzy wskaźnik o podanej nazwie i jednostce."""
    indicator = (
        session.query(Indicator)
        .filter(Indicator.name == name, Indicator.unit == unit)
        .first()
    )
    if indicator is None:
        indicator = Indicator(name=name, unit=unit)
        session.add(indicator)
        session.flush()
    return indicator


def get_or_create_import_file(
    session: Session, archive_name: str, filename: str
) -> ImportFile:
    """Pobiera lub tworzy rekord pliku importu."""
    import_file = (
        session.query(ImportFile)
        .filter(
            ImportFile.archive_name == archive_name,
            ImportFile.filename == filename,
        )
        .first()
    )
    if import_file is None:
        import_file = ImportFile(
            archive_name=archive_name,
            filename=filename,
            status=ImportStatus.PENDING,
        )
        session.add(import_file)
        session.flush()
    return import_file


def is_file_completed(session: Session, archive_name: str, filename: str) -> bool:
    """Sprawdza czy plik został już w pełni zaimportowany."""
    import_file = (
        session.query(ImportFile)
        .filter(
            ImportFile.archive_name == archive_name,
            ImportFile.filename == filename,
            ImportFile.status == ImportStatus.COMPLETED,
        )
        .first()
    )
    return import_file is not None
