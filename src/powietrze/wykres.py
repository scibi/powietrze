#!/usr/bin/env python3
"""Generowanie wykresów danych jakości powietrza."""

import os
from typing import Annotated, Optional

import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine, text

import typer

app = typer.Typer(help="Generowanie wykresów danych jakości powietrza")


def get_engine(db_url: str):
    """Tworzy silnik SQLAlchemy."""
    return create_engine(db_url)


@app.command("miesiecznie")
def wykres_miesieczny(
    stacja: Annotated[str, typer.Argument(help="Kod stacji pomiarowej (np. MzWarAlNiepo)")],
    wskaznik: Annotated[str, typer.Argument(help="Nazwa wskaźnika (np. NO2, PM10, PM25)")] = "NO2",
    rok_od: Annotated[Optional[int], typer.Option("--od", help="Rok początkowy")] = None,
    rok_do: Annotated[Optional[int], typer.Option("--do", help="Rok końcowy")] = None,
    output: Annotated[str, typer.Option("-o", "--output", help="Ścieżka pliku wyjściowego")] = "wykres.png",
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ] = "",
) -> None:
    """Generuje wykres średnich miesięcznych dla wybranej stacji i wskaźnika."""
    
    if not db_url:
        typer.echo("❌ Brak URL bazy danych. Ustaw DATABASE_URL lub użyj --db-url", err=True)
        raise typer.Exit(code=1)
    
    engine = get_engine(db_url)
    
    # Buduj warunki WHERE
    warunki = []
    if rok_od:
        warunki.append(f"measured_at >= '{rok_od}-01-01'")
    if rok_do:
        warunki.append(f"measured_at < '{rok_do + 1}-01-01'")
    
    warunki_sql = " AND ".join(warunki) if warunki else "TRUE"
    
    # Zapytanie SQL
    query = f"""
    SELECT 
        left(date_trunc('month', measured_at)::text, 7) AS miesiac,
        count(*) AS liczba_pomiarow,
        round(avg(value)::numeric, 2) AS srednia 
    FROM measurements 
    WHERE station_id = (SELECT id FROM stations WHERE code = :stacja) 
      AND indicator_id = (SELECT id FROM indicators WHERE name = :wskaznik)
      AND {warunki_sql}
    GROUP BY miesiac 
    ORDER BY miesiac
    """
    
    # Pobierz dane
    try:
        df = pd.read_sql(text(query), engine, params={"stacja": stacja, "wskaznik": wskaznik})
    except Exception as e:
        typer.echo(f"❌ Błąd pobierania danych: {e}", err=True)
        raise typer.Exit(code=1)
    
    if df.empty:
        typer.echo(f"❌ Brak danych dla stacji '{stacja}' i wskaźnika '{wskaznik}'", err=True)
        raise typer.Exit(code=1)
    
    typer.echo(f"📊 Znaleziono {len(df)} miesięcy danych")
    
    # Dodaj kolumnę z rokiem i numerem miesiąca
    df["rok"] = df["miesiac"].str[:4]
    df["nr_miesiaca"] = df["miesiac"].str[5:7]
    
    # Oblicz średnią roczną
    srednie_roczne = df.groupby("rok")["srednia"].mean()
    
    # Analiza sezonowości - średnie dla każdego miesiąca (01-12) ze wszystkich lat
    nazwy_miesiecy = {
        "01": "styczeń", "02": "luty", "03": "marzec", "04": "kwiecień",
        "05": "maj", "06": "czerwiec", "07": "lipiec", "08": "sierpień",
        "09": "wrzesień", "10": "październik", "11": "listopad", "12": "grudzień"
    }
    srednie_miesieczne = df.groupby("nr_miesiaca")["srednia"].mean().sort_values()
    
    # Top 3 i bottom 3 miesiące (z obsługą ex aequo)
    # Znajdź próg dla 3. miejsca i weź wszystkie miesiące >= lub <= tego progu
    sorted_asc = srednie_miesieczne.sort_values()
    sorted_desc = srednie_miesieczne.sort_values(ascending=False)
    
    # Bottom: wartość 3. najniższego (lub ostatniego jeśli mniej niż 3)
    bottom_threshold = sorted_asc.iloc[min(2, len(sorted_asc) - 1)]
    bottom_months = sorted_asc[sorted_asc <= bottom_threshold].index.tolist()
    
    # Top: wartość 3. najwyższego
    top_threshold = sorted_desc.iloc[min(2, len(sorted_desc) - 1)]
    top_months = sorted_desc[sorted_desc >= top_threshold].index.tolist()
    
    # Zlicz ile razy każdy miesiąc był w top/bottom 3 w każdym roku
    top_count = {m: 0 for m in nazwy_miesiecy.keys()}
    bottom_count = {m: 0 for m in nazwy_miesiecy.keys()}
    
    for rok, group in df.groupby("rok"):
        if len(group) >= 3:
            sorted_group = group.sort_values("srednia")
            # Bottom 3 z ex aequo
            bottom_thresh_rok = sorted_group["srednia"].iloc[min(2, len(sorted_group) - 1)]
            for _, row in sorted_group[sorted_group["srednia"] <= bottom_thresh_rok].iterrows():
                bottom_count[row["nr_miesiaca"]] += 1
            # Top 3 z ex aequo
            top_thresh_rok = sorted_group["srednia"].iloc[max(0, len(sorted_group) - 3)]
            for _, row in sorted_group[sorted_group["srednia"] >= top_thresh_rok].iterrows():
                top_count[row["nr_miesiaca"]] += 1
    
    # Formatuj nazwy z liczbą wystąpień (sortuj po liczbie wystąpień, malejąco)
    bottom_sorted = sorted(bottom_months, key=lambda m: bottom_count[m], reverse=True)
    top_sorted = sorted(top_months, key=lambda m: top_count[m], reverse=True)
    
    bottom_names = [f"{nazwy_miesiecy[m]} ({bottom_count[m]}x)" for m in bottom_sorted]
    top_names = [f"{nazwy_miesiecy[m]} ({top_count[m]}x)" for m in top_sorted]
    
    # Funkcja do oznaczenia top/bottom 3 w każdym roku
    def oznacz_kolory(group):
        sorted_group = group.sort_values("srednia")
        n = len(sorted_group)
        colors = ["#4a90d9"] * n
        
        if n >= 3:
            bottom_idx = sorted_group.head(3).index
            top_idx = sorted_group.tail(3).index
            
            for i, idx in enumerate(group.index):
                if idx in bottom_idx:
                    colors[list(group.index).index(idx)] = "#2ecc71"
                elif idx in top_idx:
                    colors[list(group.index).index(idx)] = "#e74c3c"
        
        return colors
    
    # Zbierz kolory dla wszystkich słupków
    all_colors = []
    for rok, group in df.groupby("rok"):
        colors = oznacz_kolory(group)
        all_colors.extend(colors)
    
    # Wykres - zwiększona wysokość dla tekstu na dole
    fig, ax = plt.subplots(figsize=(14, 7))
    
    bars = ax.bar(range(len(df)), df["srednia"], color=all_colors, edgecolor="#333", linewidth=0.5)
    
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df["miesiac"], rotation=45, ha="right")
    
    ax.set_xlabel("Miesiąc", fontsize=12)
    ax.set_ylabel(f"Średnie stężenie {wskaznik} [µg/m³]", fontsize=12)
    ax.set_title(f"Średnie miesięczne stężenie {wskaznik} - stacja {stacja}", fontsize=14)
    
    ax.grid(axis="y", alpha=0.3)
    
    # Linie przerywane dla średnich rocznych
    kolory_linii = ["#8b0000", "#00008b", "#006400", "#4b0082", "#ff8c00", "#2f4f4f"]
    for i, (rok, srednia) in enumerate(srednie_roczne.items()):
        rok_idx = df[df["rok"] == rok].index
        x_start = list(df.index).index(rok_idx[0])
        x_end = list(df.index).index(rok_idx[-1])
        
        kolor = kolory_linii[i % len(kolory_linii)]
        ax.hlines(
            y=srednia, 
            xmin=x_start - 0.4, 
            xmax=x_end + 0.4, 
            colors=kolor, 
            linestyles="dashed", 
            linewidth=2,
        )
        ax.annotate(
            f"{srednia:.1f}",
            xy=(x_end + 0.5, srednia),
            fontsize=9,
            color=kolor,
            fontweight="bold",
            va="center"
        )
    
    # Legenda
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    
    legend_elements = [
        Patch(facecolor="#e74c3c", edgecolor="#333", label="Top 3 (najwyższe w roku)"),
        Patch(facecolor="#4a90d9", edgecolor="#333", label="Pozostałe"),
        Patch(facecolor="#2ecc71", edgecolor="#333", label="Bottom 3 (najniższe w roku)"),
        Line2D([0], [0], color="gray", linestyle="--", linewidth=2, label="Średnia roczna"),
    ]
    ax.legend(handles=legend_elements, loc="upper right")
    
    # Dodaj informację o sezonowości pod wykresem
    sezonowosc_text = (
        f"▲ Typowo najwyższe wartości: {', '.join(top_names)}\n"
        f"▼ Typowo najniższe wartości: {', '.join(bottom_names)}"
    )
    fig.text(
        0.5, 0.01, sezonowosc_text,
        ha="center", va="bottom", fontsize=10,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", edgecolor="gray", alpha=0.9)
    )
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.22)  # Miejsce na tekst
    plt.savefig(output, dpi=150)
    typer.echo(f"✅ Wykres zapisany do: {output}")
    typer.echo(f"📈 Typowo najwyższe wartości: {', '.join(top_names)}")
    typer.echo(f"📉 Typowo najniższe wartości: {', '.join(bottom_names)}")


@app.command("stacje")
def lista_stacji(
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ] = "",
    szukaj: Annotated[Optional[str], typer.Option("--szukaj", "-s", help="Filtruj po kodzie stacji")] = None,
) -> None:
    """Wyświetla listę dostępnych stacji pomiarowych."""
    
    if not db_url:
        typer.echo("❌ Brak URL bazy danych. Ustaw DATABASE_URL lub użyj --db-url", err=True)
        raise typer.Exit(code=1)
    
    engine = get_engine(db_url)
    
    query = """
    SELECT s.code, count(m.id) as liczba_pomiarow
    FROM stations s
    LEFT JOIN measurements m ON s.id = m.station_id
    GROUP BY s.code
    ORDER BY s.code
    """
    
    df = pd.read_sql(text(query), engine)
    
    if szukaj:
        df = df[df["code"].str.contains(szukaj, case=False)]
    
    typer.echo(f"📍 Stacje pomiarowe ({len(df)}):\n")
    for _, row in df.iterrows():
        typer.echo(f"   {row['code']}: {row['liczba_pomiarow']} pomiarów")


@app.command("wskazniki")
def lista_wskaznikow(
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ] = "",
) -> None:
    """Wyświetla listę dostępnych wskaźników pomiarowych."""
    
    if not db_url:
        typer.echo("❌ Brak URL bazy danych. Ustaw DATABASE_URL lub użyj --db-url", err=True)
        raise typer.Exit(code=1)
    
    engine = get_engine(db_url)
    
    query = """
    SELECT i.name, i.unit, count(m.id) as liczba_pomiarow
    FROM indicators i
    LEFT JOIN measurements m ON i.id = m.indicator_id
    GROUP BY i.name, i.unit
    ORDER BY count(m.id) DESC
    """
    
    df = pd.read_sql(text(query), engine)
    
    typer.echo(f"📈 Wskaźniki pomiarowe ({len(df)}):\n")
    for _, row in df.iterrows():
        typer.echo(f"   {row['name']} [{row['unit']}]: {row['liczba_pomiarow']} pomiarów")


@app.command("sezonowosc")
def analiza_sezonowosci(
    stacja: Annotated[str, typer.Argument(help="Kod stacji pomiarowej (np. MzWarAlNiepo)")],
    wskaznik: Annotated[str, typer.Argument(help="Nazwa wskaźnika (np. NO2, PM10, PM25)")] = "NO2",
    rok_od: Annotated[Optional[int], typer.Option("--od", help="Rok początkowy")] = None,
    rok_do: Annotated[Optional[int], typer.Option("--do", help="Rok końcowy")] = None,
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            "-d",
            envvar="DATABASE_URL",
            help="URL połączenia z bazą PostgreSQL",
        ),
    ] = "",
) -> None:
    """Analizuje sezonowość - znajduje 3-miesięczne okna z najwyższymi i najniższymi wartościami."""
    
    if not db_url:
        typer.echo("❌ Brak URL bazy danych. Ustaw DATABASE_URL lub użyj --db-url", err=True)
        raise typer.Exit(code=1)
    
    engine = get_engine(db_url)
    
    # Buduj warunki WHERE
    warunki = []
    if rok_od:
        warunki.append(f"measured_at >= '{rok_od}-01-01'")
    if rok_do:
        warunki.append(f"measured_at < '{rok_do + 1}-01-01'")
    
    warunki_sql = " AND ".join(warunki) if warunki else "TRUE"
    
    query = f"""
    SELECT 
        left(date_trunc('month', measured_at)::text, 7) AS miesiac,
        round(avg(value)::numeric, 2) AS srednia 
    FROM measurements 
    WHERE station_id = (SELECT id FROM stations WHERE code = :stacja) 
      AND indicator_id = (SELECT id FROM indicators WHERE name = :wskaznik)
      AND {warunki_sql}
    GROUP BY miesiac 
    ORDER BY miesiac
    """
    
    try:
        df = pd.read_sql(text(query), engine, params={"stacja": stacja, "wskaznik": wskaznik})
    except Exception as e:
        typer.echo(f"❌ Błąd pobierania danych: {e}", err=True)
        raise typer.Exit(code=1)
    
    if df.empty:
        typer.echo(f"❌ Brak danych dla stacji '{stacja}' i wskaźnika '{wskaznik}'", err=True)
        raise typer.Exit(code=1)
    
    df["rok"] = df["miesiac"].str[:4].astype(int)
    df["nr_miesiaca"] = df["miesiac"].str[5:7].astype(int)
    
    nazwy = {1: "sty", 2: "lut", 3: "mar", 4: "kwi", 5: "maj", 6: "cze",
             7: "lip", 8: "sie", 9: "wrz", 10: "paź", 11: "lis", 12: "gru"}
    
    typer.echo("=" * 70)
    typer.echo(f"Analiza 3-miesięcznych okien dla {wskaznik} (stacja {stacja})")
    typer.echo("=" * 70)
    
    # Analiza dla każdego roku
    lata = sorted(df["rok"].unique())
    
    # Statystyki zbiorcze
    najlepsze_okna_count = {}
    najgorsze_okna_count = {}
    
    for rok in lata:
        # Pobierz dane dla tego roku i poprzedniego/następnego (dla okien na przełomie)
        df_rok = df[df["rok"].isin([rok - 1, rok, rok + 1])].copy()
        
        if len(df_rok) < 3:
            continue
        
        # Utwórz mapę: (rok, miesiąc) -> średnia
        dane = {}
        for _, row in df_rok.iterrows():
            dane[(row["rok"], row["nr_miesiaca"])] = row["srednia"]
        
        # Sprawdź wszystkie 3-miesięczne okna zaczynające się w danym roku
        wyniki_roku = []
        
        for start_miesiac in range(1, 13):
            # 3 kolejne miesiące
            okno = []
            for i in range(3):
                m = (start_miesiac + i - 1) % 12 + 1
                r = rok if (start_miesiac + i <= 12) else rok + 1
                if (r, m) in dane:
                    okno.append((r, m, dane[(r, m)]))
            
            if len(okno) == 3:
                srednia = sum(x[2] for x in okno) / 3
                miesiace = [x[1] for x in okno]
                nazwa = f"{nazwy[miesiace[0]]}-{nazwy[miesiace[1]]}-{nazwy[miesiace[2]]}"
                wyniki_roku.append((nazwa, srednia, okno))
        
        if not wyniki_roku:
            continue
        
        # Sortuj po średniej
        wyniki_roku.sort(key=lambda x: x[1])
        
        najgorsze = wyniki_roku[0]
        najlepsze = wyniki_roku[-1]
        
        # Zlicz dla statystyk
        najgorsze_okna_count[najgorsze[0]] = najgorsze_okna_count.get(najgorsze[0], 0) + 1
        najlepsze_okna_count[najlepsze[0]] = najlepsze_okna_count.get(najlepsze[0], 0) + 1
        
        roznica = najlepsze[1] - najgorsze[1]
        roznica_proc = (roznica / najgorsze[1]) * 100 if najgorsze[1] > 0 else 0
        
        typer.echo(f"\n📅 Rok {rok}:")
        typer.echo(f"   📉 Najniższe: {najgorsze[0]:12} = {najgorsze[1]:6.2f} µg/m³")
        typer.echo(f"   📈 Najwyższe: {najlepsze[0]:12} = {najlepsze[1]:6.2f} µg/m³")
        typer.echo(f"   📊 Różnica:   {roznica:6.2f} µg/m³ (+{roznica_proc:.0f}%)")
    
    # Podsumowanie
    typer.echo("\n" + "=" * 70)
    typer.echo("PODSUMOWANIE")
    typer.echo("=" * 70)
    
    typer.echo("\n📉 Najczęściej NAJNIŻSZE stężenia:")
    for okno, count in sorted(najgorsze_okna_count.items(), key=lambda x: -x[1]):
        typer.echo(f"   {okno}: {count}x")
    
    typer.echo("\n📈 Najczęściej NAJWYŻSZE stężenia:")
    for okno, count in sorted(najlepsze_okna_count.items(), key=lambda x: -x[1]):
        typer.echo(f"   {okno}: {count}x")


if __name__ == "__main__":
    app()

