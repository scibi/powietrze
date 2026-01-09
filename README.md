# Powietrze

Narzędzie do importu i analizy danych na temat jakości powietrza pobranych z [Banku Danych GIOŚ](https://powietrze.gios.gov.pl/pjp/archives) do bazy PostgreSQL.

## Instalacja

```bash
uv sync
```

## Zmienna środowiskowa

Zamiast podawać `--db-url` przy każdym wywołaniu, możesz ustawić zmienną środowiskową:

```bash
export DATABASE_URL="postgresql://user:pass@localhost/powietrze"
```

## Import danych (`powietrze`)

### Inicjalizacja bazy danych

```bash
uv run powietrze init-db
```

### Import danych z archiwów ZIP

```bash
# Import jednego archiwum
uv run powietrze import dane/2024.zip

# Import wielu archiwów
uv run powietrze import dane/2024.zip dane/2023.zip

# Import z szczegółowym logiem
uv run powietrze import dane/2024.zip --verbose
```

### Statystyki

```bash
uv run powietrze stats
```

### Status importu

```bash
# Pokaż niezakończone pliki
uv run powietrze import-status

# Pokaż wszystkie pliki
uv run powietrze import-status --all

# Resetuj pliki z błędami
uv run powietrze reset-failed
```

## Wykresy i analiza (`powietrze-wykres`)

### Wykres średnich miesięcznych

Generuje wykres słupkowy ze średnimi miesięcznymi, oznaczając kolorem miesiące z najwyższymi i najniższymi wartościami w każdym roku.

```bash
# Podstawowy wykres
uv run powietrze-wykres miesiecznie MzWarAlNiepo NO2

# Z zakresem lat
uv run powietrze-wykres miesiecznie MzWarAlNiepo PM10 --od 2020 --do 2024

# Zapisz do pliku
uv run powietrze-wykres miesiecznie MzWarAlNiepo NO2 -o wykres.png
```

Wykres zawiera:
- Słupki kolorowane: czerwone (top 3 w roku), zielone (bottom 3 w roku), niebieskie (pozostałe)
- Linie przerywane pokazujące średnią roczną
- Informację o sezonowości (które miesiące typowo mają najwyższe/najniższe wartości)

#### Przykładowy wykres

![Przykładowy wykres NO2](example/wykres.png)

### Analiza sezonowości

Znajduje 3-miesięczne okna z najwyższymi i najniższymi wartościami dla każdego roku.

```bash
uv run powietrze-wykres sezonowosc MzWarAlNiepo NO2 --od 2020 --do 2024
```

#### Przykładowy wynik

```
======================================================================
Analiza 3-miesięcznych okien dla NO2 (stacja MzWarAlNiepo)
======================================================================

📅 Rok 2020:
   📉 Najniższe: lis-gru-sty  =  31.54 µg/m³
   📈 Najwyższe: lip-sie-wrz  =  43.92 µg/m³
   📊 Różnica:    12.38 µg/m³ (+39%)

📅 Rok 2021:
   📉 Najniższe: lis-gru-sty  =  37.17 µg/m³
   📈 Najwyższe: lip-sie-wrz  =  47.70 µg/m³
   📊 Różnica:    10.54 µg/m³ (+28%)

📅 Rok 2022:
   📉 Najniższe: lis-gru-sty  =  33.85 µg/m³
   📈 Najwyższe: mar-kwi-maj  =  48.35 µg/m³
   📊 Różnica:    14.50 µg/m³ (+43%)

📅 Rok 2023:
   📉 Najniższe: lis-gru-sty  =  34.52 µg/m³
   📈 Najwyższe: lip-sie-wrz  =  53.30 µg/m³
   📊 Różnica:    18.78 µg/m³ (+54%)

📅 Rok 2024:
   📉 Najniższe: paź-lis-gru  =  33.39 µg/m³
   📈 Najwyższe: cze-lip-sie  =  44.42 µg/m³
   📊 Różnica:    11.03 µg/m³ (+33%)

======================================================================
PODSUMOWANIE
======================================================================

📉 Najczęściej NAJNIŻSZE stężenia:
   lis-gru-sty: 4x
   paź-lis-gru: 1x

📈 Najczęściej NAJWYŻSZE stężenia:
   lip-sie-wrz: 3x
   mar-kwi-maj: 1x
   cze-lip-sie: 1x
```

### Lista stacji i wskaźników

```bash
# Lista stacji
uv run powietrze-wykres stacje

# Szukaj stacji
uv run powietrze-wykres stacje --szukaj War

# Lista wskaźników
uv run powietrze-wykres wskazniki
```

## Struktura bazy danych

- `stations` - stacje pomiarowe (kod stacji)
- `indicators` - wskaźniki (NO2, PM10, PM25, etc.) z jednostkami
- `measurements` - pomiary (stacja, wskaźnik, czas, wartość)
- `import_files` - status importu plików (do wznawiania)
