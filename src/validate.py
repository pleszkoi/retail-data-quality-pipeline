from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd


LOGGER = logging.getLogger(__name__)

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# Segédfüggvény, melynek az a célja, hogy az összes validáció ugyanarra a formára alakítsa a találatokat.
def _build_issue_dataframe(
    dataframe: pd.DataFrame,
    invalid_mask: pd.Series,
    dataset_name: str,
    rule_name: str,
    column_name: str,
    message: str,
    severity: str = "ERROR",
) -> pd.DataFrame:
    """
    Build a standardized issue dataframe from invalid rows.
    """
    # .loc[invalid_mask]
    # A invalid_mask egy logikai sorozat, például: [False, True, False, True]
    # A .loc[invalid_mask] kiválasztja azokat a sorokat, ahol a maszk True.
    # Tehát ez lesz a hibás rekordok részhalmaza.

    # .copy()
    # Azért, hogy egy külön másolatot kapjunk, és ne az eredeti DataFrame nézetét módosítsuk.
    invalid_rows = dataframe.loc[invalid_mask].copy()

    # Ha nincs hibás sor, akkor egy üres DataFrame-et ad vissza.
    # A többi függvény így egységesen tud működni:
    #  - vagy van issue DataFrame
    #  - vagy üres DataFrame van
    # Nincs szükség None kezelésre.
    if invalid_rows.empty:
        return pd.DataFrame()

    # Metaadat oszlopokat ad hozzá az issue rekordokhoz.
    invalid_rows["dataset_name"] = dataset_name
    invalid_rows["rule_name"] = rule_name
    invalid_rows["column_name"] = column_name
    invalid_rows["severity"] = severity
    invalid_rows["message"] = message

    return invalid_rows

# Megnézi, hogy egy kötelező mező:
#  - null-e
#  - vagy üres string-e
def validate_required_column(
    dataframe: pd.DataFrame,
    dataset_name: str,
    column_name: str,
) -> pd.DataFrame:
    """
    Validate that a required column is not null or empty.
    """
    # A kiválasztott oszlopban megkeresi a hiányzó értékeket.
    # Az eredmény egy boolean sorozat, pl.:
    # 0    False
    # 1    False
    # 2     True
    # 3    False
    invalid_mask = dataframe[column_name].isna()

    # string oszlopoknál nem csak a null a probléma, hanem az üres string is.
    # A pandasban a szöveges oszlopok gyakran object típusúak.
    if dataframe[column_name].dtype == "object":
        # dataframe[column_name].astype(str)
        # Minden értéket stringgé alakít

        # .str.strip()
        # Leveszi az elejéről és végéről a szóközöket

        # invalid_mask | ...
        # ha valami null vagy üres string, akkor legyen hibás
        # így a " " típusú tartalmat is hibának veszi
        invalid_mask = invalid_mask | (dataframe[column_name].astype(str).str.strip() == "")

    issues = _build_issue_dataframe(
        dataframe=dataframe,
        invalid_mask=invalid_mask,
        dataset_name=dataset_name,
        rule_name="required_column",
        column_name=column_name,
        message=f"Column '{column_name}' is required and cannot be null or empty.",
        severity="ERROR",
    )

    LOGGER.info(
        "Required column validation finished for %s.%s - issues found: %s",
        dataset_name,
        column_name,
        len(issues),
    )
    return issues


def validate_unique_column(
    dataframe: pd.DataFrame,
    dataset_name: str,
    column_name: str,
) -> pd.DataFrame:
    """
    Validate that values in a column are unique.
    """
    # .duplicated(keep=False)
    # Megjelöli a duplikált értékeket.

    # keep=False, mert így a duplikátumpár összes tagját hibásnak jelöli
    invalid_mask = dataframe[column_name].duplicated(keep=False)

    issues = _build_issue_dataframe(
        dataframe=dataframe,
        invalid_mask=invalid_mask,
        dataset_name=dataset_name,
        rule_name="unique_column",
        column_name=column_name,
        message=f"Column '{column_name}' must contain unique values.",
        severity="ERROR",
    )

    LOGGER.info(
        "Unique column validation finished for %s.%s - issues found: %s",
        dataset_name,
        column_name,
        len(issues),
    )
    return issues


def validate_date_column(
    dataframe: pd.DataFrame,
    dataset_name: str,
    column_name: str,
) -> pd.DataFrame:
    """
    Validate that values in a date column can be parsed as dates.
    """
    # Megpróbálja dátummá alakítani az oszlop értékeit
    # errors="coerce"
    # Ha nem lehet értelmezni az értéket, akkor ne dobjon hibát, hanem tegye NaT-tá.
    parsed_dates = pd.to_datetime(dataframe[column_name], errors="coerce")

    # Olyan sor legyen hibás, ahol:
    # az eredeti mező nem hiányzik
    # de a parse-olt dátum hiányzó lett
    # Vagyis volt valami beírva, de az nem értelmezhető dátumként.
    invalid_mask = dataframe[column_name].notna() & parsed_dates.isna()

    issues = _build_issue_dataframe(
        dataframe=dataframe,
        invalid_mask=invalid_mask,
        dataset_name=dataset_name,
        rule_name="date_format",
        column_name=column_name,
        message=f"Column '{column_name}' contains invalid date values.",
        severity="ERROR",
    )

    LOGGER.info(
        "Date validation finished for %s.%s - issues found: %s",
        dataset_name,
        column_name,
        len(issues),
    )
    return issues

# Megnézi, hogy az adott oszlop szám-e és legalább egy megadott minimumot elér-e
def validate_numeric_min_value(
    dataframe: pd.DataFrame,
    dataset_name: str,
    column_name: str,
    min_value: float,
) -> pd.DataFrame:
    """
    Validate that a numeric column is greater than or equal to min_value.
    Non-numeric values are also treated as invalid.
    """
    # Megpróbálja számmá alakítani az oszlopot.
    # errors="coerce"
    # Ami nem konvertálható számmá, az NaN lesz.
    numeric_series = pd.to_numeric(dataframe[column_name], errors="coerce")
    
    # Hibás az a sor, ahol:
    #  - nem szám
    #  - vagy kisebb a minimumnál
    invalid_mask = numeric_series.isna() | (numeric_series < min_value)

    issues = _build_issue_dataframe(
        dataframe=dataframe,
        invalid_mask=invalid_mask,
        dataset_name=dataset_name,
        rule_name="numeric_min_value",
        column_name=column_name,
        message=f"Column '{column_name}' must be numeric and >= {min_value}.",
        severity="ERROR",
    )

    LOGGER.info(
        "Numeric minimum validation finished for %s.%s - issues found: %s",
        dataset_name,
        column_name,
        len(issues),
    )
    return issues

# Az email formátum ellenőrzése.
def validate_email_column(
    dataframe: pd.DataFrame,
    dataset_name: str,
    column_name: str,
) -> pd.DataFrame:
    """
    Validate email format for non-empty values.
    Missing emails are not handled here; they should be checked by required-column validation if needed.
    """
    # fillna("")
    # A hiányzó értékeket üres stringgé alakítja

    # astype(str)
    # Stringgé alakít minden értéket

    # .str.strip()
    # Eltávolítja a szóközöket
    email_series = dataframe[column_name].fillna("").astype(str).str.strip()

    invalid_mask = (email_series != "") & (~email_series.str.match(EMAIL_PATTERN))

    issues = _build_issue_dataframe(
        dataframe=dataframe,
        invalid_mask=invalid_mask,
        dataset_name=dataset_name,
        rule_name="email_format",
        column_name=column_name,
        message=f"Column '{column_name}' contains invalid email format.",
        severity="WARNING",
    )

    LOGGER.info(
        "Email validation finished for %s.%s - issues found: %s",
        dataset_name,
        column_name,
        len(issues),
    )
    return issues

# Kapcsolatot ellenőriz két tábla között.
def validate_foreign_key(
    dataframe: pd.DataFrame,
    reference_dataframe: pd.DataFrame,
    dataset_name: str,
    column_name: str,
    reference_column: str,
    reference_dataset_name: str,
) -> pd.DataFrame:
    """
    Validate that values in column_name exist in the reference dataframe.
    """
    # reference_dataframe[reference_column]
    # Kiválasztja a referenciaoszlopot.

    # .dropna(): Kiveszi a hiányzó értékeket.

    # .tolist(): Listává alakítja.

    # set(...)
    # Halmazt készít belőle.
    # Azért set mert a set-ben a membership ellenőrzés hatékony.
    # És a duplikátumok sem számítanak.
    valid_reference_values = set(reference_dataframe[reference_column].dropna().tolist())
    
    # .isin(valid_reference_values)
    # Megnézi, hogy az adott oszlop értéke benne van-e a referenciahalmazban.

    # ~ Negálja az eredményt.
    # Tehát hibás az, ami nincs benne.
    invalid_mask = ~dataframe[column_name].isin(valid_reference_values)

    issues = _build_issue_dataframe(
        dataframe=dataframe,
        invalid_mask=invalid_mask,
        dataset_name=dataset_name,
        rule_name="foreign_key",
        column_name=column_name,
        message=(
            f"Column '{column_name}' contains values not found in "
            f"{reference_dataset_name}.{reference_column}."
        ),
        severity="ERROR",
    )

    LOGGER.info(
        "Foreign key validation finished for %s.%s -> %s.%s - issues found: %s",
        dataset_name,
        column_name,
        reference_dataset_name,
        reference_column,
        len(issues),
    )
    return issues

# Több külön issue DataFrame-et összefűz egy közös DataFrame-be.
def combine_issue_dataframes(issue_dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine multiple issue dataframes into one dataframe while preserving original row indices.
    """
    # Kiszűri az üres DataFrame-eket.
    non_empty_issues = [df for df in issue_dataframes if not df.empty]

    # Ha egyik validáció sem talált hibát, akkor üres DataFrame-et ad vissza.
    if not non_empty_issues:
        return pd.DataFrame()

    # Egymás alá fűzi a DataFrame-eket.
    # itt most megőrizzük az eredeti indexeket.
    # Ez kell ahhoz, hogy a pipeline később vissza tudja kötni a hibákat az eredeti rekordokra.
    combined = pd.concat(non_empty_issues)
    LOGGER.info("Combined validation issues count: %s", len(combined))
    return combined

# Ez az orchestration függvény a validációs modulon belül.
# Kap egy dictionary-t a datasetekkel, és visszaadja az eredményeket.
def run_validations(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Run all initial validations for the current project datasets.
    Returns a dictionary with issue dataframes per dataset and a combined dataframe.
    """
    # Kiemeljük a három fő datasetet a bemeneti dictionary-ből.
    # Ezután könnyebb rájuk hivatkozni.
    customers_df = datasets["customers"]
    products_df = datasets["products"]
    orders_df = datasets["orders"]


    customer_issues = combine_issue_dataframes(
        [
            validate_required_column(customers_df, "customers", "customer_id"),
            validate_required_column(customers_df, "customers", "full_name"),
            validate_required_column(customers_df, "customers", "country"),
            validate_unique_column(customers_df, "customers", "customer_id"),
            validate_date_column(customers_df, "customers", "registration_date"),
            validate_email_column(customers_df, "customers", "email"),
        ]
    )

    product_issues = combine_issue_dataframes(
        [
            validate_required_column(products_df, "products", "product_id"),
            validate_required_column(products_df, "products", "product_name"),
            validate_unique_column(products_df, "products", "product_id"),
            validate_numeric_min_value(products_df, "products", "price", 0),
            validate_required_column(products_df, "products", "category"),
        ]
    )

    order_issues = combine_issue_dataframes(
        [
            validate_required_column(orders_df, "orders", "order_id"),
            validate_required_column(orders_df, "orders", "customer_id"),
            validate_required_column(orders_df, "orders", "product_id"),
            validate_unique_column(orders_df, "orders", "order_id"),
            validate_numeric_min_value(orders_df, "orders", "quantity", 1),
            validate_numeric_min_value(orders_df, "orders", "total_amount", 0),
            validate_date_column(orders_df, "orders", "order_date"),
            validate_foreign_key(
                orders_df,
                customers_df,
                "orders",
                "customer_id",
                "customer_id",
                "customers",
            ),
            validate_foreign_key(
                orders_df,
                products_df,
                "orders",
                "product_id",
                "product_id",
                "products",
            ),
        ]
    )

    # összesített issue lista az összes datasetből.
    #  - lehet belőle globális riport
    #  - lehet összes issue számot számolni
    #  - lehet dashboard alap
    all_issues = combine_issue_dataframes([customer_issues, product_issues, order_issues])

    return {
        "customers": customer_issues,
        "products": product_issues,
        "orders": order_issues,
        "all_issues": all_issues,
    }
