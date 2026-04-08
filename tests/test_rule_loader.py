from src.rule_loader import load_quality_rules


def test_load_quality_rules_returns_dictionary() -> None:
    rules = load_quality_rules()

    assert isinstance(rules, dict)
    assert "customers" in rules
    assert "products" in rules
    assert "orders" in rules


def test_load_quality_rules_contains_expected_sections() -> None:
    rules = load_quality_rules()

    assert "required_columns" in rules["customers"]
    assert "unique_columns" in rules["customers"]
    assert "date_columns" in rules["customers"]
    assert "email_columns" in rules["customers"]

    assert "numeric_rules" in rules["products"]
    assert "foreign_keys" in rules["orders"]
