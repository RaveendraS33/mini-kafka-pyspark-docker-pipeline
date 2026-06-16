from src.quality.rules import (
    ERROR_INVALID_AMOUNT,
    ERROR_INVALID_EMAIL,
    ERROR_MISSING_EVENT_TIME,
    ERROR_MISSING_TRANSACTION_ID,
    ERROR_MISSING_USER_ID,
    VALID_REASON,
    validate_record,
)


def valid_record():
    return {
        "transaction_id": "TXN0001",
        "user_id": 1234,
        "email": "student@example.com",
        "amount": 25.50,
        "event_time": "2026-06-16T00:00:00+00:00",
    }


def test_valid_record_passes():
    assert validate_record(valid_record()) == VALID_REASON


def test_missing_transaction_id_fails():
    record = valid_record()
    record["transaction_id"] = None
    assert validate_record(record) == ERROR_MISSING_TRANSACTION_ID


def test_missing_user_id_fails():
    record = valid_record()
    record["user_id"] = None
    assert validate_record(record) == ERROR_MISSING_USER_ID


def test_invalid_email_fails():
    record = valid_record()
    record["email"] = "invalid_email"
    assert validate_record(record) == ERROR_INVALID_EMAIL


def test_negative_amount_fails():
    record = valid_record()
    record["amount"] = -10
    assert validate_record(record) == ERROR_INVALID_AMOUNT


def test_missing_event_time_fails():
    record = valid_record()
    record["event_time"] = None
    assert validate_record(record) == ERROR_MISSING_EVENT_TIME
