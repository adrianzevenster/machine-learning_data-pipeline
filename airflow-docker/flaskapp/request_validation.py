def parse_positive_int(payload, key, default, maximum):
    raw_value = payload.get(key, default)
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be an integer.")

    if value <= 0:
        raise ValueError(f"{key} must be greater than 0.")
    if value > maximum:
        raise ValueError(f"{key} must be less than or equal to {maximum}.")
    return value
