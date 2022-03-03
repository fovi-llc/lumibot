from decimal import Decimal
import warnings

def check_numeric(
    input, type, error_message, positive=True, strict=False, nullable=False, ratio=False
):
    if nullable and input is None:
        return None

    error = ValueError(error_message)

    if isinstance(input, str) or (type == Decimal and not isinstance(input, Decimal)):
        try:
            input = type(input)
        except:
            raise error

    if positive:
        if strict:
            if input <= 0:
                raise error
        else:
            if input < 0:
                raise error

    if ratio:
        if input >= 0:
            if input > 1:
                raise error
        else:
            if input < -1:
                raise error

    return input


def check_positive(input, type, custom_message="", strict=False):
    if strict:
        error_message = "%r is not a strictly positive value." % input
    else:
        error_message = "%r is not a positive value." % input
    if custom_message:
        error_message = f"{error_message} {custom_message}"

    result = check_numeric(
        input,
        type,
        error_message,
        strict=strict,
    )
    return result

def check_quantity(quantity, custom_message=""):
    error_message = "%r is not a positive Decimal." % quantity
    if custom_message:
        error_message = f"{error_message} {custom_message}"
    if isinstance(quantity, float):
        warnings.simplefilter('always', DeprecationWarning)
        warnings.warn(
            f"The order quantity of {quantity} must be an integer, "
            f"string (eg '3.21'), \nor Decimal (eg: Decimal('3.21')), "
            f"not a float. Float will be deprecated in future versions.",
            DeprecationWarning,
        )
        warnings.simplefilter('ignore', DeprecationWarning)
    quantity = Decimal(quantity)
    result = check_numeric(
        quantity,
        Decimal,
        error_message,
        strict=True,
    )
    return result


def check_price(price, custom_message="", nullable=True):
    error_message = "%r is not a valid price." % price
    if custom_message:
        error_message = f"{error_message} {custom_message}"

    result = check_numeric(price, float, error_message, strict=True, nullable=nullable)
    return result
