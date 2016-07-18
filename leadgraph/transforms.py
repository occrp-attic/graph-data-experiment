import six
import fingerprints
import phonenumbers
from flanker.addresslib import address


def fingerprint(value, **kwargs):
    return fingerprints.generate(value)


def trim(value, **kwargs):
    if isinstance(value, six.string_types):
        return value.strip()
    return value


def lowercase(value, **kwargs):
    if isinstance(value, six.string_types):
        return value.lower()
    return value


def addressfp(value, **kwargs):
    return fingerprint(value, **kwargs)


def email(value, **kwargs):
    parsed = address.parse(value)
    if parsed is None:
        return None
    return parsed.address


def phone(value, prop=None, **kwargs):
    try:
        num = phonenumbers.parse(value, prop.country)
        if phonenumbers.is_possible_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.INTERNATIONAL)  # noqa
    except Exception:
        return


TRANSFORMS = {
    'fingerprint': fingerprint,
    'addressfp': addressfp,
    'lowercase': lowercase,
    'email': email,
    'phone': phone,
    'trim': trim
}
