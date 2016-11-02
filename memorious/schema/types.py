import six
import fingerprints
import countrynames
from flanker.addresslib import address
import phonenumbers
from phonenumbers.phonenumberutil import NumberParseException


class StringProperty(object):
    index_invert = None

    def __init__(self, prop):
        self.prop = prop

    def clean(self, value, record):
        if value is None:
            return None
        value = six.text_type(value).strip()
        if not len(value):
            return None
        return value

    def normalize(self, values, record):
        results = []
        for value in values:
            value = self.normalize_value(value, record)
            if value is not None:
                results.append(value)
        return set(results)

    def normalize_value(self, value, record):
        return self.clean(value, record)


class NameProperty(StringProperty):
    index_invert = 'fingerprints'

    def normalize_value(self, value, record):
        return fingerprints.generate(value)


class DateProperty(StringProperty):
    index_invert = 'dates'

    def normalize_value(self, value, record):
        return value


class CountryProperty(StringProperty):
    index_invert = 'countries'

    def normalize_value(self, value, record):
        return countrynames.to_code(value)


class AddressProperty(StringProperty):
    index_invert = 'addresses'

    def normalize_value(self, value, record):
        return value


class PhoneProperty(StringProperty):
    index_invert = 'phones'
    FORMAT = phonenumbers.PhoneNumberFormat.INTERNATIONAL

    def get_countries(self, record):
        """Find the country references on this record."""
        countries = [None]
        for prop in self.prop.mapper.properties:
            if isinstance(prop.type, CountryProperty):
                countries.extend(prop.type.normalize(None, record))
        return countries

    def normalize_value(self, value, record):
        for country in self.get_countries(record):
            try:
                num = phonenumbers.parse(value, country)
                if phonenumbers.is_possible_number(num):
                    if phonenumbers.is_valid_number(num):
                        # TODO: allow multiple?
                        return phonenumbers.format_number(num, self.FORMAT)
            except NumberParseException:
                # TODO do we want to log this?
                pass


class EmailProperty(StringProperty):
    index_invert = 'emails'

    def normalize(self, value, record):
        for value in self.prop.get_values(record):
            parsed = address.parse(value)
            if parsed is not None:
                yield parsed.address


def resolve_type(name):
    """Look up a property type by name."""
    types = {
        'string': StringProperty,
        'name': NameProperty,
        'date': DateProperty,
        'country': CountryProperty,
        'address': AddressProperty,
        'phone': PhoneProperty,
        'email': EmailProperty
    }
    type_ = types.get(name.strip().lower())
    if type_ is None:
        raise TypeError("No such type: %s" % name)
    return type_
