import six
import fingerprints
import countrynames
from flanker.addresslib import address
import phonenumbers
from phonenumbers.phonenumberutil import NumberParseException


class StringProperty(object):

    def __init__(self, prop):
        self.prop = prop

    def clean(self, value, record):
        value = six.text_type(value).strip()
        return value

    def normalize(self, value, record):
        return [self.clean(value)]


class NameProperty(StringProperty):

    def normalize(self, value, record):
        for value in self.prop.get_values(record):
            fp = fingerprints.generate(value)
            if fp is not None:
                yield fp


class CountryProperty(StringProperty):

    def normalize(self, value, record):
        for value in self.prop.get_values(record):
            cc = countrynames.to_code(value)
            if cc is not None:
                yield cc


class AddressProperty(StringProperty):

    def normalize(self, value, record):
        for value in self.prop.get_values(record):
            # TODO use libpostal here
            if value is not None:
                yield value


class PhoneProperty(StringProperty):
    FORMAT = phonenumbers.PhoneNumberFormat.INTERNATIONAL

    def normalize(self, value, record):
        countries = [None]
        for prop in self.prop.mapper.properties:
            if isinstance(prop.type, CountryProperty):
                countries.extend(prop.type.normalize(None, record))

        for value in self.prop.get_values(record):
            for country in countries:
                try:
                    num = phonenumbers.parse(value, country)
                    if phonenumbers.is_possible_number(num):
                        if phonenumbers.is_valid_number(num):
                            yield phonenumbers.format_number(num, self.FORMAT)
                except NumberParseException:
                    # TODO do we want to log this?
                    pass


class EmailProperty(StringProperty):

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
        'country': CountryProperty,
        'address': AddressProperty,
        'phone': PhoneProperty,
        'email': EmailProperty
    }
    type_ = types.get(name.strip().lower())
    if type_ is None:
        raise TypeError("No such type: %s" % name)
    return type_
