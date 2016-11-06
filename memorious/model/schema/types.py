import re
import dateparser
import fingerprints
import countrynames
from datetime import datetime
from flanker.addresslib import address
import phonenumbers
from phonenumbers.phonenumberutil import NumberParseException

from memorious.util import clean_text


class StringProperty(object):
    index_invert = None

    def __init__(self, prop):
        self.prop = prop

    def clean(self, value, record):
        return clean_text(value)

    def normalize(self, values, record):
        results = []
        for value in values:
            for value in self.normalize_value(value, record):
                if value is not None:
                    results.append(value)
        return set(results)

    def normalize_value(self, value, record):
        return [self.clean(value, record)]


class NameProperty(StringProperty):
    index_invert = 'fingerprints'

    def normalize_value(self, value, record):
        return [fingerprints.generate(value)]


class URLProperty(StringProperty):
    index_invert = None


class DateProperty(StringProperty):
    index_invert = 'dates'

    def normalize_value(self, value, record):
        date_format = self.prop.data.get('format')
        if date_format is not None:
            try:
                date = datetime.strptime(value, date_format)
                return [date.date().isoformat()]
            except:
                return []
        else:
            date = dateparser.parse(value)
            if date is not None:
                return [date.date().isoformat()]
        return []


class CountryProperty(StringProperty):
    index_invert = 'countries'

    def normalize_value(self, value, record):
        return [countrynames.to_code(value)]


class AddressProperty(StringProperty):
    index_invert = 'addresses'

    def normalize_value(self, value, record):
        return [value]


class PhoneProperty(StringProperty):
    index_invert = 'phones'
    FORMAT = phonenumbers.PhoneNumberFormat.E164

    def get_countries(self, record):
        """Find the country references on this record."""
        countries = [self.prop.data.get('country')]
        if countries[0] is None:
            for prop in self.prop.mapper.properties:
                if isinstance(prop.type, CountryProperty):
                    countries.extend(prop.type.normalize(None, record))
        return countries

    def normalize(self, values, record):
        countries = [self.prop.data.get('country')]
        if countries[0] is None:
            for prop in self.prop.mapper.properties:
                if isinstance(prop.type, CountryProperty):
                    pvalues = prop.get_values(record)
                    countries.extend(prop.type.normalize(pvalues, record))

        for value in values:
            for country in countries:
                try:
                    num = phonenumbers.parse(value, country)
                    if phonenumbers.is_possible_number(num):
                        if phonenumbers.is_valid_number(num):
                            num = phonenumbers.format_number(num, self.FORMAT)
                            yield num
                except NumberParseException:
                    # TODO do we want to log this?
                    pass


class EmailProperty(StringProperty):
    index_invert = 'emails'

    def normalize_value(self, value, record):
        parsed = address.parse(value)
        if parsed is not None:
            return [parsed.address]


class IdentiferProperty(StringProperty):
    index_invert = 'fingerprints'
    clean_re = re.compile('[^a-zA-Z0-9]*')

    def normalize_value(self, value, record):
        scheme = self.prop.data.get('scheme')
        if scheme is None:
            raise TypeError("No scheme given for: %s", self.prop)
        value = self.clean_re.sub('', value).upper()
        if not len(value):
            return []
        return ['%s:%s' % (scheme, value)]


def resolve_type(name):
    """Look up a property type by name."""
    types = {
        'string': StringProperty,
        'name': NameProperty,
        'date': DateProperty,
        'country': CountryProperty,
        'address': AddressProperty,
        'phone': PhoneProperty,
        'email': EmailProperty,
        'url': URLProperty,
        'uri': URLProperty,
        'identifier': IdentiferProperty
    }
    type_ = types.get(name.strip().lower())
    if type_ is None:
        raise TypeError("No such type: %s" % name)
    return type_
