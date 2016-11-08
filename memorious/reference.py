from pycountry import countries

COUNTRY_NAMES = {
    'ZZ': 'Global',
    'EU': 'European Union',
    'XK': 'Kosovo',
    'YU': 'Yugoslavia'
}

for country in countries:
    COUNTRY_NAMES[country.alpha2] = country.name
