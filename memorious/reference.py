import pycountry

COUNTRY_NAMES = {
    'ZZ': 'Global',
    'EU': 'European Union',
    'XK': 'Kosovo',
    'YU': 'Yugoslavia'
}

for country in pycountry.countries:
    if hasattr(country, 'common_name'):
        COUNTRY_NAMES[country.alpha_2] = country.common_name
    else:
        COUNTRY_NAMES[country.alpha_2] = country.name
