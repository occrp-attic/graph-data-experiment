from setuptools import setup, find_packages


setup(
    name='memorious',
    version='0.1',
    description="A data cross-referencing tool.",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ],
    keywords='elasticsearch graph etl data',
    author='Friedrich Lindenberg',
    author_email='pudo@occrp.org',
    url='http://github.com/occrp/memorious',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={},
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        'six'
    ],
    tests_require=[],
    entry_points={
        'console_scripts': [
            'memorious = memorious.manage:main',
        ]
    }
)
