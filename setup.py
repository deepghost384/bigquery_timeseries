from setuptools import setup, find_packages

setup(
    name='bigquery_timeseries',
    version='0.1.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='A custom library for working with BigQuery timeseries data.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/bigquery_timeseries',
    packages=find_packages(),
    python_requires='>=3.10',
    install_requires=[
        'pandas>=1.5.2',
        'gcsfs>=2022.11.0',
        'pyarrow>=10.0.1',
        'google-cloud-bigquery>=3.4.1',
        'db-dtypes>=1.0.5',
        'pandas-gbq>=0.19.1',
    ],
    extras_require={
        'dev': [
            'mypy>=0.991',
            'flake8>=6.0.0',
            'black>=22.12.0',
            'pytest>=7.2.0',
            'pytest-order>=1.0.1',
            'pytest-cov>=4.0.0',
            'pytest-memray>=1.4.0',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    include_package_data=True,
)
