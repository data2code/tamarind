from setuptools import setup, find_packages
import glob

setup(
    name='tamarind',
    version='0.1.1',
    description='A Python wrapper for the Tamarind.bio API',
    url='https://github.com/data2code/tamarind',
    author='Yingyao Zhou',
    author_email='yingyao.zhou@novartis.com',
    license= 'MIT',
    python_requires=">=3.7",
    zip_safe=False,
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    scripts=glob.glob("bin/tmr*"), # this include all tmr command line tools
    install_requires=['pandas',
                      'tqdm',
                      'requests',
                      ],

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.7',
    ],
)
