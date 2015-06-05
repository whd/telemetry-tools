from distutils.core import setup

setup(name='telemetry-tools',
      description='Utility code to work with Mozilla Telemetry data.',
      version='1.0.5',
      author='Mozilla',
      url='https://github.com/mozilla/telemetry-tools',
      packages=['telemetry', 'telemetry.util'],
      license='Mozilla Public License v2.0',
      platforms = "Posix; MacOS X",
      classifiers = ["Development Status :: 4 - Beta",
                     "Intended Audience :: Developers",
                     "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
                     "Operating System :: OS Independent",
                     "Topic :: Internet",
                     "Programming Language :: Python :: 2.7"],
      )
