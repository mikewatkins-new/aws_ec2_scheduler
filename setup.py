import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="aws_automated_scheduler",
    version="0.0.1",

    description="CDK Python APP for automated scheduling of instances",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="Mike Watkins",

    package_dir={"": "aws_automated_scheduler"},
    packages=setuptools.find_packages(where="aws_automated_scheduler"),

    install_requires=[
        "aws-cdk.core",
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
