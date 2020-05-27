import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pymachinery",  # Replace with your own username
    version="0.0.6",
    author="Nathaniel M. Capule",
    author_email="nmcapule@gmail.com",
    description="Python workers for RichardKnop/machinery",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nmcapule/pymachinery",
    packages=setuptools.find_packages(),
    install_requires=["pymongo", "redis"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
