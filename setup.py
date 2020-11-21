import setuptools

#with open("README.md", "r") as fh:
#    long_description = fh.read()

setuptools.setup(
    name="rpc_audit_os",
    version="0.0.1",
    author="Jakob Mueller",
    author_email="jakob.mueller@agdsn.de",
    description="A small example package",
    long_description='',
    long_description_content_type="text/markdown",
    url="https://publicgitlab.cloudandheat.com/cloud-kritis/rpc-audit",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
