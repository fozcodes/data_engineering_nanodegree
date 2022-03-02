from setuptools import setup


def parse_requirements(filename):
    with open(filename) as f:
        lineiter = (line.strip() for line in f)
        return [
            line.replace(" \\", "").strip()
            for line in lineiter
            if (
                line
                and not line.startswith("#")
                and not line.startswith("-e")
                and not line.startswith("--")
            )
        ]


INSTALL_REQUIREMENTS = parse_requirements("requirements.txt")


setup(
    name="sparkify",
    version="0.0.0",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.9",
    ],
    packages=["sparkify"],
    package_dir={"sparkify": "."},
    include_package_data=True,
    install_requires=INSTALL_REQUIREMENTS,
    zip_safe=False,
    extras_require={
        "develop": parse_requirements("requirements.txt"),
    },
)
