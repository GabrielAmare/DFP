from install37 import setup
from dfp37.__meta__ import __version__

if __name__ == "__main__":
    setup(
        name="dfp37",
        version=__version__,
        author="Gabriel Amare", 
        author_email="gabriel.amare.31@gmail.com",
        description="Database Framework for Python", 
        url="https://github.com/GabrielAmare/DFP", 
        packages=["dfp37"], 
        classifiers=[], 
        python_requires=">=3.7"
    )
