from setuptools import find_packages, setup

setup(
    name="pycoffeemaker", 
    version="0.0.1", 
    author="Soumit Salman Rahman",
    description="Backend collector, indexer and storage utility package for Project Cafecit.io ",    
    long_description="Some more text or copy-paste from your README.md.",
    long_description_content_type='text/markdown',
    keywords="keyword1 keyword2 keyword3", # 'openai tiktoken chatgpt chatbot',
    url='<github url or your site or your twitter handle. whatevs>', # i usually use the github url of the project repository
    author_email="The email you gave yourself",
    license="MIT", # or some other license
    packages=find_packages(),
    install_requires=[ # these are the pips that need to exist for your pip to function
        'numpy',
        'pandas',
        'icecream',
        'transformers',
        'scipy'
    ], 
    zip_safe=False    
)