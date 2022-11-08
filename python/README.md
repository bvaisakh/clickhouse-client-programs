# Classic Computer Science Problems by David Kopec

## Creating virtual environments

- `python3 -m venv --system-site-packages ./venv`
- `source ./venv/bin/activate`
- `python -m pip install -U pip`
- `python -m pip install -r requirements.txt`

All pip install should be from / recorded in requirements.txt

## Development

- Activating environment  
  `source ./venv/bin/activate`
- Formatting the source files  
  `autopep8 --in-place --aggressive <filename>`
