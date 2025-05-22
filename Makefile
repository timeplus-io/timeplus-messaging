
timeplus:
	docker run -d --name timeplus -p 8463:8463 -p 8000:8000 timeplus/timeplus-enterprise

dev:
	uv pip install --editable .

test:
	uv run pytest

build:
	uv build

publish:
	uv publish

lint:
	flake8 ./src --count --select=E9,F63,F7,F82 --ignore=E501,W293 --show-source --statistics
	flake8 ./src --count --exit-zero --max-complexity=10 --ignore=E501,W503,W293 --max-line-length=127 --statistics

format:
	black ./src