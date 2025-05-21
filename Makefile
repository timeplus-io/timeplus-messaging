
timeplus:
	docker run -d --name timeplus -p 8463:8463 -p 8000:8000 timeplus/timeplus-enterprise

dev:
	uv pip install --editable .

test:
	uv run pytest