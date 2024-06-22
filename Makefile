install:
	pip install -r requirements.txt

test:
	pytest --cov --cov-report=xml

clean:
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -f coverage.xml