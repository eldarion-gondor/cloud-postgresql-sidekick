FROM python:3.5-onbuild
CMD ["gunicorn", "--threads=14", "sidekick:app"]
