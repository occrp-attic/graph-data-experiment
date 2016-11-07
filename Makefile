

worker:
	celery -A memorious.queue -B -c 6 -l INFO worker

