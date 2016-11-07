

worker:
	celery -A memorious.queue -B -c 3 -l INFO worker

