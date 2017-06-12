#!/bin/bash

case "$1" in
    python)
        shift 1; /usr/bin/python $@;
        ;;
    celery_beat)
        cd /app; celery -A re_processor beat -l info > /data/supervisor/celery_beat.log 2>&1;
        ;;
    celery_worker)
        cd /app; celery -A re_processor worker -l info > /data/supervisor/celery_worker.log 2>&1;
        ;;
    *)
        /usr/bin/supervisord -n
        ;;
esac

exit 0
