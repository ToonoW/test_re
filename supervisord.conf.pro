; supervisor config file

[unix_http_server]
file=/data/supervisor/supervisor.sock   ; (the path to the socket file)
chmod=0700                       ; sockef file mode (default 0700)

[supervisord]
logfile=/data/supervisor/supervisord.log ; (main log file;default $CWD/supervisord.log)
pidfile=/data/supervisor/supervisord.pid ; (supervisord pidfile;default supervisord.pid)
childlogdir=/data/supervisor            ; ('AUTO' child log dir, default $TEMP)

; the below section must remain in the config file for RPC
; (supervisorctl/web interface) to work, additional interfaces may be
; added by defining them in separate rpcinterface: sections
[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix:///data/supervisor/supervisor.sock ; use a unix:// URL  for a unix socket

; The [include] section can just contain the "files" setting.  This
; setting can list multiple files (separated by whitespace or
; newlines).  It can also contain wildcards.  The filenames are
; interpreted as relative to this file.  Included files *cannot*
; include files themselves.

[program:core_all]
command=/env-gw_re_processor/bin/python start.py
stopsignal=INT

[program:core_data]
command=/env-gw_re_processor/bin/python start.py --queue=data
stopsignal=INT

[program:core_tmp]
command=/env-gw_re_processor/bin/python start.py --only-tmp-consumer
stopsignal=INT

[program:core_gdmshttp]
command=/env-gw_re_processor/bin/python start.py --only-gdmshttp-consumer
stopsignal=INT
