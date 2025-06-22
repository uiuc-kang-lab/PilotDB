cd /home
export FLASK_RUN_PORT=8080
export FLASK_RUN_HOST=0.0.0.0
$HOME/.local/bin/uv run flask --app pilotdb_server run --debug
