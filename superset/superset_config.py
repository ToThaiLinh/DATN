DEV_MODE= "false"
SUPERSET_LOAD_EXAMPLES= "no"

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

ENABLE_PROXY_FIX = True
SECRET_KEY = "tothailinh2003"

SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://postgres:postgres@postgres:5432/superset"

SUPERSET_WEBSERVER_PORT = 8088