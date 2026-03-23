from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "replace-me-in-prod"
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    'django.contrib.humanize',
    # "django.contrib.gis",
    "popcase",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "popcase_project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "popcase_project.wsgi.application"

# DATABASES = {
#     "default": {
#         "ENGINE": "django.db.backends.sqlite3",
#         "NAME": BASE_DIR / "db.sqlite3",
#     }
# }
DATABASES = {
    "popcase_manual_etl": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "popcase_manual_etl",
        "USER": "popcase_admin",
        "PASSWORD": "D4=&HxM(Wt8A7]9Dz",
        "HOST": "cicbpopcase.cwru.edu",
        "PORT": "5432",
        # "OPTIONS": {
        #     "options": "-c search_path=popcaseui"
        # },
    },
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "popcase",
        "USER": "popcase_admin",
        "PASSWORD": "D4=&HxM(Wt8A7]9Dz",
        "HOST": "cicbpopcase.cwru.edu",
        "PORT": "5432",
        "OPTIONS": {
            "options": "-c search_path=public"
        },
    },
}

DATABASE_ROUTERS = ["popcase_project.db_router.PopcaseRouter"]

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATICFILES_DIRS = []
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "popcase:login"
LOGIN_REDIRECT_URL = "popcase:wizard"
LOGOUT_REDIRECT_URL = "popcase:login"

SESSION_ENGINE = "django.contrib.sessions.backends.signed_cookies"

