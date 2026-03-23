class PopcaseRouter:
    def db_for_read(self, model, **hints):
        if model._meta.db_table == "cdc_places_tract_data_2024":
            return "popcase_manual_etl"
        return "default"

    def db_for_write(self, model, **hints):
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return False
