import pandas as pd
from google.cloud import bigquery
import pandas_gbq

class SchemaManager:
    def __init__(self, bq_client: bigquery.Client):
        self.bq_client = bq_client

    def generate_schema(self, df: pd.DataFrame) -> List[bigquery.SchemaField]:
        schema = pandas_gbq.schema.generate_bq_schema(df)['fields']
        for field in schema:
            if field['name'] == 'partition_dt':
                field['type'] = 'DATE'
            elif field['name'] == 'dt':
                field['type'] = 'DATETIME'
        return [bigquery.SchemaField.from_api_repr(field) for field in schema]

    def get_current_schema(self, table_id: str) -> Optional[List[bigquery.SchemaField]]:
        try:
            table = self.bq_client.get_table(table_id)
            return table.schema
        except bigquery.NotFound:
            return None

    def compare_schemas(self, current_schema: Optional[List[bigquery.SchemaField]], new_schema: List[bigquery.SchemaField]) -> Tuple[bool, List[bigquery.SchemaField]]:
        if current_schema is None:
            return False, new_schema

        current_fields = {field.name: field for field in current_schema}
        new_fields = {field.name: field for field in new_schema}

        if set(current_fields.keys()) != set(new_fields.keys()):
            return True, new_schema

        for name, new_field in new_fields.items():
            current_field = current_fields[name]
            if current_field.field_type != new_field.field_type:
                return True, new_schema

        return False, current_schema

    def update_table_schema(self, table_id: str, new_schema: List[bigquery.SchemaField]):
        table = self.bq_client.get_table(table_id)
        table.schema = new_schema
        self.bq_client.update_table(table, ['schema'])