from tpt_script_generator.step import Step

class TPTScript:
    def __init__(self, job_name, charset=None):
        self.charset = charset
        self.job_name = job_name
        self.description = ""
        self.variables = {}
        self.steps = []
        self.schemas = {}
        self.operators = []

    def with_description(self, description):
        self.description = description
        return self

    def with_var(self, key, val):
        self.variables[key] = val
        return self

    def with_schema(self, schema_name, schema_definition):
        self.schemas[schema_name] = schema_definition
        return self

    def with_operator(self, operator):
        self.operators.append(operator)
        return self

    def with_step(self, step: Step):
        self.steps.append(step)
        return self

    def build(self):
        script = f"DEFINE JOB {self.job_name}\n" if self.charset is None else f"USING CHARACTER SET {self.charset}\nDEFINE JOB {self.job_name}\n"
        script += f"DESCRIPTION '{self.description}'\n(\n"
        
        for var_name, var_value in self.variables.items():
            script += f"    SET {var_name} = {var_value}\n"
        
        for schema_name, schema_definition in self.schemas.items():
            script += f"    DEFINE SCHEMA {schema_name}\n    (\n"
            for column_name, column_type in schema_definition.items():
                script += f"""        "{column_name}" {column_type},\n"""
            script = script.rstrip(',\n')  # Remove the trailing comma
            script += "\n    );\n\n"
        
        for operator in self.operators:
            script += operator.build() + "\n"
        
        for step in self.steps:
            script += step.build() + "\n"
        
        script += ");\n"
        return script

    def save(self, filename):
        with open(filename, 'w') as file:
            file.write(self.build())
