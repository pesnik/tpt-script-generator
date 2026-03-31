# tpt-builder

[![PyPI - Version](https://img.shields.io/pypi/v/tpt-builder.svg)](https://pypi.org/project/tpt-builder)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/tpt-builder.svg)](https://pypi.org/project/tpt-builder)

A Python package for building Teradata PT (TPT) scripts programmatically.

## Installation

```console
pip install tpt-builder
```

## Usage

### Basic Load Job

```python
from tpt_script_generator import TPTScript, LoadOperator, Step

# Create a TPT script
script = TPTScript("MyLoadJob")
script.with_description("Load data into target table")

# Add variables
script.with_var("SourceTable", "'source_db.my_table'")
script.with_var("TargetTable", "'target_db.my_table'")

# Add Load operator
load_op = LoadOperator()
load_op.with_tdp_id("TDPID")
load_op.with_user_name("username")
load_op.with_user_password("password")
load_op.with_target_table("TargetTable")
load_op.with_error_limit(25)
load_op.with_max_sessions(4)
script.with_operator(load_op)

# Add step
step = Step("LOAD_STEP")
step.with_operator("LOAD")
step.with_definition("LOAD OPERATOR (SOURCE_OPERATOR)\nAPPLY TO OPERATOR (TARGET_OPERATOR)\nSELECT * FROM OPERATOR (SOURCE_OPERATOR);")
script.with_step(step)

# Build and print
print(script.build())
```

### DDL Job

```python
from tpt_script_generator import TPTScript, DDLOperator

script = TPTScript("MyDDLJob")
ddl_op = DDLOperator()
ddl_op.with_tdp_id("TDPID")
ddl_op.with_user_name("username")
ddl_op.with_user_password("password")
ddl_op.with_sql_cmd_file_name("ddl_script.sql")
script.with_operator(ddl_op)

print(script.build())
```

### Character Set Support

```python
script = TPTScript("MyJob", charset="UTF8")
```

### Save to File

```python
script.save("output.tpt")
```

## Features

- **LoadOperator** - Build TPT LOAD scripts with full attribute support
- **DDLOperator** - Build TPT DDL scripts
- **UpdateOperator** - Build TPT UPDATE scripts
- **Schema Definitions** - Define schemas with column types
- **Variables** - Set TPT job variables
- **Steps** - Define job execution steps
- **Character Set** - Support for `USING CHARACTER SET` directive

## License

`tpt-builder` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
