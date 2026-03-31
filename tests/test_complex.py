import unittest
import os
import tempfile

from tpt_script_generator import TPTScript, LoadOperator, DDLOperator, Step
from tpt_script_generator.update_operator import UpdateOperator
from tpt_script_generator.enums.load_operator import (
    BufferSizeUnit,
    CheckpointRowCount,
    DataEncryption,
    DateForm,
    DropErrorTable,
    DropLogTable,
    LogSQL,
    NotifyExitIsDLL,
    NotifyLevel,
    NotifyMethod,
    PauseAcq,
    TASMFASTFAIL,
    UnicodePassThrough,
    WildcardInsert,
)
from tpt_script_generator.enums.ddl_operator import (
    DataEncryptionOption,
    LogSQLOption,
    TraceLevelOption,
)
from tpt_script_generator.enums.update_operator import (
    LogonMech,
    LogSQL as UpdateLogSQL,
    NotifyMethod as UpdateNotifyMethod,
    NotifyLevel as UpdateNotifyLevel,
    TraceLevel,
    TransformGroup,
    ReplicationOverride,
)


class TestMultiOperatorETLPipeline(unittest.TestCase):
    """Test complex ETL pipeline with multiple operators."""

    def setUp(self):
        self.maxDiff = None

    def test_full_etl_pipeline_with_all_operators(self):
        """Test building a complete ETL pipeline with DDL, Load, and Update operators."""
        script = (
            TPTScript("FullETLPipeline")
            .with_description("Complete ETL pipeline: DDL -> Load -> Update")
            .with_var("TdpId", "@etl_tdpid")
            .with_var("UserName", "@etl_username")
            .with_var("UserPassword", "@etl_password")
            .with_var("SourceTable", "'staging.raw_data'")
            .with_var("TargetTable", "'production.processed_data'")
            .with_var("ErrorLimit", "500")
            .with_var("MaxSessions", "8")
            .with_schema(
                "StagingSchema",
                {
                    "ID": "INTEGER",
                    "NAME": "VARCHAR(255)",
                    "CREATED_DATE": "DATE",
                    "AMOUNT": "DECIMAL(18,2)",
                    "STATUS": "VARCHAR(50)",
                },
            )
            .with_schema(
                "TargetSchema",
                {
                    "ID": "INTEGER",
                    "NAME": "VARCHAR(255)",
                    "CREATED_DATE": "DATE",
                    "PROCESSED_DATE": "TIMESTAMP",
                    "AMOUNT": "DECIMAL(18,2)",
                    "STATUS": "VARCHAR(50)",
                    "BATCH_ID": "BIGINT",
                },
            )
            .with_operator(
                DDLOperator()
                .with_tdp_id("@TdpId")
                .with_username("@UserName")
                .with_user_password("@UserPassword")
                .with_connect_string("@ConnectString")
                .with_error_list("'3807','5589'")
                .with_log_sql(LogSQLOption.YES)
                .with_trace_level(TraceLevelOption.ALL)
            )
            .with_operator(
                LoadOperator()
                .with_tdp_id("@TdpId")
                .with_user_name("@UserName")
                .with_user_password("@UserPassword")
                .with_target_table("@TargetTable")
                .with_buffer_size(2048, BufferSizeUnit.KB)
                .with_checkpoint_row_count(CheckpointRowCount.YES)
                .with_data_encryption(DataEncryption.ON)
                .with_date_form(DateForm.ANSI_DATE)
                .with_drop_error_table(DropErrorTable.YES)
                .with_drop_log_table(DropLogTable.YES)
                .with_error_limit(500)
                .with_max_sessions(8)
                .with_min_sessions(2)
                .with_tasm_fast_fail(TASMFASTFAIL.YES)
                .with_tenacity_hours(2)
                .with_tenacity_sleep(30)
                .with_unicode_pass_through(UnicodePassThrough.ON)
                .with_wildcard_insert(WildcardInsert.YES)
            )
            .with_operator(
                UpdateOperator()
                .with_schema("TargetSchema")
                .with_tdp_id("@TdpId")
                .with_user_name("@UserName")
                .with_user_password("@UserPassword")
                .with_target_table("@TargetTable")
                .with_buffer_size(4096)
                .with_error_limit(250)
                .with_max_sessions(4)
                .with_min_sessions(1)
                .with_logon_mech(LogonMech.TERADATA)
                .with_log_sql(UpdateLogSQL.DETAIL)
                .with_pack(50)
                .with_trace_level([TraceLevel.CLI, TraceLevel.PX])
                .with_transform_group([TransformGroup.JSON, TransformGroup.ST_GEOMETRY])
                .with_replication_override(ReplicationOverride.ON)
                .with_tasmfastfail("YES")
                .with_treat_dbs_restart_as_fatal("YES")
            )
            .with_step(
                Step("DDL_Create_Staging")
                .add_operation("APPLY")
                .add_operation(
                    "('CREATE TABLE ' || @TargetTable || ' AS ' || @SourceTable || ' WITH DATA;')"
                )
                .add_operation("TO OPERATOR (DDLOperator)")
            )
            .with_step(
                Step("Load_Staging_To_Target")
                .add_operation("APPLY ('INSERT INTO ' || @TargetTable)")
                .add_operation(
                    "('SELECT * FROM ' || @SourceTable || ' WHERE STATUS = ''ACTIVE'';')"
                )
                .add_operation("TO OPERATOR (LoadOperator)")
                .add_operation("SELECT * FROM OPERATOR (SourceReader)")
            )
            .with_step(
                Step("Update_Target_Processed")
                .add_operation("APPLY")
                .add_operation(
                    "('UPDATE ' || @TargetTable || ' SET PROCESSED_DATE = CURRENT_TIMESTAMP, BATCH_ID = 12345;')"
                )
                .add_operation("TO OPERATOR (UpdateOperator)")
            )
            .with_step(
                Step("Cleanup_Error_Tables")
                .add_operation("APPLY")
                .add_operation("('DROP TABLE ' || @TargetTable || '_ET;')")
                .add_operation("('DROP TABLE ' || @TargetTable || '_UV;')")
                .add_operation("TO OPERATOR (DDLOperator)")
            )
        )

        output = script.build()

        self.assertIn("DEFINE JOB FullETLPipeline", output)
        self.assertIn(
            "DESCRIPTION 'Complete ETL pipeline: DDL -> Load -> Update'", output
        )
        self.assertIn("SET TdpId = @etl_tdpid", output)
        self.assertIn("SET UserName = @etl_username", output)
        self.assertIn("SET ErrorLimit = 500", output)
        self.assertIn("DEFINE SCHEMA StagingSchema", output)
        self.assertIn("DEFINE SCHEMA TargetSchema", output)
        self.assertIn("DEFINE OPERATOR DDLOperator", output)
        self.assertIn("TYPE DDL", output)
        self.assertIn("DEFINE OPERATOR LoadOperator", output)
        self.assertIn("TYPE LOAD", output)
        self.assertIn("DEFINE OPERATOR UpdateOperator", output)
        self.assertIn("TYPE Update", output)
        self.assertIn("SCHEMA TargetSchema", output)
        self.assertIn("STEP DDL_Create_Staging", output)
        self.assertIn("STEP Load_Staging_To_Target", output)
        self.assertIn("STEP Update_Target_Processed", output)
        self.assertIn("STEP Cleanup_Error_Tables", output)

    def test_charset_with_multiple_operators(self):
        """Test character set directive with multiple operators."""
        script = (
            TPTScript("UnicodeETL", charset="UTF8")
            .with_description("ETL with UTF-8 character set")
            .with_var("TdpId", "@tdp")
            .with_schema(
                "UnicodeSchema",
                {"UNICODE_COL": "VARCHAR(1000)", "LATIN_COL": "VARCHAR(500)"},
            )
            .with_operator(
                LoadOperator()
                .with_tdp_id("@TdpId")
                .with_user_name("user")
                .with_user_password("pass")
                .with_target_table("target_table")
                .with_unicode_pass_through(UnicodePassThrough.ON)
            )
        )

        output = script.build()

        self.assertTrue(output.startswith("USING CHARACTER SET UTF8"))
        self.assertIn("DEFINE JOB UnicodeETL", output)
        self.assertIn("DEFINE OPERATOR LoadOperator", output)

    def test_charset_variant_16(self):
        """Test with UTF16 character set variant."""
        script = TPTScript("UTF16Job", charset="UTF16")
        output = script.build()
        self.assertTrue(output.startswith("USING CHARACTER SET UTF16"))


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        self.maxDiff = None

    def test_empty_script(self):
        """Test minimal script with no operators, variables, or steps."""
        script = TPTScript("EmptyJob")
        output = script.build()

        self.assertIn("DEFINE JOB EmptyJob", output)
        self.assertIn("DESCRIPTION ''", output)
        self.assertIn(");", output)

    def test_only_variables_no_operators(self):
        """Test script with only variables set."""
        script = (
            TPTScript("VarsOnly")
            .with_var("Var1", "'value1'")
            .with_var("Var2", "123")
            .with_var("Var3", "@dynamic_var")
        )
        output = script.build()

        self.assertIn("SET Var1 = 'value1'", output)
        self.assertIn("SET Var2 = 123", output)
        self.assertIn("SET Var3 = @dynamic_var", output)
        self.assertNotIn("DEFINE OPERATOR", output)
        self.assertNotIn("STEP", output)

    def test_multiple_schemas(self):
        """Test script with many schemas."""
        script = TPTScript("MultiSchema")
        for i in range(10):
            script.with_schema(
                f"Schema_{i}",
                {f"COL_{j}": f"VARCHAR({100 + j})" for j in range(5)},
            )

        output = script.build()

        for i in range(10):
            self.assertIn(f"DEFINE SCHEMA Schema_{i}", output)
            for j in range(5):
                self.assertIn(f'"COL_{j}" VARCHAR({100 + j})', output)

    def test_large_variable_count(self):
        """Test script with many variables."""
        script = TPTScript("ManyVars")
        for i in range(50):
            script.with_var(f"Variable_{i}", f"'value_{i}'")

        output = script.build()
        for i in range(50):
            self.assertIn(f"SET Variable_{i} = 'value_{i}'", output)

    def test_operators_without_schema(self):
        """Test operators defined without schema association."""
        script = TPTScript("NoSchema").with_operator(
            LoadOperator()
            .with_tdp_id("tdp")
            .with_user_name("user")
            .with_user_password("pass")
            .with_target_table("table")
        )
        output = script.build()

        self.assertIn("DEFINE OPERATOR LoadOperator", output)
        self.assertNotIn("SCHEMA", output.split("ATTRIBUTES")[0])

    def test_step_with_many_operations(self):
        """Test step with a large number of operations."""
        script = TPTScript("ManyOps")
        step = Step("BigStep")
        for i in range(20):
            step.add_operation(f"OPERATION_{i}")
        script.with_step(step)

        output = script.build()
        for i in range(20):
            self.assertIn(f"OPERATION_{i}", output)


class TestValidationAndErrors(unittest.TestCase):
    """Test input validation and error handling."""

    def test_buffer_size_validation_too_high(self):
        """Test that buffer size exceeding max raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_buffer_size(20000)
        self.assertIn("BufferSize must be between 1 and 16384", str(ctx.exception))

    def test_buffer_size_validation_too_low(self):
        """Test that buffer size below min raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_buffer_size(0)
        self.assertIn("BufferSize must be between 1 and 16384", str(ctx.exception))

    def test_buffer_size_boundary_values(self):
        """Test buffer size at boundary values."""
        op = LoadOperator()
        op.with_buffer_size(1)
        op.with_buffer_size(16384)

    def test_error_limit_validation(self):
        """Test that error limit below 1 raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_error_limit(0)
        self.assertIn("ErrorLimit must be greater than 0", str(ctx.exception))

    def test_logon_mech_length_validation(self):
        """Test that logon mech exceeding 8 bytes raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_logon_mech("toolongvalue")
        self.assertIn("LogonMech must not exceed 8 bytes", str(ctx.exception))

    def test_logon_mech_boundary(self):
        """Test logon mech at exactly 8 bytes."""
        op = LoadOperator()
        op.with_logon_mech("exactly8")
        self.assertIn("LogonMech", op.attributes)

    def test_notify_string_length_validation(self):
        """Test that notify string exceeding 80 bytes raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_notify_string("x" * 81)
        self.assertIn("NotifyString must not exceed 80 bytes", str(ctx.exception))

    def test_tenacity_hours_validation(self):
        """Test that negative tenacity hours raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_tenacity_hours(-1)
        self.assertIn(
            "TenacityHours must be greater than or equal to 0", str(ctx.exception)
        )

    def test_tenacity_sleep_validation(self):
        """Test that tenacity sleep below 1 raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_tenacity_sleep(0)
        self.assertIn(
            "TenacitySleep must be greater than or equal to 1", str(ctx.exception)
        )

    def test_min_sessions_validation(self):
        """Test that min sessions below 1 raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_min_sessions(0)
        self.assertIn("MinSessions must be greater than 0", str(ctx.exception))

    def test_max_sessions_validation(self):
        """Test that max sessions below 1 raises error."""
        op = LoadOperator()
        with self.assertRaises(ValueError) as ctx:
            op.with_max_sessions(0)
        self.assertIn("MaxSessions must be greater than 0", str(ctx.exception))


class TestFileOperations(unittest.TestCase):
    """Test file save and output operations."""

    def test_save_to_file(self):
        """Test saving script to file."""
        script = (
            TPTScript("SaveTest")
            .with_description("Save test")
            .with_var("TestVar", "'test_value'")
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".tpt", delete=False) as f:
            temp_path = f.name

        try:
            script.save(temp_path)
            with open(temp_path, "r") as f:
                content = f.read()

            self.assertIn("DEFINE JOB SaveTest", content)
            self.assertIn("SET TestVar = 'test_value'", content)
        finally:
            os.unlink(temp_path)

    def test_save_complex_script(self):
        """Test saving complex multi-operator script."""
        script = (
            TPTScript("ComplexSave")
            .with_description("Complex save test")
            .with_operator(
                LoadOperator()
                .with_tdp_id("tdp")
                .with_user_name("user")
                .with_user_password("pass")
                .with_target_table("table")
                .with_max_sessions(5)
                .with_error_limit(100)
            )
            .with_step(
                Step("Load_Step")
                .add_operation("APPLY INSERT")
                .add_operation("TO OPERATOR (LoadOperator)")
            )
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".tpt", delete=False) as f:
            temp_path = f.name

        try:
            script.save(temp_path)
            with open(temp_path, "r") as f:
                content = f.read()

            self.assertIn("DEFINE JOB ComplexSave", content)
            self.assertIn("DEFINE OPERATOR LoadOperator", content)
            self.assertIn("STEP Load_Step", content)
        finally:
            os.unlink(temp_path)


class TestBuilderPatternChaining(unittest.TestCase):
    """Test fluent builder pattern chaining."""

    def setUp(self):
        self.maxDiff = None

    def test_method_chaining_order_independence(self):
        """Test that method calls can be chained in different orders."""
        script1 = (
            TPTScript("Chain1")
            .with_description("Desc")
            .with_var("A", "1")
            .with_var("B", "2")
            .with_operator(LoadOperator().with_tdp_id("tdp"))
        )

        script2 = (
            TPTScript("Chain2")
            .with_operator(LoadOperator().with_tdp_id("tdp"))
            .with_var("A", "1")
            .with_var("B", "2")
            .with_description("Desc")
        )

        out1 = script1.build()
        out2 = script2.build()

        self.assertIn("SET A = 1", out1)
        self.assertIn("SET B = 2", out1)
        self.assertIn("SET A = 1", out2)
        self.assertIn("SET B = 2", out2)

    def test_multiple_steps_order_preserved(self):
        """Test that steps maintain their order."""
        script = TPTScript("OrderedSteps")
        for i in range(5):
            script.with_step(Step(f"Step_{i}").add_operation(f"OP_{i}"))

        output = script.build()
        step_positions = [output.find(f"STEP Step_{i}") for i in range(5)]

        for i in range(4):
            self.assertLess(step_positions[i], step_positions[i + 1])

    def test_multiple_operators_order_preserved(self):
        """Test that operators maintain their order."""
        script = TPTScript("OrderedOps")
        script.with_operator(DDLOperator().with_tdp_id("tdp1").with_username("user1"))
        script.with_operator(
            LoadOperator()
            .with_tdp_id("tdp2")
            .with_user_name("user2")
            .with_user_password("pass")
            .with_target_table("table")
        )
        script.with_operator(
            UpdateOperator()
            .with_tdp_id("tdp3")
            .with_user_name("user3")
            .with_user_password("pass")
            .with_target_table("table")
        )

        output = script.build()
        ddl_pos = output.find("DEFINE OPERATOR DDLOperator")
        load_pos = output.find("DEFINE OPERATOR LoadOperator")
        update_pos = output.find("DEFINE OPERATOR UpdateOperator")

        self.assertLess(ddl_pos, load_pos)
        self.assertLess(load_pos, update_pos)


class TestVariableInterpolation(unittest.TestCase):
    """Test variable interpolation patterns in scripts."""

    def setUp(self):
        self.maxDiff = None

    def test_at_sign_variables(self):
        """Test variables with @ prefix for dynamic values."""
        script = (
            TPTScript("AtVars")
            .with_var("ConnStr", "@connection_string")
            .with_var("TdpId", "@tdp_identifier")
            .with_var("TargetTable", "@target_tbl_name")
        )
        output = script.build()

        self.assertIn("SET ConnStr = @connection_string", output)
        self.assertIn("SET TdpId = @tdp_identifier", output)
        self.assertIn("SET TargetTable = @target_tbl_name", output)

    def test_quoted_string_variables(self):
        """Test variables with quoted string values."""
        script = (
            TPTScript("QuotedVars")
            .with_var("ErrorList", "'3807','5589','5980'")
            .with_var("LogName", "'MY_LOG_TABLE'")
            .with_var("Description", "'This is a long description with spaces'")
        )
        output = script.build()

        self.assertIn("SET ErrorList = '3807','5589','5980'", output)
        self.assertIn("SET LogName = 'MY_LOG_TABLE'", output)
        self.assertIn(
            "SET Description = 'This is a long description with spaces'", output
        )

    def test_numeric_variables(self):
        """Test variables with numeric values."""
        script = (
            TPTScript("NumVars")
            .with_var("MaxSessions", "10")
            .with_var("ErrorLimit", "500")
            .with_var("BufferSize", "4096")
        )
        output = script.build()

        self.assertIn("SET MaxSessions = 10", output)
        self.assertIn("SET ErrorLimit = 500", output)
        self.assertIn("SET BufferSize = 4096", output)

    def test_mixed_variable_types(self):
        """Test script with mixed variable types."""
        script = (
            TPTScript("MixedVars")
            .with_var("DynamicVar", "@dynamic")
            .with_var("StringVar", "'literal'")
            .with_var("NumberVar", "42")
            .with_var("BooleanLike", "'TRUE'")
        )
        output = script.build()

        self.assertIn("SET DynamicVar = @dynamic", output)
        self.assertIn("SET StringVar = 'literal'", output)
        self.assertIn("SET NumberVar = 42", output)
        self.assertIn("SET BooleanLike = 'TRUE'", output)


class TestLoadOperatorAttributes(unittest.TestCase):
    """Test comprehensive LoadOperator attribute handling."""

    def setUp(self):
        self.maxDiff = None

    def test_all_load_operator_attributes(self):
        """Test setting all available LoadOperator attributes."""
        op = (
            LoadOperator()
            .with_account_id("test_account")
            .with_buffer_size(8192)
            .with_checkpoint_row_count(CheckpointRowCount.YES)
            .with_connect_string("dbc/Teradata")
            .with_data_encryption(DataEncryption.ON)
            .with_date_form(DateForm.INTEGER_DATE)
            .with_drop_error_table(DropErrorTable.YES)
            .with_drop_log_table(DropLogTable.YES)
            .with_error_limit(1000)
            .with_error_table1("ERR_1")
            .with_error_table2("ERR_2")
            .with_logon_mech("TD2")
            .with_logon_mech_data("mech_data")
            .with_log_sql(LogSQL.YES)
            .with_log_table("LOG_TBL")
            .with_max_sessions(16)
            .with_min_sessions(4)
            .with_notify_exit("exit_mod")
            .with_notify_exit_is_dll(NotifyExitIsDLL.YES)
            .with_notify_level(NotifyLevel.HIGH)
            .with_notify_method(NotifyMethod.MSG)
            .with_notify_string("Alert!")
            .with_pause_acq(PauseAcq.NO)
            .with_private_log_name("private_log")
            .with_query_band_sess_info("App=ETL;")
            .with_role_name("ETL_ROLE")
            .with_target_table("TARGET")
            .with_tasm_fast_fail(TASMFASTFAIL.YES)
            .with_tenacity_hours(1)
            .with_tenacity_sleep(15)
            .with_tdp_id("TDP123")
            .with_time_zone_sess_info("GMT")
            .with_transform_group("JSON")
            .with_unicode_pass_through(UnicodePassThrough.ON)
            .with_user_name("etl_user")
            .with_user_password("etl_pass")
            .with_wildcard_insert(WildcardInsert.YES)
            .with_working_database("WORK_DB")
        )

        script = TPTScript("AllAttrs").with_operator(op)
        output = script.build()

        self.assertIn("VARCHAR AccountId = 'test_account'", output)
        self.assertIn("INTEGER BufferSize = 8192", output)
        self.assertIn("VARCHAR CheckpointRowCount = 'Yes'", output)
        self.assertIn("VARCHAR DataEncryption = 'On'", output)
        self.assertIn("INTEGER ErrorLimit = 1000", output)
        self.assertIn("INTEGER MaxSessions = 16", output)
        self.assertIn("INTEGER MinSessions = 4", output)
        self.assertIn("INTEGER TenacityHours = 1", output)
        self.assertIn("INTEGER TenacitySleep = 15", output)
        self.assertIn("VARCHAR UnicodePassThrough = 'On'", output)

    def test_buffer_size_boundary_values(self):
        """Test buffer size at min and max boundaries."""
        op_min = LoadOperator().with_buffer_size(1)
        op_max = LoadOperator().with_buffer_size(16384)

        script_min = TPTScript("BufMin").with_operator(op_min)
        script_max = TPTScript("BufMax").with_operator(op_max)

        self.assertIn("INTEGER BufferSize = 1", script_min.build())
        self.assertIn("INTEGER BufferSize = 16384", script_max.build())


if __name__ == "__main__":
    unittest.main()
