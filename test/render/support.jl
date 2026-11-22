struct UnprintableError <: Exception end
Base.showerror(::IO, ::UnprintableError) = error("can't print this")

# TEST_CONFIG's IRB dates and project_id are also the values config_file()
# falls back to when the file doesn't give them, so checks on those can't
# tell whether the file was read. Those checks use this copy instead.
const OVERRIDE_CONFIG = merge(TEST_CONFIG, OrderedDict(
    "project_id" => "a0n000000000000AAA",
    "irb_start_date" => "2015-03-01",
    "irb_end_date" => "2020-06-30"))

@testset "with_test_env" begin
    @testset "config_file() sees TEST_CONFIG" begin
        with_test_env() do
            @test isfile(TRDW.CONFIG_FILE)
            config = TRDW.config_file()
            @test config[:project_code] == "005547"
            @test config[:irb_id] == "11642"
            @test config[:irb_start_date] == "2010-01-01"
            @test config[:irb_end_date] == "2029-12-31"
            @test config[:project_id] == TEST_CONFIG["project_id"]
            @test config[:project_slug] == TEST_CONFIG["project_slug"]
            @test config[:project_title] == TEST_CONFIG["project_title"]
            @test config[:description] == TEST_CONFIG["description"]
            @test TRDW.is_discovery()
            @test !TRDW.is_quality()
            @test TRDW.irb_date_range() == ("2010-01-01", "2029-12-31")
        end
    end

    @testset "config_file() reads the dates and project_id from the file" begin
        with_test_env(config = OVERRIDE_CONFIG) do
            config = TRDW.config_file()
            @test config[:project_id] == "a0n000000000000AAA"
            @test TRDW.irb_date_range() == ("2015-03-01", "2020-06-30")
        end
    end

    @testset "schemas and the subject table" begin
        with_test_env() do
            @test user_project_schema() == "zz_tester_005547"
            @test project_schema() == "zz_005547"
            @test !TRDW.is_production_schema_prefix()
            t = funsql_subject_table()
            @test t.qualifiers == [:ctsi, :person_map]
            @test t.name == Symbol("005547")
        end
        with_test_env(env = ("DATABRICKS_CATALOG" => "other",
                             "DATABRICKS_TEMP_SCHEMA_PREFIX" => "zz")) do
            @test funsql_subject_table().qualifiers == [:other, :person_map]
            @test user_project_schema() == "zz_005547"
            @test TRDW.is_production_schema_prefix()
        end
    end

    @testset "IRB dates reach the SQL" begin
        sql = with_test_env(config = OVERRIDE_CONFIG) do
            render_ok(@funsql from(person).select(is_during_irb_date_range(birth_datetime)))
        end
        @test sql !== nothing
        @test occursin("'2015-03-01'", sql.raw)
        @test occursin("'2020-06-30'", sql.raw)
    end

    @testset "config = nothing" begin
        with_test_env(config = nothing) do
            @test !isfile(TRDW.CONFIG_FILE)
            # No IRB, so config_file() falls back to the quality project.
            @test TRDW.is_quality()
            @test TRDW.config_file()[:project_code] == "005627"
        end
    end

    @testset "restores the environment and directory" begin
        dir = pwd()
        user = get(ENV, "USER", nothing)
        catalog = get(ENV, "DATABRICKS_CATALOG", nothing)
        inside = with_test_env(() -> pwd())
        @test inside != dir
        @test !isdir(inside)
        @test_throws ErrorException with_test_env(env = ("DATABRICKS_CATALOG" => "other")) do
            error("thrown inside with_test_env")
        end
        @test pwd() == dir
        @test get(ENV, "USER", nothing) == user
        @test get(ENV, "DATABRICKS_CATALOG", nothing) == catalog
    end
end

@testset "render_ok" begin
    @test render_ok(@funsql person()) isa FunSQL.SQLString
    sql = @test_logs (:error, r"render failed: .*no_such_column") render_ok(
        @funsql from(person).select(no_such_column))
    @test sql === nothing
    @test_logs (:error, r"printing it threw ErrorException") print_render_error(UnprintableError())
end
